#!/usr/bin/env python3
"""
WebSocket Stream Test — connects as a real frontend client to the datafeed
service, subscribes to all market streams, and validates the complete
live market data pipeline.

Usage:
    python tests/websocket_stream_test.py
    python tests/websocket_stream_test.py --local
    python tests/websocket_stream_test.py --duration 120
    python tests/websocket_stream_test.py --report-dir ./reports
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import statistics
import sys
import time
import socket
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import websockets

# ── ANSI helpers ────────────────────────────────────────────

CLEAR = "\033[2J\033[H"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
GREY = "\033[90m"
RESET = "\033[0m"


def bold(s: str) -> str:
    return f"{BOLD}{s}{RESET}"


def green(s: str) -> str:
    return f"{GREEN}{s}{RESET}"


def red(s: str) -> str:
    return f"{RED}{s}{RESET}"


def yellow(s: str) -> str:
    return f"{YELLOW}{s}{RESET}"


def ok() -> str:
    return green("✓")


def fail() -> str:
    return red("✗")


# ── Configuration ───────────────────────────────────────────

WS_LOCAL = "ws://localhost:4444"
WS_PROD = "wss://datafeed.fun"
REST_LOCAL = "http://localhost:4444"
REST_PROD = "https://datafeed.fun"

EXCHANGE = "BINANCE"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


@dataclass
class Config:
    ws_url: str = WS_PROD
    rest_base: str = REST_PROD
    duration: int = 60
    report_dir: str = "reports"

    @classmethod
    def from_args(cls) -> Config:
        p = argparse.ArgumentParser(description="WebSocket Stream Test")
        p.add_argument("--local", action="store_true", help="Use localhost:4444")
        p.add_argument("--duration", type=int, default=60, help="Test duration (seconds)")
        p.add_argument("--report-dir", default="reports", help="Report output directory")
        p.add_argument("--ws-url", help="WebSocket URL override")
        p.add_argument("--rest-base", help="REST API base URL override")
        args = p.parse_args()

        if args.ws_url:
            ws = args.ws_url
            rest = args.rest_base or (REST_PROD if "datafeed.fun" in ws else REST_LOCAL)
        elif args.local:
            ws = WS_LOCAL
            rest = REST_LOCAL
        else:
            ws = WS_PROD
            rest = REST_PROD

        return cls(ws_url=ws, rest_base=rest, duration=args.duration, report_dir=args.report_dir)


# ── Per-Symbol tracking ─────────────────────────────────────

@dataclass
class SymbolStats:
    symbol: str = ""
    updates: int = 0
    latest_price: float = 0.0
    latest_bid: float = 0.0
    latest_ask: float = 0.0
    first_ts: float = 0.0
    last_ts: float = 0.0


# ── Dashboard snapshot ──────────────────────────────────────

@dataclass
class DashSnapshot:
    timestamp: float = 0.0
    raw: dict = field(default_factory=dict)

    def get(self, path: str, default: Any = None) -> Any:
        parts = path.split(".")
        cur = self.raw
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p, {})
            else:
                return default
        return cur if cur != {} else default

    @property
    def messages_per_sec(self) -> float:
        return self.get("throughput.messages_per_sec", 0) or 0

    @property
    def packets_per_sec(self) -> float:
        return self.get("throughput.packets_per_sec", 0) or 0

    @property
    def bytes_per_sec(self) -> float:
        return self.get("throughput.bytes_per_sec", 0) or 0

    @property
    def active_sessions(self) -> int:
        return self.get("sessions.active_sessions", 0) or 0

    @property
    def active_subscriptions(self) -> int:
        return self.get("sessions.active_subscriptions", 0) or 0

    @property
    def feed_health(self) -> float:
        return self.get("feed.health_score", 0) or self.get("health_score", 0) or 0


# ── Test Statistics ─────────────────────────────────────────

@dataclass
class TestStats:
    # Connection
    connected: bool = False
    connect_time: float = 0.0
    connect_duration_ms: float = 0.0
    reconnects: int = 0
    disconnects: int = 0

    # Subscriptions
    subs_sent: int = 0
    subs_acked: int = 0
    subs_active: int = 0
    subs_failed: int = 0

    # Messages
    total_messages: int = 0
    market_data_msgs: int = 0
    monitoring_msgs: int = 0
    unknown_msgs: int = 0
    total_bytes: int = 0

    # Timing
    intervals_ms: list = field(default_factory=list)
    receive_times: list = field(default_factory=list)
    start_time: float = 0.0
    first_market_ts: float = 0.0
    last_market_ts: float = 0.0

    # Symbols
    symbols: dict = field(default_factory=lambda: defaultdict(SymbolStats))

    # Messages (raw for analysis)
    all_raw_messages: list = field(default_factory=list)

    # Errors
    errors: list = field(default_factory=list)

    # Dashboard
    dash_snapshots: list = field(default_factory=list)

    # Diagnostics
    diagnostics_triggered: bool = False
    diagnostics_output: list = field(default_factory=list)

    # Protocol
    live_mode_sent: bool = False
    exchange_switched: bool = False

    def process_message(self, raw: str) -> None:
        now = time.time()
        self.total_messages += 1
        byte_len = len(raw.encode("utf-8"))
        self.total_bytes += byte_len
        self.receive_times.append(now)

        # Track intervals
        if len(self.receive_times) >= 2:
            ms = (self.receive_times[-1] - self.receive_times[-2]) * 1000
            self.intervals_ms.append(ms)

        try:
            data = json.loads(raw)
            if not isinstance(data, dict):
                self.unknown_msgs += 1
                return
        except json.JSONDecodeError:
            self.unknown_msgs += 1
            return

        # Classify: market data (ticker topic with symbol/price/bid/ask)
        topic = data.get("topic", "")
        msg_type = data.get("type", "")

        if topic == "ticker_" and "symbol" in data:
            self.market_data_msgs += 1
            sym = str(data.get("symbol", ""))
            if sym:
                s = self.symbols[sym]
                s.symbol = sym
                s.updates += 1
                s.latest_price = data.get("price", s.latest_price)
                s.latest_bid = data.get("bid", s.latest_bid)
                s.latest_ask = data.get("ask", s.latest_ask)
                if s.first_ts == 0:
                    s.first_ts = data.get("timestamp", now)
                s.last_ts = data.get("timestamp", now)
            if self.first_market_ts == 0:
                self.first_market_ts = now
            self.last_market_ts = now
        elif msg_type in (
            "dashboard", "metrics", "performance",
            "exchange", "system", "network", "feed", "queues",
        ):
            self.monitoring_msgs += 1
        else:
            self.unknown_msgs += 1

    @property
    def elapsed(self) -> float:
        if self.start_time == 0:
            return 0.0
        return time.time() - self.start_time

    @property
    def messages_per_sec(self) -> float:
        if self.elapsed < 1:
            return 0.0
        return self.total_messages / self.elapsed

    @property
    def bytes_per_sec(self) -> float:
        if self.elapsed < 1:
            return 0.0
        return self.total_bytes / self.elapsed

    @property
    def market_data_per_sec(self) -> float:
        if self.last_market_ts == 0 or self.first_market_ts == 0:
            return 0.0
        dur = self.last_market_ts - self.first_market_ts
        if dur <= 0:
            return 0.0
        return self.market_data_msgs / dur

    def interval_stats(self) -> dict:
        if not self.intervals_ms:
            return {"avg": 0, "min": 0, "max": 0, "p50": 0, "p95": 0, "p99": 0}
        s = sorted(self.intervals_ms)
        n = len(s)
        def perc(p):
            idx = int(n * p / 100)
            return s[min(idx, n - 1)]
        return {
            "avg": statistics.mean(self.intervals_ms),
            "min": min(self.intervals_ms),
            "max": max(self.intervals_ms),
            "p50": perc(50),
            "p95": perc(95),
            "p99": perc(99),
        }

    def hr_bytes(self, b: float) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if abs(b) < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"


# ── Terminal Display ────────────────────────────────────────

def fmt_duration(sec: float) -> str:
    m = int(sec // 60)
    s = int(sec % 60)
    return f"{m:02d}:{s:02d}"


def render_dashboard(stats: TestStats, config: Config) -> str:
    dash = stats.dash_snapshots[-1] if stats.dash_snapshots else DashSnapshot()
    lines = []
    lines.append("=" * 54)
    lines.append(f"        {BOLD}DATAFEED WEBSOCKET STREAM TEST{RESET}")
    lines.append("=" * 54)
    lines.append("")

    # Status line
    status = green("CONNECTED") if stats.connected else red("DISCONNECTED")
    lines.append(f"  {BOLD}Status{RESET}           {status}")
    lines.append(f"  {BOLD}Exchange{RESET}         {EXCHANGE}")
    lines.append(f"  {BOLD}Duration{RESET}         {fmt_duration(stats.elapsed)} / {fmt_duration(config.duration)} sec")
    lines.append("")

    # Subscriptions
    lines.append(f"  {BOLD}Subscriptions{RESET}")
    for sym in SYMBOLS:
        s = stats.symbols.get(sym)
        if s and s.updates > 0:
            lines.append(f"    {ok()} {sym}")
        else:
            lines.append(f"    {fail()} {sym}")
    lines.append("")

    # Messages
    lines.append(f"  {BOLD}Messages{RESET}")
    lines.append(f"    {'Total':<20} {stats.total_messages}")
    lines.append(f"    {'Market':<20} {stats.market_data_msgs}")
    lines.append(f"    {'Monitoring':<20} {stats.monitoring_msgs}")
    lines.append(f"    {'Unknown':<20} {stats.unknown_msgs}")
    lines.append("")

    # Rate
    lines.append(f"  {BOLD}Rate{RESET}")
    lines.append(f"    {'Messages/sec':<20} {stats.messages_per_sec:.1f}")
    lines.append(f"    {'Bytes/sec':<20} {stats.hr_bytes(stats.bytes_per_sec)}")
    lines.append("")

    # Per-symbol
    for sym in SYMBOLS:
        s = stats.symbols.get(sym)
        if s and s.updates > 0:
            price_str = f"{s.latest_price:.2f}" if s.latest_price != 0 else "--"
            bid_str = f"{s.latest_bid:.2f}" if s.latest_bid != 0 else "--"
            ask_str = f"{s.latest_ask:.2f}" if s.latest_ask != 0 else "--"
            lines.append(f"  {BOLD}{sym}{RESET}")
            lines.append(f"    {'Price':<20} {price_str}")
            lines.append(f"    {'Bid':<20} {bid_str}")
            lines.append(f"    {'Ask':<20} {ask_str}")
            lines.append(f"    {'Updates':<20} {s.updates}")
            lines.append("")

    # Dashboard
    lines.append(f"  {BOLD}Dashboard{RESET}")
    lines.append(f"    {'Sessions':<20} {dash.active_sessions}")
    lines.append(f"    {'Subscriptions':<20} {dash.active_subscriptions}")
    lines.append(f"    {'Feed Health':<20} {dash.feed_health}")
    lines.append("")

    # Elapsed / Remaining
    remaining = max(0, config.duration - stats.elapsed)
    lines.append(f"  {BOLD}{'Elapsed':<20}{RESET} {stats.elapsed:.0f} sec")
    lines.append(f"  {BOLD}{'Remaining':<20}{RESET} {remaining:.0f} sec")
    lines.append("")
    lines.append("=" * 54)

    return "\n".join(lines)


# ── Diagnostics ─────────────────────────────────────────────

def run_diagnostics(stats: TestStats, config: Config) -> list:
    diags = []
    elapsed = stats.elapsed

    if stats.total_messages == 0:
        diags.append("No messages received at all — connection may have failed or server is unreachable.")
        return diags

    if stats.market_data_msgs == 0 and stats.monitoring_msgs > 0:
        diags.append("Monitoring messages are flowing but NO market data received.")
        diags.append("  Most likely causes:")
        diags.append("  1. Subscription to 'ticker_' topic failed — verify subscribe message format.")
        diags.append(f"  2. Exchange '{EXCHANGE}' adapter is not connected or not producing data.")
        diags.append("  3. Feed instance is not registered or the adapter is not broadcasting.")
        diags.append("  4. The live source may not be running — check if _Live mode was activated.")

    if stats.monitoring_msgs == 0:
        diags.append("No monitoring messages received — subscriptions may not be working.")
        diags.append("  The server uses substring matching on subscribe messages.")
        diags.append("  Verify that keywords like 'dashboard', 'metrics', etc. appear in subscribe text.")

    if not stats.live_mode_sent:
        diags.append("_Live mode was not activated — backtest mode may be active.")
    elif not stats.exchange_switched:
        diags.append("Exchange switch command was not sent.")

    # Check dashboard
    if stats.dash_snapshots:
        latest = stats.dash_snapshots[-1]
        if latest.messages_per_sec == 0 and latest.packets_per_sec == 0:
            diags.append("Dashboard also shows zero throughput — confirms server-side issue.")
        if latest.active_sessions == 0:
            diags.append("Dashboard reports zero active sessions — session may not be registered.")
        if latest.active_subscriptions == 0:
            diags.append("Dashboard reports zero active subscriptions — subscription may not be registered.")
        if latest.feed_health == 0:
            diags.append("Dashboard reports feed health = 0 — feed instance may be unhealthy.")

    if not diags:
        diags.append("Diagnostics inconclusive — market data may still arrive. Check exchange connection status.")

    return diags


# ── Report Generation ───────────────────────────────────────

def generate_report(stats: TestStats, config: Config, passed: bool) -> dict:
    iv = stats.interval_stats()
    dash = stats.dash_snapshots[-1] if stats.dash_snapshots else DashSnapshot()

    # Determine overall result
    checks = {
        "connected": stats.connected,
        "market_data_received": stats.market_data_msgs > 0,
        "all_symbols_seen": all(sym in stats.symbols for sym in SYMBOLS),
        "monitoring_received": stats.monitoring_msgs > 0,
        "subscriptions_active": dash.active_subscriptions > 0,
        "sessions_active": dash.active_sessions > 0,
    }

    recommendations = []
    if stats.market_data_msgs == 0:
        recommendations.append("Verify the exchange adapter (Binance) is connected and producing data.")
        recommendations.append("Check that the subscription message contains the keyword 'ticker' for substring matching.")
        recommendations.append("Ensure _Live mode is activated before subscribing.")
    if stats.monitoring_msgs == 0:
        recommendations.append("Add monitoring topic keywords (dashboard, metrics, etc.) to subscription messages.")
    if dash.active_subscriptions == 0 and stats.subs_sent > 0:
        recommendations.append("Subscriptions may not be registered — verify server's add_client_topics implementation.")
    if not recommendations:
        recommendations.append("Pipeline is healthy.")

    report = {
        "test_name": "WebSocket Stream Test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "ws_url": config.ws_url,
            "rest_base": config.rest_base,
            "duration_seconds": config.duration,
            "exchange": EXCHANGE,
            "symbols": SYMBOLS,
        },
        "connection": {
            "connected": stats.connected,
            "connect_time_utc": datetime.fromtimestamp(stats.connect_time, tz=timezone.utc).isoformat(),
            "connect_duration_ms": round(stats.connect_duration_ms, 2),
            "reconnects": stats.reconnects,
            "disconnects": stats.disconnects,
        },
        "subscriptions": {
            "requests_sent": stats.subs_sent,
            "acknowledgements": stats.subs_acked,
            "active_on_dashboard": dash.active_subscriptions,
            "failed": stats.subs_failed,
        },
        "messages": {
            "total": stats.total_messages,
            "market_data": stats.market_data_msgs,
            "monitoring": stats.monitoring_msgs,
            "unknown": stats.unknown_msgs,
        },
        "symbols": {},
        "throughput": {
            "messages_per_sec": round(stats.messages_per_sec, 2),
            "market_data_per_sec": round(stats.market_data_per_sec, 2),
            "bytes_per_sec": round(stats.bytes_per_sec, 2),
            "total_bytes": stats.total_bytes,
            "total_packets": stats.total_messages,
        },
        "latency": {
            "avg_interval_ms": round(iv["avg"], 3),
            "min_interval_ms": round(iv["min"], 3),
            "max_interval_ms": round(iv["max"], 3),
            "p50_ms": round(iv["p50"], 3),
            "p95_ms": round(iv["p95"], 3),
            "p99_ms": round(iv["p99"], 3),
        },
        "dashboard_comparison": {
            "dashboard_snapshots": len(stats.dash_snapshots),
            "dashboard_messages_per_sec": round(dash.messages_per_sec, 2),
            "dashboard_packets_per_sec": round(dash.packets_per_sec, 2),
            "dashboard_bytes_per_sec": round(dash.bytes_per_sec, 2),
            "dashboard_sessions": dash.active_sessions,
            "dashboard_subscriptions": dash.active_subscriptions,
            "dashboard_feed_health": dash.feed_health,
        },
        "diagnostics": stats.diagnostics_output if stats.diagnostics_triggered else [],
        "errors": stats.errors[:20],
        "recommendations": recommendations,
        "checks": checks,
        "passed": passed,
    }

    for sym in SYMBOLS:
        s = stats.symbols.get(sym)
        if s:
            report["symbols"][sym] = {
                "price": s.latest_price,
                "bid": s.latest_bid,
                "ask": s.latest_ask,
                "updates": s.updates,
                "first_timestamp": s.first_ts,
                "last_timestamp": s.last_ts,
            }
        else:
            report["symbols"][sym] = {
                "price": 0, "bid": 0, "ask": 0,
                "updates": 0, "first_timestamp": 0, "last_timestamp": 0,
            }

    return report


def write_text_report(report: dict, path: str) -> None:
    lines = []
    lines.append("=" * 60)
    lines.append("  DATAFEED WEBSOCKET STREAM TEST REPORT")
    lines.append("=" * 60)
    lines.append("")

    c = report["config"]
    lines.append(f"  Test:         {report['test_name']}")
    lines.append(f"  Timestamp:    {report['timestamp']}")
    lines.append(f"  Duration:     {c['duration_seconds']}s")
    lines.append(f"  WS URL:       {c['ws_url']}")
    lines.append(f"  REST Base:    {c['rest_base']}")
    lines.append(f"  Exchange:     {c['exchange']}")
    lines.append(f"  Symbols:      {', '.join(c['symbols'])}")
    lines.append("")

    # Result
    passed = report["passed"]
    lines.append(f"  Result:       {green('PASS') if passed else red('FAIL')}")
    lines.append("")
    lines.append("-" * 60)

    # Connection
    conn = report["connection"]
    lines.append("")
    lines.append("  CONNECTION")
    lines.append(f"    Connected:         {conn['connected']}")
    lines.append(f"    Connect time:      {conn['connect_time_utc']}")
    lines.append(f"    Connect duration:  {conn['connect_duration_ms']} ms")
    lines.append(f"    Reconnects:        {conn['reconnects']}")
    lines.append(f"    Disconnects:       {conn['disconnects']}")

    # Subscriptions
    sub = report["subscriptions"]
    lines.append("")
    lines.append("  SUBSCRIPTIONS")
    lines.append(f"    Requests sent:     {sub['requests_sent']}")
    lines.append(f"    Acknowledgements: {sub['acknowledgements']}")
    lines.append(f"    Dashboard active:  {sub['active_on_dashboard']}")
    lines.append(f"    Failed:            {sub['failed']}")

    # Messages
    msgs = report["messages"]
    lines.append("")
    lines.append("  MESSAGES")
    lines.append(f"    Total:             {msgs['total']}")
    lines.append(f"    Market data:       {msgs['market_data']}")
    lines.append(f"    Monitoring:        {msgs['monitoring']}")
    lines.append(f"    Unknown:           {msgs['unknown']}")

    # Throughput
    tp = report["throughput"]
    lines.append("")
    lines.append("  THROUGHPUT")
    lines.append(f"    Messages/sec:      {tp['messages_per_sec']}")
    lines.append(f"    Market data/sec:   {tp['market_data_per_sec']}")
    lines.append(f"    Bytes/sec:         {tp['bytes_per_sec']}")
    lines.append(f"    Total bytes:       {tp['total_bytes']}")
    lines.append(f"    Total packets:     {tp['total_packets']}")

    # Latency
    lat = report["latency"]
    lines.append("")
    lines.append("  LATENCY (ms)")
    lines.append(f"    Average interval:  {lat['avg_interval_ms']}")
    lines.append(f"    Minimum interval:  {lat['min_interval_ms']}")
    lines.append(f"    Maximum interval:  {lat['max_interval_ms']}")
    lines.append(f"    P50:               {lat['p50_ms']}")
    lines.append(f"    P95:               {lat['p95_ms']}")
    lines.append(f"    P99:               {lat['p99_ms']}")

    # Symbols
    lines.append("")
    lines.append("  PER-SYMBOL STATISTICS")
    for sym, sdata in report["symbols"].items():
        lines.append(f"    {sym}:")
        lines.append(f"      Price:    {sdata['price']}")
        lines.append(f"      Bid:      {sdata['bid']}")
        lines.append(f"      Ask:      {sdata['ask']}")
        lines.append(f"      Updates:  {sdata['updates']}")

    # Dashboard comparison
    dc = report.get("dashboard_comparison", {})
    lines.append("")
    lines.append("  DASHBOARD COMPARISON")
    lines.append(f"    Snapshots taken:          {dc.get('dashboard_snapshots', 0)}")
    lines.append(f"    Dashboard msgs/sec:       {dc.get('dashboard_messages_per_sec', 0)}")
    lines.append(f"    Dashboard pkts/sec:       {dc.get('dashboard_packets_per_sec', 0)}")
    lines.append(f"    Dashboard bytes/sec:      {dc.get('dashboard_bytes_per_sec', 0)}")
    lines.append(f"    Dashboard sessions:       {dc.get('dashboard_sessions', 0)}")
    lines.append(f"    Dashboard subscriptions:  {dc.get('dashboard_subscriptions', 0)}")
    lines.append(f"    Dashboard feed health:    {dc.get('dashboard_feed_health', 0)}")

    # Checks
    lines.append("")
    lines.append("  CHECKS")
    for check, result in report["checks"].items():
        lines.append(f"    {'✓' if result else '✗'} {check}")

    # Diagnostics
    if report["diagnostics"]:
        lines.append("")
        lines.append("  DIAGNOSTICS")
        for d in report["diagnostics"]:
            lines.append(f"    {d}")

    # Errors
    if report["errors"]:
        lines.append("")
        lines.append("  ERRORS")
        for e in report["errors"]:
            lines.append(f"    {e}")

    # Recommendations
    lines.append("")
    lines.append("  RECOMMENDATIONS")
    for r in report["recommendations"]:
        lines.append(f"    * {r}")

    lines.append("")
    lines.append("=" * 60)
    lines.append(f"  OVERALL: {green('PASS') if passed else red('FAIL')}")
    lines.append("=" * 60)
    lines.append("")

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(lines))
        f.write("\n")


def write_json_report(report: dict, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=2, default=str)


# ── Redirect Resolution ─────────────────────────────────────

def resolve_ws_endpoint(ws_url: str) -> tuple[str, Optional[str], Optional[str]]:
    """
    Check for HTTP 301/302 and convert Location to proper WebSocket URI.
    Returns (final_ws_url, redirect_target, converted_ws_url).
    """
    http_url = ws_url.replace("wss://", "https://", 1).replace("ws://", "http://", 1)
    try:
        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, hdrs, newurl):
                return None
        opener = urllib.request.build_opener(_NoRedirect)
        req = urllib.request.Request(http_url, method="HEAD")
        with opener.open(req, timeout=5) as resp:
            if resp.status in (301, 302):
                location = resp.headers.get("Location", "")
                if location:
                    converted = location.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
                    return converted, location, converted
        return ws_url, None, None
    except Exception:
        return ws_url, None, None


# ── Main Test Logic ─────────────────────────────────────────

async def run_test(config: Config) -> bool:
    stats = TestStats()
    stats.start_time = time.time()

    # ── Connection Diagnostics ──
    print(f"{CLEAR}")
    print(f"  {BOLD}Connection Diagnostics{RESET}")
    print(f"  {'Configured Endpoint':<25} {config.ws_url}")

    resolved_url, redirect_target, converted_url = resolve_ws_endpoint(config.ws_url)
    print(f"  {'Resolved Endpoint':<25} {resolved_url}")

    canonical = resolved_url
    if redirect_target:
        print(f"  {'301 Redirect Detected':<25} YES")
        print(f"  {'Original':<25} {config.ws_url}")
        print(f"  {'Redirect':<25} {redirect_target}")
        print(f"  {'Converted':<25} {converted_url}")
        canonical = converted_url
        config.ws_url = canonical
        print(f"  {'Canonical Endpoint':<25} {canonical}")
        print(f"  {'Retrying...':<25}")
    else:
        print(f"  {'Canonical Endpoint':<25} {canonical}")

    try:
        host = config.ws_url.split("://")[1].split("/")[0].split(":")[0]
        ips = socket.getaddrinfo(host, 443 if "wss" in config.ws_url else 80)
        print(f"  {'DNS Resolution':<25} {ips[0][4][0]}")
    except Exception:
        print(f"  {'DNS Resolution':<25} FAILED")

    tls_enabled = "wss" in config.ws_url
    print(f"  {'TLS Enabled':<25} {tls_enabled}")
    print(f"  {'WebSocket URI':<25} {config.ws_url}")
    print("")

    # ── Connect ──
    header = (
        f"  {BOLD}Connecting to{RESET} {config.ws_url} ...\n"
        f"  {BOLD}Exchange{RESET}     {EXCHANGE}\n"
        f"  {BOLD}Symbols{RESET}     {', '.join(SYMBOLS)}\n"
    )
    print(header)

    connect_start = time.time()
    try:
        ws = await websockets.connect(
            config.ws_url,
            ping_interval=10,
            ping_timeout=5,
            max_size=2 ** 24,
        )
        stats.connected = True
        stats.connect_time = time.time()
        stats.connect_duration_ms = (time.time() - connect_start) * 1000
    except Exception as e:
        error_type = type(e).__name__
        stats.connected = False
        error_msg = f"Connect failed ({error_type}): {e}"
        stats.errors.append(error_msg)
        print(f"\n  {red('FAIL')} Could not connect: {e}\n")
        dt = datetime.now(timezone.utc).isoformat()
        report = {
            "test_name": "WebSocket Stream Test",
            "timestamp": dt,
            "config": {"ws_url": config.ws_url, "rest_base": config.rest_base,
                       "duration_seconds": config.duration, "exchange": EXCHANGE,
                       "symbols": SYMBOLS},
            "connection": {"connected": False, "connect_time_utc": dt,
                           "connect_duration_ms": 0, "reconnects": 0, "disconnects": 1},
            "subscriptions": {"requests_sent": 0, "acknowledgements": 0,
                              "active_on_dashboard": 0, "failed": 0},
            "messages": {"total": 0, "market_data": 0, "monitoring": 0, "unknown": 0},
            "symbols": {s: {"price": 0, "bid": 0, "ask": 0, "updates": 0,
                            "first_timestamp": 0, "last_timestamp": 0} for s in SYMBOLS},
            "throughput": {"messages_per_sec": 0, "market_data_per_sec": 0,
                           "bytes_per_sec": 0, "total_bytes": 0, "total_packets": 0},
            "latency": {"avg_interval_ms": 0, "min_interval_ms": 0, "max_interval_ms": 0,
                        "p50_ms": 0, "p95_ms": 0, "p99_ms": 0},
            "dashboard_comparison": {
                "dashboard_snapshots": 0,
                "dashboard_messages_per_sec": 0,
                "dashboard_packets_per_sec": 0,
                "dashboard_bytes_per_sec": 0,
                "dashboard_sessions": 0,
                "dashboard_subscriptions": 0,
                "dashboard_feed_health": 0,
            },
            "diagnostics": [],
            "errors": [error_msg],
            "recommendations": ["Check server availability and network connectivity.",
                                "Verify WebSocket URL and port."],
            "checks": {"connected": False, "market_data_received": False,
                       "all_symbols_seen": False, "monitoring_received": False,
                       "subscriptions_active": False, "sessions_active": False},
            "passed": False,
        }
        write_text_report(report, os.path.join(config.report_dir, "websocket_stream_report.txt"))
        write_json_report(report, os.path.join(config.report_dir, "websocket_stream_report.json"))
        print(f"\n  Report: {config.report_dir}/websocket_stream_report.txt")
        return False

    # ── Protocol Handshake ──
    try:
        # 1. Enable live mode
        await ws.send("_Live")
        stats.live_mode_sent = True
        await asyncio.sleep(0.2)

        # 2. Switch exchange
        switch_msg = {
            "type": "switch_exchange",
            "exchange": EXCHANGE,
            "symbols": SYMBOLS,
        }
        await ws.send(json.dumps(switch_msg))
        stats.exchange_switched = True
        await asyncio.sleep(0.3)

        # 3. Subscribe to everything via "all" keyword
        await ws.send(json.dumps({"subscribe": "all"}))
        stats.subs_sent += 1

        # 4. Also subscribe to individual topics for redundancy
        for topic in ["ticker_", "dashboard", "metrics", "performance",
                      "exchange", "system", "network", "feed", "queues"]:
            await ws.send(json.dumps({"subscribe": topic}))
            stats.subs_sent += 1
            await asyncio.sleep(0.02)

    except websockets.ConnectionClosed as e:
        stats.errors.append(f"Connection closed during handshake: {e}")
        await ws.close()
        report_data = generate_report(stats, config, False)
        write_text_report(report_data, os.path.join(config.report_dir, "websocket_stream_report.txt"))
        write_json_report(report_data, os.path.join(config.report_dir, "websocket_stream_report.json"))
        return False

    # ── Main Loop ──
    stop_event = asyncio.Event()
    deadline = time.time() + config.duration

    async def collect() -> None:
        nonlocal ws
        while not stop_event.is_set():
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
                stats.process_message(str(msg))
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed as e:
                stats.disconnects += 1
                stats.errors.append(f"Connection closed during test: {e}")
                # Attempt reconnect
                try:
                    ws = await websockets.connect(
                        config.ws_url,
                        ping_interval=10,
                        ping_timeout=5,
                        max_size=2 ** 24,
                    )
                    stats.reconnects += 1
                    stats.connected = True
                    # Re-send protocol
                    await ws.send("_Live")
                    await asyncio.sleep(0.1)
                    await ws.send(json.dumps({
                        "type": "switch_exchange", "exchange": EXCHANGE, "symbols": SYMBOLS,
                    }))
                    await asyncio.sleep(0.1)
                    for topic in ["ticker_", "dashboard", "metrics", "performance",
                                  "exchange", "system", "network", "feed", "queues"]:
                        await ws.send(json.dumps({"subscribe": topic}))
                        await asyncio.sleep(0.02)
                except Exception as re:
                    stats.errors.append(f"Reconnect failed: {re}")
                    stats.connected = False
                    break

    async def display_loop() -> None:
        while not stop_event.is_set():
            print(f"{CLEAR}{render_dashboard(stats, config)}")
            await asyncio.sleep(1.0)

    async def poll_dashboard() -> None:
        while not stop_event.is_set():
            try:
                loop = asyncio.get_running_loop()
                resp = await loop.run_in_executor(None, lambda: _fetch_json(
                    f"{config.rest_base}/api/v1/dashboard"))
                if resp is not None:
                    stats.dash_snapshots.append(DashSnapshot(timestamp=time.time(), raw=resp))
            except Exception as e:
                pass
            await asyncio.sleep(1.0)

    async def check_diagnostics() -> None:
        await asyncio.sleep(10)
        if stats.market_data_msgs == 0 and not stats.diagnostics_triggered:
            stats.diagnostics_triggered = True
            stats.diagnostics_output = run_diagnostics(stats, config)

    collector_task = asyncio.create_task(collect())
    display_task = asyncio.create_task(display_loop())
    poller_task = asyncio.create_task(poll_dashboard())
    diag_task = asyncio.create_task(check_diagnostics())

    try:
        await asyncio.sleep(config.duration)
    finally:
        stop_event.set()
        await asyncio.gather(collector_task, display_task, poller_task, diag_task,
                             return_exceptions=True)
        try:
            await ws.close()
        except Exception:
            pass

    # ── Determine Result ──
    passed = (
        stats.connected
        and stats.market_data_msgs > 0
        and all(sym in stats.symbols and stats.symbols[sym].updates > 0 for sym in SYMBOLS)
        and stats.monitoring_msgs > 0
    )

    # ── Generate Reports ──
    report = generate_report(stats, config, passed)
    txt_path = os.path.join(config.report_dir, "websocket_stream_report.txt")
    json_path = os.path.join(config.report_dir, "websocket_stream_report.json")
    write_text_report(report, txt_path)
    write_json_report(report, json_path)

    # Final output
    print(f"{CLEAR}{render_dashboard(stats, config)}")
    print("")
    print(f"  {BOLD}Test Complete{RESET}")
    print(f"  Result: {green('PASS') if passed else red('FAIL')}")
    print(f"  Report: {txt_path}")
    print(f"  Report: {json_path}")
    print("")

    return passed


def _fetch_json(url: str) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


# ── Entry Point ─────────────────────────────────────────────

def main() -> int:
    config = Config.from_args()
    try:
        passed = asyncio.run(run_test(config))
        return 0 if passed else 1
    except KeyboardInterrupt:
        print(f"\n  {yellow('Test interrupted by user')}")
        return 130


if __name__ == "__main__":
    sys.exit(main())
