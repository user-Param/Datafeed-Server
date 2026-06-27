#!/usr/bin/env python3
"""
Production-grade end-to-end validation suite for the PROTYPE Datafeed infrastructure.

Validates the complete pipeline:
    Exchange → Feed Manager → Parser → Normalizer → Metrics Collector →
    WebSocket → Dashboard API → Client

Usage:
    python tests/stress_test.py
    python tests/stress_test.py --report-dir ./reports
    python tests/stress_test.py --stress-rate 500
    python tests/stress_test.py --ci                    # exit 0/1 for CI/CD
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import statistics
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable, Set
from urllib.parse import urlparse

import aiohttp
import aiohttp.client_ws

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("datafeed-validator")

# ──────────────────────────────────────────────
# ANSI helpers
# ──────────────────────────────────────────────

class ANSI:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
    GREY = "\033[90m"

def ok(text: str) -> str:
    return f"{ANSI.GREEN}PASS{ANSI.RESET}"

def fail(text: str = "FAIL") -> str:
    return f"{ANSI.RED}{text}{ANSI.RESET}"

def warn(text: str) -> str:
    return f"{ANSI.YELLOW}{text}{ANSI.RESET}"

def bold(text: str) -> str:
    return f"{ANSI.BOLD}{text}{ANSI.RESET}"

def color_status(passed: bool) -> str:
    return ok("PASS") if passed else fail("FAIL")

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

@dataclass
class Config:
    rest_base: str = "https://datafeed.fun"
    ws_url: str = "wss://datafeed.fun"
    dashboard_poll_seconds: int = 30
    dashboard_poll_interval: float = 1.0
    websocket_wait_seconds: int = 15
    resource_monitor_seconds: int = 60
    resource_monitor_interval: float = 2.0
    stress_rate: int = 50
    stress_duration: int = 10
    benchmark_iterations: int = 5
    timeout: int = 15
    max_redirects: int = 5
    report_dir: str = "reports"
    verbose: bool = False
    ci_mode: bool = False
    skip_stress: bool = False
    skip_websocket: bool = False
    skip_resource_monitor: bool = False


# ──────────────────────────────────────────────
# Structured Results
# ──────────────────────────────────────────────

@dataclass
class StageResult:
    name: str
    passed: bool
    detail: str = ""
    diagnostics: List[str] = field(default_factory=list)

@dataclass
class PipelineReport:
    stages: List[StageResult] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str = "", diagnostics: Optional[List[str]] = None) -> None:
        self.stages.append(StageResult(name=name, passed=passed, detail=detail, diagnostics=diagnostics or []))

    def passed_count(self) -> int:
        return sum(1 for s in self.stages if s.passed)

    def failed_count(self) -> int:
        return sum(1 for s in self.stages if not s.passed)

    def total(self) -> int:
        return len(self.stages)

    def all_passed(self) -> bool:
        return self.failed_count() == 0

    def critical_failures(self) -> List[StageResult]:
        critical = {"Exchange", "Feed Manager", "Parser", "Normalizer", "Metrics Collector", "Dashboard", "WebSocket", "Database", "Market Data"}
        return [s for s in self.stages if not s.passed and s.name in critical]


@dataclass
class LatencyStats:
    endpoint: str = ""
    method: str = ""
    samples: List[float] = field(default_factory=list)

    @property
    def avg(self) -> float:
        return statistics.mean(self.samples) if self.samples else 0.0

    @property
    def min(self) -> float:
        return min(self.samples) if self.samples else 0.0

    @property
    def max(self) -> float:
        return max(self.samples) if self.samples else 0.0

    @property
    def p50(self) -> float:
        return self._perc(50)

    @property
    def p90(self) -> float:
        return self._perc(90)

    @property
    def p95(self) -> float:
        return self._perc(95)

    @property
    def p99(self) -> float:
        return self._perc(99)

    def _perc(self, p: float) -> float:
        if not self.samples:
            return 0.0
        s = sorted(self.samples)
        idx = int(len(s) * p / 100)
        return s[min(idx, len(s) - 1)]


@dataclass
class ResourceSnapshot:
    timestamp: float = 0.0
    cpu: float = 0.0
    memory_rss: float = 0.0
    heap: float = 0.0
    threads: int = 0
    uptime: float = 0.0
    queue_depth: int = 0


@dataclass
class WebSocketMessage:
    type: str
    data: dict
    received_at: float


@dataclass
class DashboardSnapshot:
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
    def uptime(self) -> float:
        return self.get("system.uptime_seconds", 0) or 0

    @property
    def health_score(self) -> float:
        return self.get("health_score", 0) or 0

    @property
    def total_ticks(self) -> int:
        return self.get("throughput.cumulative.ticks", 0) or 0

    @property
    def total_messages(self) -> int:
        return self.get("throughput.cumulative.messages", 0) or 0

    @property
    def total_packets(self) -> int:
        return self.get("throughput.cumulative.packets", 0) or 0

    @property
    def bytes_received(self) -> int:
        return self.get("network.bytes_received", 0) or 0

    @property
    def bytes_transmitted(self) -> int:
        return self.get("network.bytes_transmitted", 0) or 0

    @property
    def trades_per_second(self) -> float:
        return self.get("throughput.trades_per_sec", 0) or 0

    @property
    def ticks_per_second(self) -> float:
        return self.get("throughput.ticks_per_sec", 0) or 0

    @property
    def packets_per_second(self) -> float:
        return self.get("throughput.packets_per_sec", 0) or 0

    @property
    def broadcasts_per_second(self) -> float:
        return self.get("throughput.broadcasts_per_sec", 0) or 0

    @property
    def messages_per_second(self) -> float:
        return self.get("throughput.messages_per_sec", 0) or 0

    @property
    def active_sessions(self) -> int:
        return self.get("sessions.active_sessions", 0) or 0

    @property
    def active_subscriptions(self) -> int:
        return self.get("sessions.active_subscriptions", 0) or 0

    @property
    def queue_depth(self) -> int:
        return self.get("queues.incoming_depth", 0) or 0

    @property
    def latency(self) -> float:
        return self.get("performance.processing.average", 0) or 0

    @property
    def exchange_latency(self) -> float:
        return self.get("performance.exchange.average", 0) or 0

    @property
    def cpu(self) -> float:
        return self.get("system.cpu_usage", 0) or 0

    @property
    def memory(self) -> float:
        return self.get("system.memory_rss", 0) or 0

    @property
    def heap(self) -> float:
        return self.get("system.heap_usage", 0) or 0

    @property
    def thread_count(self) -> int:
        return self.get("system.thread_count", 0) or 0

    @property
    def db_connected(self) -> bool:
        return self.get("database.status") == "connected" or self.get("db_connected") is True

    @property
    def db_insert_latency(self) -> float:
        return self.get("database.insert_latency_ms", 0) or 0

    @property
    def db_writes_per_sec(self) -> float:
        return self.get("database.writes_per_sec", 0) or 0


# ──────────────────────────────────────────────
# HTTP Session (connection pooling + retry)
# ──────────────────────────────────────────────

class HTTPSession:
    def __init__(self, config: Config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._redirected_base: Optional[str] = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        )
        await self._detect_redirects()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    @property
    def base(self) -> str:
        return self._redirected_base or self.config.rest_base

    async def _detect_redirects(self) -> None:
        try:
            async with self.session.get(
                self.config.rest_base + "/health",
                allow_redirects=True,
                max_redirects=self.config.max_redirects,
            ) as resp:
                final_url = str(resp.url)
                if final_url.rstrip("/") != (self.config.rest_base + "/health").rstrip("/"):
                    scheme = resp.url.scheme
                    host = resp.url.host
                    port = resp.url.port
                    base = f"{scheme}://{host}"
                    if port and port not in (80, 443):
                        base += f":{port}"
                    self._redirected_base = base
                    logger.info("REST redirect detected: %s → %s", self.config.rest_base, self._redirected_base)
                else:
                    logger.info("No REST redirect: %s", self.config.rest_base)
        except Exception as e:
            logger.warning("Redirect detection failed: %s", e)

    async def request(
        self, method: str, path: str, **kwargs
    ) -> Tuple[int, Any, float]:
        url = self.base + path
        start = time.monotonic()
        try:
            async with self.session.request(method, url, **kwargs) as resp:
                elapsed = (time.monotonic() - start) * 1000
                status = resp.status
                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    data = {"_raw": text[:500]}
                return status, data, elapsed
        except asyncio.TimeoutError:
            return -1, {"error": "timeout"}, (time.monotonic() - start) * 1000
        except aiohttp.ClientConnectorError as e:
            return -2, {"error": f"connection_error: {e}"}, (time.monotonic() - start) * 1000
        except Exception as e:
            return -3, {"error": f"exception: {e}"}, (time.monotonic() - start) * 1000

    async def get(self, path: str, **kwargs) -> Tuple[int, Any, float]:
        return await self.request("GET", path, **kwargs)

    async def post(self, path: str, **kwargs) -> Tuple[int, Any, float]:
        return await self.request("POST", path, **kwargs)


# ──────────────────────────────────────────────
# Pipeline Validator
# ──────────────────────────────────────────────

class PipelineValidator:
    def __init__(self, http: HTTPSession, config: Config):
        self.http = http
        self.config = config
        self.report = PipelineReport()

    async def validate(self) -> PipelineReport:
        logger.info(bold("\n═══ Pipeline Validation ═══"))
        await self._check_service()
        await self._check_exchange()
        await self._check_feed_manager()
        await self._check_parser()
        await self._check_normalizer()
        await self._check_metrics_collector()
        await self._check_dashboard()
        await self._check_database()
        await self._check_sessions()
        await self._check_subscriptions()
        return self.report

    async def _check_service(self) -> None:
        status, data, elapsed = await self.http.get("/health")
        ok = status == 200
        detail = f"Status={status}, response={str(data)[:100]}"
        self.report.add("Service", ok, detail)

    async def _check_exchange(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/exchanges")
        if status == 200 and isinstance(data, list):
            connected = any(
                isinstance(ex, dict) and ex.get("connected") is True
                for ex in data
            ) if data else False
            detail = f"{len(data)} exchange(s), connected={connected}"
            self.report.add("Exchange", True, detail)
            if not connected and len(data) > 0:
                self.report.stages[-1].diagnostics.append("Exchanges registered but none connected")
        elif status == 200:
            self.report.add("Exchange", True, "0 exchanges registered (no feed instances)")
        else:
            self.report.add("Exchange", False, f"Status={status}: {str(data)[:100]}")

        # Check individual exchange detail
        exchanges_to_check = []
        if status == 200 and isinstance(data, list):
            for ex in data:
                if isinstance(ex, dict) and "exchange" in ex:
                    exchanges_to_check.append(ex["exchange"])
        for exchange in ["BINANCE", "COINBASE", "BYBIT", "OKX", "KRAKEN"]:
            if exchange not in exchanges_to_check:
                continue
            s, d, _ = await self.http.get(f"/api/v1/exchange/{exchange}")
            if s == 200:
                conn = d.get("connected", False)
                lag = d.get("feed_lag_ms", 0)
                latency = d.get("exchange_latency_ms", 0)
                self.report.stages[-1].diagnostics.append(
                    f"{exchange}: connected={conn}, lag={lag}ms, latency={latency}ms"
                )

    async def _check_feed_manager(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/feed/status")
        if status in (200, 500):
            detail = f"Status={status}, response={str(data)[:150]}"
            status_ok = data.get("status", "") if isinstance(data, dict) else ""
            ok = status == 200 and status_ok in ("active", "running", "connected")
            self.report.add("Feed Manager", ok, detail)
        else:
            self.report.add("Feed Manager", False, f"Unexpected status={status}")

    async def _check_parser(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/feed/live")
        if status == 200:
            pf = data.get("parse_failures", -1) if isinstance(data, dict) else -1
            cp = data.get("corrupted_packets", -1) if isinstance(data, dict) else -1
            ok = True
            diags = []
            diags.append(f"parse_failures={pf}, corrupted_packets={cp}")
            if isinstance(data, dict):
                health = data.get("health_score", 1)
                if health < 0.5:
                    ok = False
                    diags.append(f"health_score={health} below threshold")
            self.report.add("Parser", ok, f"Status={status}, {', '.join(diags)}")
        else:
            self.report.add("Parser", False, f"Status={status}")

    async def _check_normalizer(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/throughput/live")
        if status == 200 and isinstance(data, dict):
            ticks = data.get("ticks_per_sec", -1)
            trades = data.get("trades_per_sec", -1)
            ok = True
            diags = [f"ticks/sec={ticks}, trades/sec={trades}"]
            self.report.add("Normalizer", ok, f"Status={status}, {', '.join(diags)}")
        else:
            self.report.add("Normalizer", False, f"Status={status}")

    async def _check_metrics_collector(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/metrics/live")
        if status == 200 and isinstance(data, dict):
            has_data = len(data) > 2
            if has_data:
                diags = [f"keys={list(data.keys())[:8]}"]
            else:
                diags = ["empty response"]
            self.report.add("Metrics Collector", has_data, f"Status={status}, {', '.join(diags)}")
        else:
            self.report.add("Metrics Collector", False, f"Status={status}")

    async def _check_dashboard(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/dashboard")
        if status == 200 and isinstance(data, dict):
            has_health = "health_score" in data
            has_sections = any(k in data for k in ["system", "throughput", "sessions", "database", "performance"])
            ok = has_health
            diags = []
            diags.append(f"health_score={data.get('health_score', 'N/A')}")
            if has_sections:
                diags.append("has_sections")
            self.report.add("Dashboard", ok, f"Status={status}, {', '.join(diags)}")
        else:
            self.report.add("Dashboard", False, f"Status={status}")

    async def _check_database(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/database/live")
        if status == 200 and isinstance(data, dict):
            db_status = data.get("status", "unknown")
            active = data.get("active_connections", 0)
            writes = data.get("successful_writes", 0)
            ok = True
            diags = [f"status={db_status}, connections={active}, writes={writes}"]
            self.report.add("Database", ok, f"Status={status}, {', '.join(diags)}")
        else:
            status_h, data_h, _ = await self.http.get("/health")
            db_ok = isinstance(data_h, dict) and data_h.get("db") == "connected"
            self.report.add("Database", db_ok, f"health.db={data_h.get('db', 'N/A') if isinstance(data_h, dict) else 'N/A'}")

    async def _check_sessions(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/sessions/live")
        if status == 200 and isinstance(data, dict):
            active_sessions = data.get("active_sessions", 0)
            active_clients = data.get("active_clients", 0)
            total_connections = data.get("total_connections", 0)
            ok = True
            diags = [f"active_sessions={active_sessions}, active_clients={active_clients}, total_connections={total_connections}"]
            self.report.add("Sessions", ok, f"Status={status}, {', '.join(diags)}")
        else:
            self.report.add("Sessions", False, f"Status={status}")

    async def _check_subscriptions(self) -> None:
        status, data, elapsed = await self.http.get("/api/v1/sessions/live")
        if status == 200 and isinstance(data, dict):
            subs = data.get("active_subscriptions", 0)
            ok = True
            self.report.add("Subscriptions", ok, f"active_subscriptions={subs}")
        else:
            self.report.add("Subscriptions", False, f"Status={status}")


# ──────────────────────────────────────────────
# Dashboard Poller (Live Metrics Verification)
# ──────────────────────────────────────────────

class DashboardPoller:
    def __init__(self, http: HTTPSession, config: Config):
        self.http = http
        self.config = config
        self.snapshots: List[DashboardSnapshot] = []

    async def poll(self) -> List[DashboardSnapshot]:
        logger.info(bold("\n═══ Dashboard Live Polling (%ds) ═══"), self.config.dashboard_poll_seconds)
        deadline = time.time() + self.config.dashboard_poll_seconds
        while time.time() < deadline:
            status, data, elapsed = await self.http.get("/api/v1/dashboard")
            if status == 200 and isinstance(data, dict):
                self.snapshots.append(DashboardSnapshot(timestamp=time.time(), raw=data))
                self._log_tick()
            else:
                logger.warning("Dashboard poll failed: status=%s", status)
            await asyncio.sleep(self.config.dashboard_poll_interval)
        logger.info("Collected %d dashboard snapshots", len(self.snapshots))
        return self.snapshots

    def _log_tick(self) -> None:
        if not self.snapshots:
            return
        s = self.snapshots[-1]
        logger.info(
            "  dash: uptime=%.0fs  ticks=%d  msgs=%d  packets=%d  health=%.1f  sessions=%d  subs=%d  cpu=%.1f%%  mem=%.0f",
            s.uptime, s.total_ticks, s.total_messages, s.total_packets,
            s.health_score, s.active_sessions, s.active_subscriptions,
            s.cpu, s.memory,
        )

    def metrics_changed(self) -> bool:
        if len(self.snapshots) < 2:
            return False
        first = self.snapshots[0]
        last = self.snapshots[-1]
        keys_to_check = [
            "total_ticks", "total_messages", "total_packets",
            "bytes_received", "bytes_transmitted",
            "uptime", "active_sessions", "active_subscriptions",
        ]
        for key in keys_to_check:
            fv = getattr(first, key, 0) or 0
            lv = getattr(last, key, 0) or 0
            if lv > fv:
                return True
        return False

    def metrics_all_zero(self) -> bool:
        if not self.snapshots:
            return True
        for snap in self.snapshots:
            if (snap.total_ticks > 0 or snap.total_messages > 0 or
                    snap.total_packets > 0 or snap.bytes_received > 0 or
                    snap.active_subscriptions > 0 or snap.active_sessions > 0):
                return False
        return True

    def feed_producing(self) -> bool:
        for snap in self.snapshots:
            if snap.messages_per_second > 0 or snap.ticks_per_second > 0 or snap.packets_per_second > 0:
                return True
        return False

    def subscriptions_became_active(self) -> bool:
        return any(s.active_subscriptions > 0 for s in self.snapshots)

    def metrics_grew_over_period(self) -> bool:
        if len(self.snapshots) < 2:
            return False
        return (self.snapshots[-1].total_ticks > self.snapshots[0].total_ticks and
                self.snapshots[-1].total_messages > self.snapshots[0].total_messages)

    def rates_are_positive(self) -> dict:
        """Check if per-second rates are positive. Returns per-metric results."""
        if not self.snapshots:
            return {}
        last = self.snapshots[-1]
        return {
            "messages_per_sec": last.messages_per_second > 0,
            "ticks_per_sec": last.ticks_per_second > 0,
            "packets_per_sec": last.packets_per_second > 0,
            "trades_per_sec": last.trades_per_second > 0,
        }

    def bytes_are_increasing(self) -> bool:
        if len(self.snapshots) < 2:
            return False
        return (self.snapshots[-1].bytes_received > self.snapshots[0].bytes_received or
                self.snapshots[-1].bytes_transmitted > self.snapshots[0].bytes_transmitted)

    def rates_are_increasing(self) -> dict:
        """Check if per-second rates grew over the observation period. Returns per-metric results."""
        if len(self.snapshots) < 3:
            return {}
        mid = self.snapshots[len(self.snapshots) // 2]
        last = self.snapshots[-1]
        return {
            "messages_per_sec_grew": last.messages_per_second > mid.messages_per_second,
            "ticks_per_sec_grew": last.ticks_per_second > mid.ticks_per_second,
            "packets_per_sec_grew": last.packets_per_second > mid.packets_per_second,
        }


# ──────────────────────────────────────────────
# WebSocket Validator
# ──────────────────────────────────────────────

class WebSocketValidator:
    EXCHANGE = "BINANCE"
    SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    SUBSCRIPTION_TOPICS = ["ticker_", "price_", "trade_"]
    ADDITIONAL_TOPICS = ["orderbook_"]

    def __init__(self, config: Config):
        self.config = config
        self.all_messages: List[WebSocketMessage] = []
        self.market_data_messages: List[WebSocketMessage] = []
        self.dashboard_messages: List[WebSocketMessage] = []
        self.reconnects: int = 0
        self.dropped: int = 0
        self.duplicates: int = 0
        self.reconnect_events: List[float] = []
        self._received_types: Set[str] = set()
        self._message_times: List[float] = []
        self._errors: List[str] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self.symbols_seen: Set[str] = set()
        self.subscriptions_confirmed: bool = False
        self.first_market_data: Optional[float] = None
        self.last_market_data: Optional[float] = None
        self.topics_subscribed: List[str] = []
        self.exchange_switched: bool = False
        self.live_mode_enabled: bool = False
        self.bytes_received: int = 0
        self.bytes_sent: int = 0
        self.consecutive_timeouts: int = 0

    # ── protocol helpers ──────────────────────────────────

    @staticmethod
    def is_market_data(data: dict) -> bool:
        msg_type = str(data.get("type", data.get("topic", ""))).lower()
        if any(kw in msg_type for kw in ["ticker", "price", "trade", "orderbook", "quote", "book"]):
            return True
        has_symbol = "symbol" in data or "s" in data
        has_price = any(k in data for k in ["bid", "ask", "ltp", "last", "p", "close"])
        return has_symbol and has_price

    @staticmethod
    def is_dashboard_message(data: dict) -> bool:
        msg_type = str(data.get("type", "")).lower()
        if msg_type in ("dashboard", "metrics", "throughput", "feed_health", "system", "sessions",
                        "dashboard_snapshot", "live_metrics", "snapshot"):
            return True
        # Also detect by content: has aggregated fields
        return "health_score" in data or "active_sessions" in data or "active_subscriptions" in data

    # ── main flow ─────────────────────────────────────────

    async def _send_message(self, ws, msg) -> None:
        raw = json.dumps(msg) if isinstance(msg, dict) else msg
        await ws.send_str(raw)
        raw_bytes = len(raw.encode() if isinstance(raw, str) else raw)
        self.bytes_sent += raw_bytes

    async def _wait_for_market_data(
        self, ws, timeout: float = 30.0, min_messages: int = 5
    ) -> bool:
        """Actively wait until market data starts flowing or timeout expires."""
        logger.info("Waiting for market data (timeout=%ds, min_messages=%d)...",
                     timeout, min_messages)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
            except asyncio.TimeoutError:
                self.consecutive_timeouts += 1
                if self.consecutive_timeouts >= 4:
                    logger.warning("Multiple timeouts waiting for market data")
                continue

            self.consecutive_timeouts = 0

            if msg.type == aiohttp.WSMsgType.TEXT:
                raw = msg.data
                if isinstance(raw, bytes):
                    raw = raw.decode()
                self.bytes_received += len(raw.encode())
                self._message_times.append(time.time())
                try:
                    parsed = json.loads(raw)
                    if not isinstance(parsed, dict):
                        continue
                    msg_type = parsed.get("type", parsed.get("topic", "unknown"))
                    wsm = WebSocketMessage(type=msg_type, data=parsed, received_at=time.time())
                    self.all_messages.append(wsm)
                    self._received_types.add(msg_type)
                    if self.is_market_data(parsed):
                        self.market_data_messages.append(wsm)
                        if self.first_market_data is None:
                            self.first_market_data = time.time()
                            logger.info("First market data received! type=%s  symbol=%s",
                                        msg_type, parsed.get("symbol", parsed.get("s", "?")))
                        self.last_market_data = time.time()
                        symbol = parsed.get("symbol", parsed.get("s", ""))
                        if symbol:
                            self.symbols_seen.add(str(symbol))
                        if len(self.market_data_messages) >= min_messages:
                            logger.info("Received %d market data messages — data is flowing",
                                        len(self.market_data_messages))
                            return True
                    elif self.is_dashboard_message(parsed):
                        self.dashboard_messages.append(wsm)
                except (json.JSONDecodeError, TypeError):
                    self.dropped += 1
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warning("WebSocket closed while waiting for market data")
                return False
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self.dropped += 1

        had_some = len(self.market_data_messages) > 0
        if had_some:
            logger.info("Got %d market data messages before timeout",
                        len(self.market_data_messages))
        else:
            logger.warning("No market data received within %.0fs timeout", timeout)
        return had_some

    async def connect_and_subscribe(self, min_duration: float = 0) -> bool:
        logger.info(bold("\n═══ WebSocket Market Data Validation ═══"))
        logger.info("Connecting to %s", self.config.ws_url)
        logger.info("Exchange: %s, Symbols: %s, Topics: %s",
                     self.EXCHANGE, self.SYMBOLS, self.SUBSCRIPTION_TOPICS)

        self._session = aiohttp.ClientSession()

        try:
            ws = await self._session.ws_connect(
                self.config.ws_url,
                timeout=self.config.timeout,
                max_msg_size=0,
                heartbeat=5.0,
            )
            logger.info("WebSocket connected")
        except Exception as e:
            logger.warning("WebSocket connect failed: %s", e)
            self._errors.append(f"Connect failed: {e}")
            await self._session.close()
            return False

        all_topics = list(self.SUBSCRIPTION_TOPICS) + self.ADDITIONAL_TOPICS

        try:
            # Step 1 – switch to live mode (must precede subscriptions)
            await self._send_message(ws, "_Live")
            self.live_mode_enabled = True
            logger.info("Live mode enabled")
            await asyncio.sleep(0.2)

            # Step 2 – switch to desired exchange with symbols
            switch_msg = {
                "type": "switch_exchange",
                "exchange": self.EXCHANGE,
                "symbols": self.SYMBOLS,
            }
            await self._send_message(ws, switch_msg)
            self.exchange_switched = True
            logger.info("Switched to exchange: %s with symbols: %s",
                        self.EXCHANGE, self.SYMBOLS)
            await asyncio.sleep(0.3)

            # Step 3 – subscribe to all topics (both market data and monitoring)
            monitor_topics = [
                "dashboard", "metrics", "performance", "exchange",
                "system", "network", "feed", "queues",
            ]
            for topic in all_topics + monitor_topics:
                sub_msg = {"subscribe": topic}
                await self._send_message(ws, sub_msg)
                self.topics_subscribed.append(topic)
                await asyncio.sleep(0.05)

            # Also send structured subscribe messages with symbols
            for topic in self.SUBSCRIPTION_TOPICS:
                sub_msg = {
                    "subscribe": topic,
                    "symbols": self.SYMBOLS,
                }
                await self._send_message(ws, sub_msg)
                await asyncio.sleep(0.05)

            logger.info("Subscribed to %d topics (market data + monitoring)",
                        len(self.topics_subscribed))

            # Step 4 – wait for actual market data to begin flowing
            market_data_timeout = max(self.config.websocket_wait_seconds, 30)
            market_data_ok = await self._wait_for_market_data(
                ws, timeout=market_data_timeout, min_messages=3
            )

            if not market_data_ok:
                logger.warning("No market data received within %ds",
                               market_data_timeout)
                await ws.close()
                return False

            # Step 5 – continue collecting data for the remaining duration
            effective_wait = max(self.config.websocket_wait_seconds, min_duration)
            elapsed = time.time() - (self.first_market_data or time.time())
            remaining = max(effective_wait - elapsed, 5.0)
            deadline = time.time() + remaining
            last_activity = time.time()
            no_data_warning_logged = False

            while time.time() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                except asyncio.TimeoutError:
                    elapsed_idle = time.time() - last_activity
                    if elapsed_idle > 15 and not no_data_warning_logged:
                        logger.warning("No WebSocket messages for %.0f s", elapsed_idle)
                        no_data_warning_logged = True
                    continue

                last_activity = time.time()
                no_data_warning_logged = False

                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    self.bytes_received += len(raw.encode())
                    self._message_times.append(time.time())
                    try:
                        parsed = json.loads(raw)
                        if not isinstance(parsed, dict):
                            continue

                        msg_type = parsed.get("type", parsed.get("topic", "unknown"))
                        wsm = WebSocketMessage(type=msg_type, data=parsed, received_at=time.time())
                        self.all_messages.append(wsm)
                        self._received_types.add(msg_type)

                        if self.is_market_data(parsed):
                            self.market_data_messages.append(wsm)
                            if self.first_market_data is None:
                                self.first_market_data = time.time()
                                logger.info("First market data received! type=%s  symbol=%s",
                                            msg_type, parsed.get("symbol", parsed.get("s", "?")))
                            self.last_market_data = time.time()
                            symbol = parsed.get("symbol", parsed.get("s", ""))
                            if symbol:
                                self.symbols_seen.add(str(symbol))
                        elif self.is_dashboard_message(parsed):
                            self.dashboard_messages.append(wsm)
                            subs = (parsed.get("active_subscriptions") or
                                    parsed.get("subscriptions") or
                                    parsed.get("sessions", {}).get("active_subscriptions") or
                                    parsed.get("sessions", {}).get("subscriptions") or 0)
                            if isinstance(subs, (int, float)) and subs > 0:
                                if not self.subscriptions_confirmed:
                                    logger.info("Active subscriptions confirmed via dashboard: %d", int(subs))
                                    self.subscriptions_confirmed = True

                        if msg_type in ("subscribed", "subscription", "subscription_response",
                                        "confirm", "subscription_update", "ack"):
                            self.subscriptions_confirmed = True
                            logger.info("Subscription confirmed by server: %s", msg_type)

                    except (json.JSONDecodeError, TypeError):
                        self.dropped += 1

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    self.reconnects += 1
                    self.reconnect_events.append(time.time())
                    logger.warning("WebSocket closed unexpectedly")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.dropped += 1

            await ws.close()

            has_market = len(self.market_data_messages) > 0
            has_any = len(self.all_messages) > 0
            logger.info("WebSocket done: %d total, %d market-data, %d dashboard, %d symbols",
                        len(self.all_messages), len(self.market_data_messages),
                        len(self.dashboard_messages), len(self.symbols_seen))
            logger.info("Subscriptions confirmed: %s", self.subscriptions_confirmed)
            if not has_market:
                logger.warning("Received %d messages but none are market data — pipeline may not be flowing",
                               len(self.all_messages))
            return has_market

        except Exception as e:
            self._errors.append(f"WebSocket error: {e}")
            return False
        finally:
            await self._session.close()

    @property
    def messages(self) -> List[WebSocketMessage]:
        return self.all_messages

    @property
    def message_rate(self) -> float:
        if len(self._message_times) < 2:
            return 0.0
        duration = self._message_times[-1] - self._message_times[0]
        return len(self._message_times) / duration if duration > 0 else 0.0

    @property
    def avg_interval_ms(self) -> float:
        if len(self._message_times) < 2:
            return 0.0
        intervals = [self._message_times[i+1] - self._message_times[i] for i in range(len(self._message_times)-1)]
        return (statistics.mean(intervals) * 1000) if intervals else 0.0

    @property
    def market_data_rate(self) -> float:
        if self.first_market_data is None or self.last_market_data is None:
            return 0.0
        duration = self.last_market_data - self.first_market_data
        return len(self.market_data_messages) / duration if duration > 0 else 0.0

    def has_type(self, t: str) -> bool:
        return any(m.type == t for m in self.all_messages)

    def summary(self) -> str:
        parts = [
            f"messages={len(self.all_messages)}",
            f"market_data={len(self.market_data_messages)}",
            f"types={len(self._received_types)}",
            f"rate={self.message_rate:.1f}/s",
            f"interval={self.avg_interval_ms:.0f}ms",
            f"reconnects={self.reconnects}",
            f"subs_confirmed={self.subscriptions_confirmed}",
            f"symbols={len(self.symbols_seen)}",
            f"topics={len(self.topics_subscribed)}",
            f"bytes_recv={self.bytes_received}",
        ]
        return ", ".join(parts)


# ──────────────────────────────────────────────
# Latency Benchmarker
# ──────────────────────────────────────────────

class LatencyBenchmarker:
    ENDPOINTS = [
        ("GET", "/health"),
        ("GET", "/api/v1/dashboard"),
        ("GET", "/api/v1/metrics/live"),
        ("GET", "/api/v1/metrics/history"),
        ("GET", "/api/v1/performance/live"),
        ("GET", "/api/v1/performance/history"),
        ("GET", "/api/v1/throughput/live"),
        ("GET", "/api/v1/throughput/history"),
        ("GET", "/api/v1/feed/live"),
        ("GET", "/api/v1/feed/history"),
        ("GET", "/api/v1/exchanges"),
        ("GET", "/api/v1/queues/live"),
        ("GET", "/api/v1/queues/history"),
        ("GET", "/api/v1/network/live"),
        ("GET", "/api/v1/network/history"),
        ("GET", "/api/v1/database/live"),
        ("GET", "/api/v1/database/history"),
        ("GET", "/api/v1/system/live"),
        ("GET", "/api/v1/system/history"),
        ("GET", "/api/v1/sessions/live"),
        ("GET", "/api/v1/sessions/history"),
        ("GET", "/api/v1/alerts"),
        ("GET", "/api/v1/config"),
        ("GET", "/api/v1/thresholds"),
        ("GET", "/api/v1/analytics"),
        ("GET", "/api/v1/timeline"),
        ("GET", "/api/v1/dependencies"),
        ("GET", "/api/v1/topology"),
        ("GET", "/api/v1/search"),
        ("GET", "/api/v1/feed/status"),
        ("GET", "/api/v1/clients"),
    ]

    def __init__(self, http: HTTPSession, config: Config):
        self.http = http
        self.config = config
        self.stats: Dict[str, LatencyStats] = {}

    async def benchmark(self) -> Dict[str, LatencyStats]:
        logger.info(bold("\n═══ Latency Benchmark (%d iterations each) ═══"), self.config.benchmark_iterations)
        for method, path in self.ENDPOINTS:
            key = f"{method} {path}"
            ls = LatencyStats(endpoint=path, method=method)
            for _ in range(self.config.benchmark_iterations):
                status, data, elapsed = await self.http.get(path)
                if status > 0:
                    ls.samples.append(elapsed)
            self.stats[key] = ls
            if ls.samples:
                logger.info("  %-45s avg=%7.1fms  p50=%7.1fms  p95=%7.1fms  p99=%7.1fms",
                            key, ls.avg, ls.p50, ls.p95, ls.p99)
        return self.stats

    def get_slowest(self) -> Tuple[str, float]:
        worst = max(self.stats.items(), key=lambda kv: kv[1].avg if kv[1].samples else 0)
        return worst[0], worst[1].avg

    def get_fastest(self) -> Tuple[str, float]:
        best = min(
            ((k, v) for k, v in self.stats.items() if v.samples),
            key=lambda kv: kv[1].avg,
            default=("", 0.0),
        )
        return best[0], best[1].avg

    def ranking(self) -> List[Tuple[str, float]]:
        return sorted(
            [(k, v.avg) for k, v in self.stats.items() if v.samples],
            key=lambda x: x[1],
        )

    def overall_stats(self) -> Dict[str, float]:
        all_lats = []
        for ls in self.stats.values():
            all_lats.extend(ls.samples)
        if not all_lats:
            return {}
        s_all = sorted(all_lats)
        n = len(s_all)
        def pct(p):
            idx = int(n * p / 100)
            return s_all[min(idx, n - 1)]
        return {
            "avg": statistics.mean(all_lats),
            "p50": pct(50),
            "p90": pct(90),
            "p95": pct(95),
            "p99": pct(99),
            "max": max(all_lats),
            "min": min(all_lats),
        }


# ──────────────────────────────────────────────
# Resource Monitor
# ──────────────────────────────────────────────

class ResourceMonitor:
    def __init__(self, http: HTTPSession, config: Config):
        self.http = http
        self.config = config
        self.snapshots: List[ResourceSnapshot] = []

    async def monitor(self) -> List[ResourceSnapshot]:
        logger.info(bold("\n═══ Resource Monitoring (%ds) ═══"), self.config.resource_monitor_seconds)
        deadline = time.time() + self.config.resource_monitor_seconds
        while time.time() < deadline:
            snap = await self._capture()
            self.snapshots.append(snap)
            logger.info("  cpu=%5.1f%%  mem=%8.0f  heap=%8.0f  threads=%3d  uptime=%7.0fs  queue=%4d",
                        snap.cpu, snap.memory_rss, snap.heap, snap.threads, snap.uptime, snap.queue_depth)
            await asyncio.sleep(self.config.resource_monitor_interval)
        logger.info("Collected %d resource snapshots", len(self.snapshots))
        self._analyze()
        return self.snapshots

    async def _capture(self) -> ResourceSnapshot:
        snap = ResourceSnapshot(timestamp=time.time())
        status, data, _ = await self.http.get("/api/v1/system/live")
        if status == 200 and isinstance(data, dict):
            snap.cpu = data.get("cpu_usage", 0) or 0
            snap.memory_rss = data.get("memory_rss", 0) or 0
            snap.heap = data.get("heap_usage", 0) or 0
            snap.threads = data.get("thread_count", 0) or 0
            snap.uptime = data.get("uptime_seconds", 0) or 0

        status2, data2, _ = await self.http.get("/api/v1/queues/live")
        if status2 == 200 and isinstance(data2, dict):
            snap.queue_depth = data2.get("incoming_depth", 0) or 0

        return snap

    def _analyze(self) -> None:
        if len(self.snapshots) < 3:
            logger.info("  Resource analysis: insufficient data")
            return
        first = self.snapshots[0]
        last = self.snapshots[-1]

        rss_growth = last.memory_rss - first.memory_rss
        rss_growth_pct = (rss_growth / first.memory_rss * 100) if first.memory_rss > 0 else 0
        if rss_growth_pct > 20:
            logger.warning(warn("  ⚠ MEMORY GROWTH: RSS increased %.1f%% (%.0f → %.0f)"),
                           rss_growth_pct, first.memory_rss, last.memory_rss)

        cpu_vals = [s.cpu for s in self.snapshots]
        avg_cpu = statistics.mean(cpu_vals)
        max_cpu = max(cpu_vals)
        if max_cpu > 80:
            logger.warning(warn("  ⚠ CPU SPIKE: max=%.1f%% avg=%.1f%%"), max_cpu, avg_cpu)

        queue_vals = [s.queue_depth for s in self.snapshots]
        max_q = max(queue_vals)
        last_q = queue_vals[-1]
        if max_q > 1000 and last_q > max_q * 0.8:
            logger.warning(warn("  ⚠ QUEUE BUILDUP: max=%d recent=%d"), max_q, last_q)

        mem_vals = [s.memory_rss for s in self.snapshots]
        if len(mem_vals) > 5:
            trend = statistics.linear_regression(range(len(mem_vals)), mem_vals)
            if trend.slope > mem_vals[0] * 0.01:
                logger.warning(warn("  ⚠ MEMORY LEAK DETECTED: RSS increasing (slope=%.1f/step)"), trend.slope)

        logger.info("  Resource summary: avg_cpu=%.1f%%  max_cpu=%.1f%%  avg_mem=%.0f  rss_growth=%.1f%%",
                    avg_cpu, max_cpu, statistics.mean(mem_vals), rss_growth_pct)


# ──────────────────────────────────────────────
# Stress Tester
# ──────────────────────────────────────────────

@dataclass
class StressResult:
    total: int = 0
    success: int = 0
    failed: int = 0
    timeout: int = 0
    latencies: List[float] = field(default_factory=list)
    duration: float = 0.0
    errors: List[str] = field(default_factory=list)

    @property
    def avg_latency(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0.0

    @property
    def throughput(self) -> float:
        return self.total / self.duration if self.duration > 0 else 0.0

    @property
    def failure_rate(self) -> float:
        return (self.failed / self.total * 100) if self.total > 0 else 0.0

    @property
    def p50(self) -> float:
        return self._perc(50)

    @property
    def p95(self) -> float:
        return self._perc(95)

    @property
    def p99(self) -> float:
        return self._perc(99)

    def _perc(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        idx = int(len(s) * p / 100)
        return s[min(idx, len(s) - 1)]


class StressTester:
    ENDPOINTS = [
        "/health", "/api/v1/dashboard", "/api/v1/metrics/live",
        "/api/v1/performance/live", "/api/v1/throughput/live",
        "/api/v1/feed/live", "/api/v1/exchanges",
        "/api/v1/system/live", "/api/v1/sessions/live",
        "/api/v1/queues/live", "/api/v1/network/live",
        "/api/v1/database/live", "/api/v1/config",
        "/api/v1/alerts", "/api/v1/topology",
    ]

    def __init__(self, http: HTTPSession, config: Config):
        self.http = http
        self.config = config

    async def run(self) -> StressResult:
        result = StressResult()
        logger.info(bold("\n═══ Stress Test (%d req/s for %ds) ═══"),
                    self.config.stress_rate, self.config.stress_duration)

        start = time.monotonic()
        deadline = start + self.config.stress_duration
        tasks: List[asyncio.Task] = []
        completed = 0

        async def _hit(path: str) -> None:
            nonlocal completed
            try:
                status, data, elapsed = await self.http.get(path)
                result.latencies.append(elapsed)
                if status > 0:
                    result.success += 1
                else:
                    result.failed += 1
                completed += 1
            except asyncio.TimeoutError:
                result.timeout += 1
                result.failed += 1
            except Exception as e:
                result.errors.append(str(e)[:100])
                result.failed += 1
            finally:
                result.total += 1

        while time.monotonic() < deadline:
            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                break
            batch_size = min(self.config.stress_rate, max(1, int(self.config.stress_rate * remaining)))
            batch_paths = [self.ENDPOINTS[i % len(self.ENDPOINTS)] for i in range(batch_size)]
            batch = [asyncio.create_task(_hit(p)) for p in batch_paths]
            tasks.extend(batch)
            await asyncio.sleep(min(1.0, remaining))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        result.duration = time.monotonic() - start

        logger.info("  Results: %d requests, %.1f req/s, failure_rate=%.1f%%, avg=%.1fms, p95=%.1fms, p99=%.1fms",
                    result.total, result.throughput, result.failure_rate,
                    result.avg_latency, result.p95, result.p99)
        if result.errors:
            for err in result.errors[:5]:
                logger.warning("  Error: %s", err)

        return result


# ──────────────────────────────────────────────
# REST vs WebSocket Consistency Checker
# ──────────────────────────────────────────────

class ConsistencyChecker:
    def __init__(self, http: HTTPSession):
        self.http = http

    def _deep_get(self, d: dict, path: str, default: Any = None) -> Any:
        parts = path.split(".")
        cur = d
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p, {})
            else:
                return default
        return cur if cur != {} else default

    async def check(self, dash_snapshots: List[DashboardSnapshot], ws_messages: List[WebSocketMessage]) -> List[str]:
        mismatches: List[str] = []
        if not dash_snapshots or not ws_messages:
            return mismatches

        latest_dash = dash_snapshots[-1]
        relevant_ws = [m for m in ws_messages if m.type in ("dashboard", "metrics")]
        if not relevant_ws:
            return mismatches

        latest_ws = relevant_ws[-1]
        ws_data = latest_ws.data

        comparisons = [
            ("health_score", "health_score", 5.0),
            ("uptime", "system.uptime_seconds", 5.0),
            ("active_sessions", "sessions.active_sessions", 0),
            ("queue_depth", "queues.incoming_depth", 0),
            ("cpu", "system.cpu_usage", 10.0),
        ]

        for name, dash_path, threshold in comparisons:
            dash_val = DashboardSnapshot.get(latest_dash, dash_path, 0)
            dash_val = dash_val if dash_val is not None else 0
            try:
                dash_val = float(dash_val)
            except (ValueError, TypeError):
                dash_val = 0.0

            ws_keys_to_try = [
                name, dash_path,
                name.replace("_", ""),
                dash_path.split(".")[-1],
            ]
            ws_val = None
            for k in set(ws_keys_to_try):
                v = self._deep_get(ws_data, k)
                if v is not None:
                    ws_val = v
                    break
            if ws_val is None:
                ws_val = 0.0
            try:
                ws_val = float(ws_val)
            except (ValueError, TypeError):
                ws_val = 0.0

            diff = abs(dash_val - ws_val)
            if diff > threshold:
                mismatches.append(f"{name}: REST={dash_val} WS={ws_val} diff={diff:.1f}")

        return mismatches


# ──────────────────────────────────────────────
# Tick Validator
# ──────────────────────────────────────────────

class TickValidator:
    def __init__(self):
        self.ticks_seen: List[dict] = []
        self._symbol_counts: Dict[str, int] = defaultdict(int)
        self._stale_count: int = 0
        self._invalid_count: int = 0

    @staticmethod
    def _is_tick_message(m: WebSocketMessage) -> bool:
        """Detect market-data messages by type prefix or by content fields."""
        type_lower = str(m.type).lower()
        if any(kw in type_lower for kw in ["ticker", "price", "trade", "orderbook", "quote", "book", "tick"]):
            return True
        d = m.data
        return bool(d.get("symbol") or d.get("s")) and bool(
            d.get("bid") or d.get("ask") or d.get("ltp") or d.get("last") or d.get("p") or d.get("close")
        )

    def validate(self, messages: List[WebSocketMessage]) -> Dict[str, Any]:
        tick_msgs = [m for m in messages if self._is_tick_message(m)]
        results = {
            "total_ticks": len(tick_msgs),
            "stale_ticks": 0,
            "duplicate_ticks": 0,
            "missing_timestamps": 0,
            "invalid_prices": 0,
            "invalid_spreads": 0,
            "unique_symbols": set(),
            "exchanges": set(),
            "errors": [],
        }

        seen_ids: Set[str] = set()

        for msg in tick_msgs:
            d = msg.data
            symbol = d.get("symbol", d.get("s", "unknown"))
            exchange = d.get("exchange", d.get("ex", "unknown"))
            results["unique_symbols"].add(str(symbol))
            results["exchanges"].add(str(exchange))
            self._symbol_counts[str(symbol)] += 1

            # Check timestamp
            ts = d.get("timestamp", d.get("t", d.get("time", 0)))
            if not ts or ts == 0:
                results["missing_timestamps"] += 1
                results["errors"].append(f"Missing timestamp on {symbol}")

            # Check prices
            bid = d.get("bid", d.get("b", -1))
            ask = d.get("ask", d.get("a", -1))
            ltp = d.get("ltp", d.get("p", d.get("last", -1)))
            try:
                bid = float(bid) if bid is not None else -1
                ask = float(ask) if ask is not None else -1
                ltp = float(ltp) if ltp is not None else -1
            except (ValueError, TypeError):
                results["invalid_prices"] += 1
                continue

            if (bid > 0 and ask > 0 and bid >= ask) or (bid > 0 and ask <= 0):
                if bid > 0 and ask > 0 and bid >= ask:
                    results["invalid_spreads"] += 1

            if ltp <= 0 and bid <= 0 and ask <= 0:
                results["invalid_prices"] += 1

            # Check for stale ticks (timestamp older than 60s)
            now = time.time()
            if ts and isinstance(ts, (int, float)) and ts > 1e9:
                if now - ts > 60:
                    results["stale_ticks"] += 1

            # Check duplicates
            dup_key = f"{symbol}|{exchange}|{ts}"
            if dup_key in seen_ids:
                results["duplicate_ticks"] += 1
            else:
                seen_ids.add(dup_key)

        results["unique_symbols"] = list(results["unique_symbols"])
        results["exchanges"] = list(results["exchanges"])
        return results


# ──────────────────────────────────────────────
# Feed Validator
# ──────────────────────────────────────────────

class FeedValidator:
    def __init__(self, http: HTTPSession):
        self.http = http

    async def validate(self, dash_snapshots: List[DashboardSnapshot],
                       ws_market_data_count: int = 0,
                       subscriptions_confirmed: bool = False) -> Tuple[bool, List[str]]:
        diagnostics: List[str] = []

        if not dash_snapshots:
            return False, ["No dashboard data available"]

        first = dash_snapshots[0]
        last = dash_snapshots[-1]

        messages_sec = last.messages_per_second
        ticks_sec = last.ticks_per_second
        packets_sec = last.packets_per_second

        total_ticks_inc = last.total_ticks - first.total_ticks
        total_msgs_inc = last.total_messages - first.total_messages
        total_pkts_inc = last.total_packets - first.total_packets
        bytes_inc = max(0, last.bytes_received - first.bytes_received)

        diagnostics.append(f"messages/sec={messages_sec:.1f}")
        diagnostics.append(f"ticks/sec={ticks_sec:.1f}")
        diagnostics.append(f"packets/sec={packets_sec:.1f}")
        diagnostics.append(f"total_ticks_delta={total_ticks_inc}")
        diagnostics.append(f"total_msgs_delta={total_msgs_inc}")
        diagnostics.append(f"total_packets_delta={total_pkts_inc}")
        diagnostics.append(f"bytes_received_delta={bytes_inc}")
        diagnostics.append(f"active_subscriptions={last.active_subscriptions}")

        rates_increasing = any([
            messages_sec > 0,
            ticks_sec > 0,
            packets_sec > 0,
        ])

        totals_increasing = any([
            total_ticks_inc > 0,
            total_msgs_inc > 0,
            total_pkts_inc > 0,
            bytes_inc > 0,
        ])

        has_flow = rates_increasing or totals_increasing

        if ws_market_data_count > 0:
            diagnostics.append(f"ws_market_data_messages={ws_market_data_count}")

        has_subscriptions = last.active_subscriptions > 0
        if not has_subscriptions:
            if not subscriptions_confirmed:
                diagnostics.append("No active subscriptions on dashboard and WS not confirmed")
            else:
                diagnostics.append("WS confirmed active subs but dashboard reports none")

        if not rates_increasing and totals_increasing:
            diagnostics.append("Total counts growing but per-second rates are zero")
        elif not has_flow:
            diagnostics.append("Feed connected but no market data flowing")

        if ws_market_data_count > 0 and not has_flow:
            diagnostics.append("WARNING: WS received market data but dashboard reports no flow")

        passed = (has_flow or ws_market_data_count > 0 or subscriptions_confirmed)
        if not passed:
            diagnostics.append("All feed checks failed — no market data detected through any path")

        return passed, diagnostics


# ──────────────────────────────────────────────
# Report Generator
# ──────────────────────────────────────────────

class ReportGenerator:
    def __init__(self, config: Config):
        self.config = config

    def generate_text(self, ctx: dict) -> str:
        pipe: PipelineReport = ctx.get("pipeline", PipelineReport())
        dash_snapshots: List[DashboardSnapshot] = ctx.get("dash_snapshots", [])
        ws_validator: Optional[WebSocketValidator] = ctx.get("ws_validator")
        bench_stats: Dict[str, Any] = ctx.get("bench_stats", {})
        resource_snapshots: List[ResourceSnapshot] = ctx.get("resource_snapshots", [])
        feed_alive: bool = ctx.get("feed_alive", False)
        feed_diags: List[str] = ctx.get("feed_diags", [])
        stress_result: Optional[StressResult] = ctx.get("stress_result")
        mismatches: List[str] = ctx.get("mismatches", [])
        tick_results: Dict[str, Any] = ctx.get("tick_results", {})
        overall_health: float = ctx.get("overall_health", 0.0)
        production_ready: bool = ctx.get("production_ready", False)
        timestamp: str = ctx.get("timestamp", datetime.now(timezone.utc).isoformat())
        duration: float = ctx.get("duration", 0.0)

        lines: List[str] = []

        def _section(title: str):
            lines.append(f"\n  {bold(title)}")
            lines.append("  " + "-" * (len(title) + 2))

        def _status(passed: bool) -> str:
            return color_status(passed)

        def _val(name: str, value: Any, unit: str = "") -> str:
            return f"  {name:<25} {value}{unit}"

        # Header
        lines.append("")
        lines.append("=" * 56)
        lines.append(bold("  PROTYPE DATAFEED HEALTH REPORT"))
        lines.append("=" * 56)
        lines.append(f"  Timestamp:      {timestamp}")
        lines.append(f"  Target:         {self.config.rest_base}")
        lines.append(f"  Duration:       {duration:.1f}s")
        lines.append("")

        # 1. Pipeline stages
        _section("Pipeline Stages")
        for stage in pipe.stages:
            diag_str = f" — {stage.detail}" if stage.detail else ""
            lines.append(f"  {_status(stage.passed)}  {stage.name}{diag_str}")

        lines.append("")

        # 2. Live Feed
        _section("Live Feed")
        if dash_snapshots:
            last = dash_snapshots[-1]
            lines.append(_val("Messages/sec", f"{last.messages_per_second:.1f}"))
            lines.append(_val("Ticks/sec", f"{last.ticks_per_second:.1f}"))
            lines.append(_val("Packets/sec", f"{last.packets_per_second:.1f}"))
            lines.append(_val("Subscriptions", last.active_subscriptions))
            lines.append(_val("Sessions", last.active_sessions))
            lines.append(_val("Total Messages", last.total_messages))
            lines.append(_val("Total Ticks", last.total_ticks))
            lines.append(_val("Total Packets", last.total_packets))
            lines.append(_val("Bytes Received", last.bytes_received))
            lines.append(_val("Bytes Transmitted", last.bytes_transmitted))
        lines.append("")

        # 3. Latency
        _section("Latency (ms)")
        if bench_stats:
            ov = bench_stats
            lines.append(_val("Average", f"{ov.get('avg', 0):.1f}", " ms"))
            lines.append(_val("P50", f"{ov.get('p50', 0):.1f}", " ms"))
            lines.append(_val("P90", f"{ov.get('p90', 0):.1f}", " ms"))
            lines.append(_val("P95", f"{ov.get('p95', 0):.1f}", " ms"))
            lines.append(_val("P99", f"{ov.get('p99', 0):.1f}", " ms"))
            lines.append(_val("Max", f"{ov.get('max', 0):.1f}", " ms"))
            lines.append(_val("Min", f"{ov.get('min', 0):.1f}", " ms"))
        lines.append("")

        # 4. Resources
        _section("Resources")
        if resource_snapshots:
            cpu_vals = [s.cpu for s in resource_snapshots]
            mem_vals = [s.memory_rss for s in resource_snapshots]
            thread_vals = [s.threads for s in resource_snapshots]
            heap_vals = [s.heap for s in resource_snapshots]
            rss_vals = [s.memory_rss for s in resource_snapshots]
            lines.append(_val("CPU (avg/max)", f"{statistics.mean(cpu_vals):.1f}% / {max(cpu_vals):.1f}%"))
            lines.append(_val("Memory RSS (avg)", f"{statistics.mean(mem_vals):.0f}"))
            lines.append(_val("Heap (avg)", f"{statistics.mean(heap_vals):.0f}"))
            lines.append(_val("Threads (avg)", f"{statistics.mean(thread_vals):.0f}"))
            if len(rss_vals) > 1:
                growth = rss_vals[-1] - rss_vals[0]
                growth_pct = (growth / rss_vals[0] * 100) if rss_vals[0] > 0 else 0
                lines.append(_val("RSS Growth", f"{growth_pct:.1f}%"))
        lines.append("")

        # 5. WebSocket
        if ws_validator:
            _section("WebSocket")
            lines.append(_val("Total Messages", len(ws_validator.all_messages)))
            lines.append(_val("Market Data Msgs", len(ws_validator.market_data_messages)))
            lines.append(_val("Dashboard Snapshots", len(ws_validator.dashboard_messages)))
            lines.append(_val("Symbols Seen", list(ws_validator.symbols_seen)))
            lines.append(_val("Topics Subscribed", len(ws_validator.topics_subscribed)))
            lines.append(_val("Subscriptions Confirmed", ws_validator.subscriptions_confirmed))
            lines.append(_val("Exchange Switched", ws_validator.exchange_switched))
            lines.append(_val("Live Mode Enabled", ws_validator.live_mode_enabled))
            lines.append(_val("Message Types", len(ws_validator._received_types)))
            lines.append(_val("Message Rate", f"{ws_validator.message_rate:.1f}/s"))
            lines.append(_val("Market Data Rate", f"{ws_validator.market_data_rate:.1f}/s"))
            lines.append(_val("Avg Interval", f"{ws_validator.avg_interval_ms:.0f}ms"))
            lines.append(_val("Bytes Received", ws_validator.bytes_received))
            lines.append(_val("Bytes Sent", ws_validator.bytes_sent))
            lines.append(_val("Reconnects", ws_validator.reconnects))
            lines.append(_val("Dropped", ws_validator.dropped))
            lines.append(_val("Duplicates", ws_validator.duplicates))
            lines.append("")

        # 6. Stress Test
        if stress_result:
            _section("Stress Test")
            lines.append(_val("Total Requests", stress_result.total))
            lines.append(_val("Throughput", f"{stress_result.throughput:.1f}", " req/s"))
            lines.append(_val("Failure Rate", f"{stress_result.failure_rate:.1f}%"))
            lines.append(_val("Avg Latency", f"{stress_result.avg_latency:.1f}", " ms"))
            lines.append(_val("P95", f"{stress_result.p95:.1f}", " ms"))
            lines.append(_val("P99", f"{stress_result.p99:.1f}", " ms"))
            lines.append("")

        # 7. Problems Detected
        _section("Problems Detected")
        problems: List[str] = []

        dashboard_reachable = any(s.name == "Dashboard" and s.passed for s in pipe.stages)
        problems.append(f"  {'✓' if dashboard_reachable else '✗'} Dashboard reachable")

        db_ok = any(s.name == "Database" and s.passed for s in pipe.stages)
        problems.append(f"  {'✓' if db_ok else '✗'} Database connected")

        exchange_ok = any(s.name == "Exchange" and s.passed for s in pipe.stages)
        problems.append(f"  {'✓' if exchange_ok else '✗'} Exchange connection")

        ws_ok = ws_validator and len(ws_validator.messages) > 0
        problems.append(f"  {'✓' if ws_ok else '✗'} WebSocket data flow")

        ws_market_ok = ws_validator and len(ws_validator.market_data_messages) > 0
        problems.append(f"  {'✓' if ws_market_ok else '✗'} Market data via WebSocket")

        subs_ok = (ws_validator and ws_validator.subscriptions_confirmed) or \
                  (dash_snapshots and dash_snapshots[-1].active_subscriptions > 0)
        problems.append(f"  {'✓' if subs_ok else '✗'} Active subscriptions confirmed")

        if not feed_alive:
            problems.append(f"  {'✗'} No live ticks")
            problems.append(f"  {'✗'} Throughput remains zero")

        metrics_ok = any(s.name == "Metrics Collector" and s.passed for s in pipe.stages)
        if not metrics_ok:
            problems.append(f"  {'✗'} Metrics Collector not updating")

        consistency_ok = any(s.name == "Consistency" and s.passed for s in pipe.stages)
        if not consistency_ok:
            problems.append(f"  {'✗'} REST/WebSocket metrics out of sync")

        if dash_snapshots and dash_snapshots[-1].active_sessions == 0:
            problems.append(f"  {'✗'} No active sessions")

        if dash_snapshots and dash_snapshots[-1].active_subscriptions == 0:
            problems.append(f"  {'✗'} No active subscriptions on dashboard")

        if not feed_alive:
            problems.append(f"  {'✗'} Feed pipeline appears stalled")

        if mismatches:
            for m in mismatches:
                problems.append(f"  {'✗'} REST/WS mismatch: {m}")

        lines.extend(problems)
        lines.append("")

        # 8. Overall Health
        _section("Overall Health")
        lines.append(_val("Health Score", f"{overall_health:.0f}", " / 100"))
        lines.append(_val("Production Ready", "YES" if production_ready else "NO"))
        lines.append("")
        lines.append("=" * 56)

        return "\n".join(lines)

    def generate_json(self, ctx: dict) -> str:
        pipe: PipelineReport = ctx.get("pipeline", PipelineReport())
        snapshots: List[DashboardSnapshot] = ctx.get("dash_snapshots", [])
        ws_validator: Optional[WebSocketValidator] = ctx.get("ws_validator")
        bench_stats: Dict[str, Any] = ctx.get("bench_stats", {})
        resource_snapshots: List[ResourceSnapshot] = ctx.get("resource_snapshots", [])
        feed_alive = ctx.get("feed_alive", False)
        stress_result: Optional[StressResult] = ctx.get("stress_result")
        mismatches: List[str] = ctx.get("mismatches", [])
        tick_results: Dict[str, Any] = ctx.get("tick_results", {})
        overall_health = ctx.get("overall_health", 0.0)
        production_ready = ctx.get("production_ready", False)
        timestamp = ctx.get("timestamp", datetime.now(timezone.utc).isoformat())
        duration = ctx.get("duration", 0.0)

        report: dict = {
            "timestamp": timestamp,
            "target": self.config.rest_base,
            "duration_seconds": round(duration, 2),
            "pipeline": {
                "stages": [
                    {"name": s.name, "passed": s.passed, "detail": s.detail,
                     "diagnostics": s.diagnostics}
                    for s in pipe.stages
                ],
                "passed_count": pipe.passed_count(),
                "failed_count": pipe.failed_count(),
                "all_passed": pipe.all_passed(),
            },
            "feed": {
                "alive": feed_alive,
            },
            "latency": bench_stats,
            "mismatches": mismatches,
            "health_score": round(overall_health, 1),
            "production_ready": production_ready,
        }

        if snapshots:
            last = snapshots[-1]
            report["dashboard"] = {
                "uptime": last.uptime,
                "health_score": last.health_score,
                "total_ticks": last.total_ticks,
                "total_messages": last.total_messages,
                "total_packets": last.total_packets,
                "messages_per_second": last.messages_per_second,
                "ticks_per_second": last.ticks_per_second,
                "packets_per_second": last.packets_per_second,
                "active_sessions": last.active_sessions,
                "active_subscriptions": last.active_subscriptions,
                "cpu": last.cpu,
                "memory_rss": last.memory,
                "queue_depth": last.queue_depth,
            }

            inc = {
                "total_ticks": snapshots[-1].total_ticks - snapshots[0].total_ticks,
                "total_messages": snapshots[-1].total_messages - snapshots[0].total_messages,
                "total_packets": snapshots[-1].total_packets - snapshots[0].total_packets,
                "bytes_received": snapshots[-1].bytes_received - snapshots[0].bytes_received,
            }
            report["dashboard"]["deltas"] = inc

        if ws_validator:
            report["websocket"] = {
                "total_messages": len(ws_validator.all_messages),
                "market_data_messages": len(ws_validator.market_data_messages),
                "dashboard_snapshots": len(ws_validator.dashboard_messages),
                "symbols_seen": list(ws_validator.symbols_seen),
                "topics_subscribed": ws_validator.topics_subscribed,
                "subscriptions_confirmed": ws_validator.subscriptions_confirmed,
                "exchange_switched": ws_validator.exchange_switched,
                "live_mode_enabled": ws_validator.live_mode_enabled,
                "message_types": list(ws_validator._received_types),
                "message_rate": round(ws_validator.message_rate, 2),
                "market_data_rate": round(ws_validator.market_data_rate, 2),
                "avg_interval_ms": round(ws_validator.avg_interval_ms, 2),
                "bytes_received": ws_validator.bytes_received,
                "bytes_sent": ws_validator.bytes_sent,
                "reconnects": ws_validator.reconnects,
                "dropped": ws_validator.dropped,
                "duplicates": ws_validator.duplicates,
            }

        if resource_snapshots:
            cpu_vals = [s.cpu for s in resource_snapshots]
            mem_vals = [s.memory_rss for s in resource_snapshots]
            report["resources"] = {
                "cpu_avg": round(statistics.mean(cpu_vals), 2),
                "cpu_max": round(max(cpu_vals), 2),
                "memory_rss_avg": round(statistics.mean(mem_vals), 2),
                "memory_rss_max": round(max(mem_vals), 2),
                "rss_growth_pct": round(
                    ((mem_vals[-1] - mem_vals[0]) / mem_vals[0] * 100) if mem_vals[0] > 0 else 0, 2
                ),
                "snapshots": [asdict(s) for s in resource_snapshots],
            }

        if stress_result:
            report["stress_test"] = {
                "total_requests": stress_result.total,
                "successful": stress_result.success,
                "failed": stress_result.failed,
                "timeouts": stress_result.timeout,
                "throughput_req_per_sec": round(stress_result.throughput, 2),
                "failure_rate_pct": round(stress_result.failure_rate, 2),
                "avg_latency_ms": round(stress_result.avg_latency, 2),
                "p50_ms": round(stress_result.p50, 2),
                "p95_ms": round(stress_result.p95, 2),
                "p99_ms": round(stress_result.p99, 2),
                "errors": stress_result.errors[:10],
            }

        if tick_results:
            report["ticks"] = tick_results

        return json.dumps(report, indent=2, default=str)

    def generate_markdown(self, ctx: dict) -> str:
        text = self.generate_text(ctx)
        md = text.replace(ANSI.GREEN, "**").replace(ANSI.RED, "**").replace(ANSI.YELLOW, "**")
        md = md.replace(ANSI.BOLD, "**").replace(ANSI.RESET, "**").replace(ANSI.CYAN, "").replace(ANSI.GREY, "")
        md = md.replace("PASS", "✅ **PASS**").replace("FAIL", "❌ **FAIL**")
        return md

    def generate_html(self, ctx: dict) -> str:
        jsondata = self.generate_json(ctx)
        text_report = self.generate_text(ctx)
        escaped_text = text_report.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>")

        pipe: PipelineReport = ctx.get("pipeline", PipelineReport())
        overall_health = ctx.get("overall_health", 0.0)
        production_ready = ctx.get("production_ready", False)

        stage_rows = ""
        for s in pipe.stages:
            color = "#28a745" if s.passed else "#dc3545"
            label = "PASS" if s.passed else "FAIL"
            stage_rows += f"<tr><td>{s.name}</td><td style='color:{color};font-weight:bold'>{label}</td><td>{s.detail}</td></tr>\n"

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PROTYPE Datafeed Health Report</title>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f5f5f5; margin: 20px; color: #333; }}
  .container {{ max-width: 900px; margin: 0 auto; background: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
  h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
  h2 {{ color: #2c3e50; margin-top: 30px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
  th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #ddd; }}
  th {{ background: #f8f9fa; font-weight: 600; }}
  .score {{ font-size: 2em; text-align: center; padding: 20px; }}
  .ready-yes {{ color: #28a745; font-weight: bold; }}
  .ready-no {{ color: #dc3545; font-weight: bold; }}
  .pre {{ font-family: 'Courier New', monospace; background: #f8f9fa; padding: 15px; border-radius: 5px; font-size: 13px; line-height: 1.5; white-space: pre-wrap; overflow-x: auto; }}
  .footer {{ margin-top: 30px; text-align: center; color: #888; font-size: 0.9em; }}
  .pass {{ color: #28a745; }}
  .fail {{ color: #dc3545; }}
</style>
</head>
<body>
<div class="container">
  <h1>PROTYPE Datafeed Health Report</h1>
  <p><strong>Target:</strong> {self.config.rest_base}</p>
  <p><strong>Timestamp:</strong> {ctx.get('timestamp', 'N/A')}</p>
  <p><strong>Duration:</strong> {ctx.get('duration', 0):.1f}s</p>

  <div class="score">
    <span>Health Score: <strong style="color:{'#28a745' if overall_health >= 70 else '#dc3545'}">{overall_health:.0f}</strong> / 100</span><br>
    <span class="{'ready-yes' if production_ready else 'ready-no'}">
      {'✅ Production Ready' if production_ready else '❌ NOT Production Ready'}
    </span>
  </div>

  <h2>Pipeline Stages</h2>
  <table>
    <tr><th>Stage</th><th>Status</th><th>Detail</th></tr>
    {stage_rows}
  </table>

  <h2>Raw Report</h2>
  <div class="pre">{escaped_text}</div>

  <div class="footer">Generated by PROTYPE Datafeed Validation Suite</div>
</div>
</body>
</html>"""

    def write_reports(self, ctx: dict) -> Dict[str, str]:
        report_dir = Path(self.config.report_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        paths: Dict[str, str] = {}

        for ext, gen_fn in [("txt", self.generate_text), ("json", self.generate_json),
                            (".md", self.generate_markdown), ("html", self.generate_html)]:
            ext_clean = ext.lstrip(".")
            fname = f"datafeed_health_{ts}.{ext_clean}"
            fpath = report_dir / fname
            content = gen_fn(ctx)
            fpath.write_text(content)
            paths[ext_clean] = str(fpath)
            logger.info("Report written: %s", fpath)

        return paths


# ──────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────

class Orchestrator:
    def __init__(self, config: Config):
        self.config = config
        self.http: Optional[HTTPSession] = None
        self.start_time: float = 0.0

    async def run(self) -> int:
        self.start_time = time.time()
        timestamp = datetime.now(timezone.utc).isoformat()
        logger.info(bold("\n  ╔══════════════════════════════════════════╗"))
        logger.info(bold("  ║  PROTYPE DATAFEED VALIDATION SUITE      ║"))
        logger.info(bold("  ╚══════════════════════════════════════════╝"))
        logger.info("  Target: %s", self.config.rest_base)
        logger.info("  WS:     %s", self.config.ws_url)
        logger.info("")

        ctx: dict = {"timestamp": timestamp}

        async with HTTPSession(self.config) as http:
            self.http = http

            # 1. Pipeline Validation (quick REST health checks)
            pipeline = PipelineValidator(http, self.config)
            ctx["pipeline"] = await pipeline.validate()

            # 2. WebSocket connection, subscription, and market data wait
            #    WS subscribes to real market symbols so dashboard sees non-zero metrics
            ws_validator = WebSocketValidator(self.config)
            poller = DashboardPoller(http, self.config)

            ws_ok = False
            if not self.config.skip_websocket:
                logger.info(bold("\n═══ Establishing Live Market Data Feed ═══"))
                ws_ok = await ws_validator.connect_and_subscribe(
                    min_duration=self.config.dashboard_poll_seconds + 10
                )
                if ws_ok:
                    logger.info("Market data is flowing — beginning dashboard validation")
                else:
                    logger.warning(warn("  ⚠ WebSocket did not receive market data — pipeline may be stalled"))
            else:
                logger.info("WebSocket validation skipped")

            # 3. Dashboard Polling (started after market data is confirmed)
            ctx["dash_snapshots"] = await poller.poll()

            # Classify messages for tick validation
            tick_validator = TickValidator()
            if ws_validator:
                ctx["ws_validator"] = ws_validator
                ctx["tick_results"] = tick_validator.validate(ws_validator.market_data_messages)
            else:
                ctx["ws_validator"] = None
                ctx["tick_results"] = {}
                ctx["mismatches"] = []

            # 4. Feed Validation (checks dashboard metrics grew)
            feed_val = FeedValidator(http)
            ctx["feed_alive"], ctx["feed_diags"] = await feed_val.validate(
                ctx["dash_snapshots"],
                ws_market_data_count=len(ws_validator.market_data_messages) if ws_validator else 0,
                subscriptions_confirmed=ws_validator.subscriptions_confirmed if ws_validator else False,
            )

            # 5. Pipeline failure analysis
            has_ws_market_data = ws_validator and len(ws_validator.market_data_messages) > 0
            has_ws_any = ws_validator and len(ws_validator.all_messages) > 0
            subs_confirmed = ws_validator and ws_validator.subscriptions_confirmed

            if not has_ws_any and not ws_ok:
                ctx["pipeline"].add("WebSocket", False,
                                    "WebSocket connected but received zero messages",
                                    ["Check if server is running and reachable"])
                ctx["pipeline"].add("Market Data", False,
                                    "No WebSocket messages received — pipeline dead",
                                    [f"WS URL: {self.config.ws_url}"])

            elif not has_ws_market_data:
                ctx["pipeline"].add("Market Data", False,
                                    "WebSocket subscribed but received zero market-data messages",
                                    [f"Total WS messages: {len(ws_validator.all_messages) if ws_validator else 0}",
                                     f"Topics subscribed: {ws_validator.topics_subscribed if ws_validator else []}"])

            elif poller.metrics_all_zero():
                logger.warning(warn("  ⚠ LIVE PIPELINE FAILED — all dashboard metrics remain zero"))
                ctx["pipeline"].add("Market Data", False,
                                    "All dashboard metrics remained zero for entire observation period",
                                    ["No ticks, no messages, no packets flowing despite WS market data"])

            elif not ctx["feed_alive"]:
                ctx["pipeline"].add("Market Data", False,
                                    "Feed validation failed — dashboard metrics not increasing",
                                    ctx["feed_diags"])

            elif ws_validator and not ws_validator.subscriptions_confirmed:
                ctx["pipeline"].add("Subscriptions", False,
                                    "Server never confirmed active subscriptions",
                                    [f"Symbols seen via WS: {ws_validator.symbols_seen}",
                                     f"Dashboard messages received: {len(ws_validator.dashboard_messages)}"])

            else:
                ctx["pipeline"].add("Market Data", True,
                                    f"Dashboard flowing, WS received {len(ws_validator.market_data_messages) if ws_validator else 0} market-data messages from {len(ws_validator.symbols_seen) if ws_validator else 0} symbols")
                if ws_validator:
                    ctx["pipeline"].add("Subscriptions", True,
                                        f"Active subscriptions confirmed via dashboard metrics")

            # REST/WS consistency check
            if ws_validator and ws_validator.all_messages and ctx["dash_snapshots"]:
                checker = ConsistencyChecker(http)
                ctx["mismatches"] = await checker.check(ctx["dash_snapshots"], ws_validator.all_messages)
                if ctx["mismatches"]:
                    for m in ctx["mismatches"]:
                        logger.warning(warn(f"  ⚠ REST/WS mismatch: {m}"))
                    ctx["pipeline"].add("Consistency", False,
                                        f"{len(ctx['mismatches'])} REST/WS metric mismatches",
                                        ctx["mismatches"])
                else:
                    ctx["pipeline"].add("Consistency", True, "REST and WebSocket metrics in sync")
            else:
                ctx["mismatches"] = []

            # 6. Latency Benchmark
            bench = LatencyBenchmarker(http, self.config)
            bench_results = await bench.benchmark()
            ctx["bench_stats"] = bench.overall_stats()
            ctx["bench_results"] = bench_results

            # 6. Resource Monitoring (60s)
            if not self.config.skip_resource_monitor:
                monitor = ResourceMonitor(http, self.config)
                ctx["resource_snapshots"] = await monitor.monitor()
            else:
                ctx["resource_snapshots"] = []

            # 7. Stress Test
            if not self.config.skip_stress:
                stress = StressTester(http, self.config)
                ctx["stress_result"] = await stress.run()
            else:
                ctx["stress_result"] = None

        # 8. Calculate health score
        ctx = self._calculate_health(ctx)
        ctx["duration"] = time.time() - self.start_time

        # 9. Generate reports
        generator = ReportGenerator(self.config)
        ctx["report_paths"] = generator.write_reports(ctx)

        # 10. Print final report
        print(generator.generate_text(ctx))

        production_ready = ctx.get("production_ready", False)
        return 0 if production_ready else 1

    def _calculate_health(self, ctx: dict) -> dict:
        pipe: PipelineReport = ctx.get("pipeline", PipelineReport())
        dash_snapshots: List[DashboardSnapshot] = ctx.get("dash_snapshots", [])
        ws_validator: Optional[WebSocketValidator] = ctx.get("ws_validator")
        feed_alive: bool = ctx.get("feed_alive", False)
        mismatches: List[str] = ctx.get("mismatches", [])
        stress_result: Optional[StressResult] = ctx.get("stress_result")

        score = 100.0

        # Pipeline deductions
        for s in pipe.stages:
            if not s.passed:
                deductions = {
                    "Service": 15, "Exchange": 15, "Feed Manager": 15, "Parser": 10,
                    "Normalizer": 10, "Metrics Collector": 15, "Dashboard": 10,
                    "WebSocket": 10, "Database": 15, "Sessions": 5, "Subscriptions": 5,
                    "Market Data": 20, "Consistency": 10,
                }
                score -= deductions.get(s.name, 10)

        # Feed deduction
        if not feed_alive:
            score -= 20

        # WS market data deduction
        if ws_validator:
            if not ws_validator.market_data_messages:
                score -= 25
            elif not ws_validator.subscriptions_confirmed:
                score -= 10

        # Mismatch deduction
        if mismatches:
            score -= len(mismatches) * 5

        # Stress test deduction
        if stress_result and stress_result.failure_rate > 5:
            score -= min(20, stress_result.failure_rate)

        # Dashboard data check
        if dash_snapshots:
            last = dash_snapshots[-1]
            if last.active_subscriptions == 0 and ws_validator and not ws_validator.subscriptions_confirmed:
                score -= 15
            if last.total_ticks == 0:
                score -= 10
            if last.messages_per_second == 0:
                score -= 10

        score = max(0, score)

        market_data_ok = (
            feed_alive or
            (ws_validator is not None and len(ws_validator.market_data_messages) > 0)
        )
        production_ready = score >= 70 and pipe.critical_failures() == [] and market_data_ok

        ctx["overall_health"] = score
        ctx["production_ready"] = production_ready
        return ctx


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def parse_args(argv: Optional[List[str]] = None) -> Config:
    import argparse
    p = argparse.ArgumentParser(
        description="PROTYPE Datafeed Production Validation Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python tests/stress_test.py                           # Full validation
  python tests/stress_test.py --ci                      # CI/CD mode (exit 0/1)
  python tests/stress_test.py --report-dir ./reports    # Custom report directory
  python tests/stress_test.py --skip-stress             # Skip load testing
  python tests/stress_test.py --stress-rate 500         # High stress rate
""",
    )
    p.add_argument("--rest-base", default="https://datafeed.fun", help="REST API base URL")
    p.add_argument("--ws-url", default="wss://datafeed.fun/ws/live", help="WebSocket URL")
    p.add_argument("--dashboard-poll", type=int, default=30, help="Dashboard poll duration (s)")
    p.add_argument("--dashboard-interval", type=float, default=1.0, help="Dashboard poll interval (s)")
    p.add_argument("--ws-wait", type=int, default=15, help="WebSocket observation window (s)")
    p.add_argument("--resource-monitor", type=int, default=60, help="Resource monitor duration (s)")
    p.add_argument("--stress-rate", type=int, default=50, help="Stress test request rate (req/s)")
    p.add_argument("--stress-duration", type=int, default=10, help="Stress test duration (s)")
    p.add_argument("--bench-iterations", type=int, default=5, help="Latency benchmark iterations per endpoint")
    p.add_argument("--timeout", type=int, default=15, help="Request timeout (s)")
    p.add_argument("--report-dir", default="reports", help="Report output directory")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    p.add_argument("--ci", action="store_true", help="CI mode (exit 0=pass, 1=fail)")
    p.add_argument("--skip-stress", action="store_true", help="Skip stress test")
    p.add_argument("--skip-websocket", action="store_true", help="Skip WebSocket validation")
    p.add_argument("--skip-resource-monitor", action="store_true", help="Skip resource monitoring")

    args = p.parse_args(argv)

    c = Config(
        rest_base=args.rest_base,
        ws_url=args.ws_url,
        dashboard_poll_seconds=args.dashboard_poll,
        dashboard_poll_interval=args.dashboard_interval,
        websocket_wait_seconds=args.ws_wait,
        resource_monitor_seconds=args.resource_monitor,
        resource_monitor_interval=2.0,
        stress_rate=args.stress_rate,
        stress_duration=args.stress_duration,
        benchmark_iterations=args.bench_iterations,
        timeout=args.timeout,
        report_dir=args.report_dir,
        verbose=args.verbose,
        ci_mode=args.ci,
        skip_stress=args.skip_stress,
        skip_websocket=args.skip_websocket,
        skip_resource_monitor=args.skip_resource_monitor,
    )
    return c


async def main_async(argv: Optional[List[str]] = None) -> int:
    config = parse_args(argv)

    if config.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        orch = Orchestrator(config)
        return await orch.run()
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        return 1
    except Exception as e:
        logger.error("Fatal error: %s", e)
        import traceback
        traceback.print_exc()
        return 1


def main(argv: Optional[List[str]] = None) -> int:
    return asyncio.run(main_async(argv))


if __name__ == "__main__":
    sys.exit(main())
