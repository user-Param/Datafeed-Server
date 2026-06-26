#!/usr/bin/env python3
"""
Production-grade API validation, stress testing, benchmarking, and monitoring tool
for the datafeed C++ backend.  Verifies all 30+ REST endpoints + WebSocket before
deployment.  Thread-safe, non-crashing, generates a complete report.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("api_stress_test")

# ═══════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════


class StressLevel(Enum):
    SMOKE = 1
    NORMAL = 10
    HEAVY = 50
    EXTREME = 200


@dataclass
class Config:
    host: str = "localhost"
    port: int = 4444
    base_url: str = ""
    threads: int = 10
    duration: int = 60
    iterations: int = 100
    timeout: int = 10
    output: str = ""
    websocket: bool = True
    verbose: bool = False
    stress: StressLevel = StressLevel.NORMAL

    @property
    def base(self) -> str:
        if self.base_url:
            return self.base_url.rstrip("/")
        return f"http://{self.host}:{self.port}"


def parse_args(argv: Optional[List[str]] = None) -> Config:
    p = argparse.ArgumentParser(
        description="API Stress Tester for datafeed backend"
    )
    p.add_argument("--host", default="localhost", help="Server host")
    p.add_argument("--port", type=int, default=4444, help="Server port")
    p.add_argument("--base-url", default="", help="Full base URL override")
    p.add_argument("--threads", type=int, default=10, help="Concurrent threads")
    p.add_argument("--duration", type=int, default=60, help="Test duration (seconds)")
    p.add_argument("--iterations", type=int, default=100, help="Requests per endpoint")
    p.add_argument("--timeout", type=int, default=10, help="Request timeout")
    p.add_argument("--output", default="", help="Output file for JSON report")
    p.add_argument("--no-websocket", dest="websocket", action="store_false",
                   help="Skip WebSocket tests")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    p.add_argument("--stress", choices=["smoke", "normal", "heavy", "extreme"],
                   default="normal", help="Stress level presets")
    args = p.parse_args(argv)

    stress_map = {
        "smoke": StressLevel.SMOKE,
        "normal": StressLevel.NORMAL,
        "heavy": StressLevel.HEAVY,
        "extreme": StressLevel.EXTREME,
    }
    sl = stress_map[args.stress]
    threads = args.threads if args.stress == "normal" else sl.value

    return Config(
        host=args.host, port=args.port, base_url=args.base_url,
        threads=threads, duration=args.duration, iterations=args.iterations,
        timeout=args.timeout, output=args.output, websocket=args.websocket,
        verbose=args.verbose, stress=sl,
    )


# ═══════════════════════════════════════════════════════════
# Endpoint Registry
# ═══════════════════════════════════════════════════════════


class EndpointType(Enum):
    LIVE = auto()
    HISTORY = auto()
    ACTION = auto()
    SEARCH = auto()
    HEALTH = auto()


@dataclass
class EndpointDef:
    method: str
    path: str
    expected_status: int = 200
    expected_fields: List[str] = field(default_factory=list)
    type: EndpointType = EndpointType.LIVE
    body: Optional[Dict[str, Any]] = None
    query_params: Optional[Dict[str, str]] = None
    description: str = ""
    timeout: Optional[int] = None  # per-endpoint override


def build_registry() -> List[EndpointDef]:
    """All 30+ endpoints the backend exposes."""
    r: List[EndpointDef] = []

    def add(method, path, status=200, fields=None, etype=EndpointType.LIVE,
            desc="", qp=None):
        r.append(EndpointDef(
            method=method, path=path, expected_status=status,
            expected_fields=fields or [], type=etype, description=desc,
            query_params=qp,
        ))

    # ── Dashboard ──
    add("GET", "/api/v1/dashboard",
        fields=["service_uptime", "health_score", "db_connected",
                "system", "performance", "throughput", "exchanges",
                "queues", "network", "database", "feed", "sessions",
                "recent_alerts"],
        desc="Aggregated system overview")
    # ── Metrics ──
    add("GET", "/api/v1/metrics/live",
        fields=["system", "performance", "throughput", "exchanges",
                "queues", "network", "database", "feed", "sessions"],
        desc="Live metrics")
    add("GET", "/api/v1/metrics/history",
        etype=EndpointType.HISTORY, desc="Historical metrics",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})  # noqa
    # ── Performance ──
    add("GET", "/api/v1/performance/live",
        fields=["exchange", "parsing", "normalization", "processing",
                "serialization", "broadcast", "socket_send"],
        desc="Live per-category latency")
    add("GET", "/api/v1/performance/history",
        etype=EndpointType.HISTORY, desc="Historical performance",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Throughput ──
    add("GET", "/api/v1/throughput/live",
        fields=["messages_per_sec", "packets_per_sec", "bytes_per_sec",
                "trades_per_sec", "ticks_per_sec", "orderbook_updates_per_sec",
                "subscriptions_per_sec", "broadcasts_per_sec",
                "database_reads_per_sec", "database_writes_per_sec",
                "cumulative"],
        desc="Live throughput rates")
    add("GET", "/api/v1/throughput/history",
        etype=EndpointType.HISTORY, desc="Historical throughput",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Feed ──
    add("GET", "/api/v1/feed/live",
        fields=["health_score", "packet_drops", "duplicate_packets",
                "out_of_order_packets", "sequence_gaps", "missing_ticks",
                "invalid_messages", "corrupted_packets", "parse_failures",
                "stale_feed"],
        desc="Live feed health")
    add("GET", "/api/v1/feed/history",
        etype=EndpointType.HISTORY, desc="Historical feed health",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Exchange ──
    add("GET", "/api/v1/exchanges", desc="List exchanges")
    add("GET", "/api/v1/exchange/BINANCE",
        fields=["exchange", "connected", "uptime_seconds", "reconnect_count",
                "feed_lag_ms", "exchange_latency_ms", "messages_received",
                "messages_dropped", "parse_errors", "websocket_disconnects",
                "heartbeat_failures", "stale", "health", "status"],
        desc="Exchange details")
    add("GET", "/api/v1/exchange/BINANCE/history",
        etype=EndpointType.HISTORY, desc="Exchange history",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Queues ──
    add("GET", "/api/v1/queues/live",
        fields=["incoming_depth", "outgoing_depth", "serialization_depth",
                "max_incoming_depth", "max_outgoing_depth",
                "max_serialization_depth", "overflow_count", "backpressure",
                "wait_time_ms", "processing_time_ms"],
        desc="Live queue depths")
    add("GET", "/api/v1/queues/history",
        etype=EndpointType.HISTORY, desc="Historical queues",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Network ──
    add("GET", "/api/v1/network/live",
        fields=["tcp_reconnects", "socket_errors", "read_errors",
                "write_errors", "tls_handshake_failures", "bytes_transmitted",
                "bytes_received", "socket_rtt_ms", "bandwidth_bps",
                "connection_failures"],
        desc="Live network metrics")
    add("GET", "/api/v1/network/history",
        etype=EndpointType.HISTORY, desc="Historical network",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Database ──
    add("GET", "/api/v1/database/live",
        fields=["active_connections", "insert_latency_ms", "query_latency_ms",
                "transaction_count", "successful_writes", "failed_writes",
                "reads_per_sec", "writes_per_sec", "queue_waiting",
                "connection_failures"],
        desc="Live database metrics")
    add("GET", "/api/v1/database/history",
        etype=EndpointType.HISTORY, desc="Historical database",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── System ──
    add("GET", "/api/v1/system/live",
        fields=["cpu_usage", "memory_rss", "peak_rss", "virtual_memory",
                "heap_usage", "memory_growth", "thread_count",
                "uptime_seconds"],
        desc="Live system metrics")
    add("GET", "/api/v1/system/history",
        etype=EndpointType.HISTORY, desc="Historical system metrics",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Sessions ──
    add("GET", "/api/v1/sessions/live",
        fields=["active_clients", "active_sessions", "active_subscriptions",
                "authentication_failures", "reconnect_count",
                "avg_session_duration_ms", "longest_session_duration_ms",
                "total_connections", "total_disconnections"],
        desc="Live session metrics")
    add("GET", "/api/v1/sessions/history",
        etype=EndpointType.HISTORY, desc="Historical sessions",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Alerts ──
    add("GET", "/api/v1/alerts", desc="Active alerts")
    add("GET", "/api/v1/alerts/history", desc="Alert history")
    # POST /ack requires a real alert_id; tested via negative test
    # ── Audit ──
    add("GET", "/api/v1/audit", desc="Audit events",
        qp={"from": "1000", "to": str(int(time.time() * 1000))})
    # ── Config ──
    add("GET", "/api/v1/config",
        fields=["version", "adapter_version", "deployment_id",
                "schema_version", "runtime"],
        desc="Runtime configuration")
    # ── Thresholds ──
    add("GET", "/api/v1/thresholds", desc="Metric thresholds")
    # ── Analytics ──
    add("GET", "/api/v1/analytics",
        fields=["average_latency", "worst_latency", "peak_throughput",
                "average_throughput", "peak_cpu", "average_cpu",
                "peak_memory", "average_memory", "exchange_uptime_seconds",
                "most_active_exchange", "health_score",
                "database_performance"],
        desc="Calculated KPIs")
    # ── Timeline ──
    add("GET", "/api/v1/timeline", desc="Recent operational events")
    # ── Dependencies ──
    add("GET", "/api/v1/dependencies",
        fields=["database", "exchange_connections", "websocket_layer",
                "snapshot_scheduler", "state_manager", "persistence_layer",
                "monitoring_system"],
        desc="Dependency health")
    # ── Topology ──
    add("GET", "/api/v1/topology",
        fields=["server", "router", "controllers", "services",
                "state_manager", "metrics_collector", "database",
                "exchanges"],
        desc="System topology")
    # ── Search ──
    add("GET", "/api/v1/search",
        fields=["exchanges", "clients", "sessions", "alerts", "events"],
        desc="Cross-entity search",
        qp={"q": "test"})
    # ── Health & Status ──
    add("GET", "/health",
        fields=["status", "db", "feed_instances"],
        desc="Health check",
        etype=EndpointType.HEALTH)
    add("GET", "/api/v1/feed/status",
        fields=["instance_id", "exchange", "status", "uptime"],
        desc="Feed status",
        etype=EndpointType.HEALTH)
    # ── Clients ──
    add("GET", "/api/v1/clients", desc="List clients",
        etype=EndpointType.HEALTH)

    return r


# ═══════════════════════════════════════════════════════════
# Validator
# ═══════════════════════════════════════════════════════════


class ValidationResult:
    def __init__(self):
        self.valid: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def merge(self, other: ValidationResult) -> None:
        if not other.valid:
            self.valid = False
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)


def validate_json(data: Any, endpoint: EndpointDef) -> ValidationResult:
    """Validate a parsed JSON response against endpoint expectations."""
    result = ValidationResult()

    if data is None:
        result.valid = False
        result.errors.append("Response is null/None")
        return result

    if isinstance(data, list):
        # List endpoints: exchanges, alerts, history, etc.
        if not endpoint.expected_fields:
            return result
        # If fields are specified, each list item should contain them
        for i, item in enumerate(data[:5]):
            if isinstance(item, dict):
                for f in endpoint.expected_fields:
                    if f not in item:
                        result.warnings.append(
                            f"Item[{i}] missing field '{f}'")
        return result

    if not isinstance(data, dict):
        result.valid = False
        result.errors.append(
            f"Expected dict/list, got {type(data).__name__}")
        return result

    for field in endpoint.expected_fields:
        if field not in data:
            result.valid = False
            result.errors.append(f"Missing required field: '{field}'")
        elif data[field] is None:
            result.warnings.append(f"Field '{field}' is null")

    return result


# ═══════════════════════════════════════════════════════════
# HTTP Test Runner
# ═══════════════════════════════════════════════════════════


@dataclass
class RequestResult:
    endpoint: str
    method: str
    status_code: int
    elapsed: float
    valid: bool
    validation_errors: List[str]
    validation_warnings: List[str]
    error: Optional[str] = None
    response_size: int = 0


class HTTPTestRunner:
    """Sends requests, validates responses, collects results."""

    def __init__(self, config: Config, registry: List[EndpointDef]):
        self.config = config
        self.registry = registry
        self.session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        s = requests.Session()
        retry = Retry(total=2, backoff_factor=0.1,
                      status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100,
                              pool_maxsize=200)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        return s

    def _build_url(self, ep: EndpointDef) -> str:
        url = f"{self.config.base}{ep.path}"
        if ep.query_params:
            sep = "&" if "?" in url else "?"
            qs = "&".join(f"{k}={v}" for k, v in ep.query_params.items())
            url = f"{url}{sep}{qs}"
        return url

    def run_single(self, ep: EndpointDef, index: int = 0) -> RequestResult:
        url = self._build_url(ep)
        method = ep.method.upper()
        timeout = ep.timeout or self.config.timeout
        start = time.monotonic()
        error: Optional[str] = None
        status_code = 0
        data: Any = None
        response_size = 0

        try:
            if method == "GET":
                resp = self.session.get(url, timeout=timeout)
            elif method == "POST":
                resp = self.session.post(
                    url, json=ep.body, timeout=timeout)
            elif method == "PUT":
                resp = self.session.put(
                    url, json=ep.body, timeout=timeout)
            elif method == "DELETE":
                resp = self.session.delete(url, timeout=timeout)
            else:
                error = f"Unsupported method {method}"
                elapsed = time.monotonic() - start
                return RequestResult(
                    endpoint=ep.path, method=ep.method,
                    status_code=0, elapsed=elapsed,
                    valid=False, validation_errors=[error],
                    validation_warnings=[], error=error)

            elapsed = time.monotonic() - start
            status_code = resp.status_code
            response_size = len(resp.content)

            # Accept 200 or 404 for exchange detail (exchange may not be connected)
            expected = ep.expected_status
            if ep.path.startswith("/api/v1/exchange/") and \
                    "/history" not in ep.path:
                expected = {200, 404}
            # Accept 200 or 500 for feed/status (DB may not be connected)
            if ep.path == "/api/v1/feed/status":
                expected = {200, 500}

            if (isinstance(expected, set) and
                    status_code not in expected) or \
                    (isinstance(expected, int) and
                     status_code != expected):
                error = (f"Expected status {ep.expected_status}, "
                         f"got {status_code}")

            # Parse JSON
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                try:
                    data = resp.json()
                except json.JSONDecodeError as e:
                    error = f"Invalid JSON: {e}"
            elif resp.text.strip():
                error = f"Unexpected content type: {content_type}"
            else:
                data = {}

        except requests.exceptions.Timeout:
            elapsed = time.monotonic() - start
            error = "Timeout"
            status_code = -1
        except requests.exceptions.ConnectionError as e:
            elapsed = time.monotonic() - start
            error = f"Connection error: {e}"
            status_code = -2
        except Exception as e:
            elapsed = time.monotonic() - start
            error = f"Exception: {e}"
            status_code = -3

        # Validate (only check fields on 200 responses; error responses have different shapes)
        val_result = ValidationResult()
        if data is not None and status_code == 200:
            val_result = validate_json(data, ep)

        errors = []
        warnings = []
        if error:
            errors.append(error)
            val_result.valid = False
        errors.extend(val_result.errors)
        warnings.extend(val_result.warnings)

        return RequestResult(
            endpoint=ep.path, method=ep.method,
            status_code=status_code, elapsed=elapsed,
            valid=val_result.valid and (error is None or status_code < 0),
            validation_errors=errors, validation_warnings=warnings,
            error=error, response_size=response_size,
        )


# ═══════════════════════════════════════════════════════════
# Concurrency Engine
# ═══════════════════════════════════════════════════════════


@dataclass
class ConcurrencyResult:
    results: List[RequestResult] = field(default_factory=list)
    duration: float = 0.0


class ConcurrencyEngine:
    """Runs multiple endpoint tests concurrently."""

    def __init__(self, config: Config, runner: HTTPTestRunner,
                 registry: List[EndpointDef]):
        self.config = config
        self.runner = runner
        self.registry = registry

    def run_functional(self) -> ConcurrencyResult:
        """Run each endpoint once for functional verification."""
        cr = ConcurrencyResult()
        start = time.monotonic()
        for ep in self.registry:
            result = self.runner.run_single(ep)
            cr.results.append(result)
            level = logging.WARNING if not result.valid else logging.DEBUG
            if not result.valid:
                logger.log(level, "%s %s → %d (%.1fms) %s",
                           ep.method, ep.path, result.status_code,
                           result.elapsed * 1000,
                           result.error or "")
            else:
                logger.debug("%s %s → %d (%.1fms)",
                             ep.method, ep.path, result.status_code,
                             result.elapsed * 1000)
        cr.duration = time.monotonic() - start
        return cr

    def run_load(self, iterations: int) -> ConcurrencyResult:
        """Run concurrent load test across all endpoints."""
        cr = ConcurrencyResult()
        start = time.monotonic()
        endpoints = self.registry * max(1, iterations // len(self.registry))
        # Shuffle to avoid thundering herd
        import random
        random.shuffle(endpoints)

        with ThreadPoolExecutor(max_workers=self.config.threads) as pool:
            fut_to_ep = {
                pool.submit(self.runner.run_single, ep, i): ep
                for i, ep in enumerate(endpoints[:iterations])
            }
            for fut in as_completed(fut_to_ep):
                try:
                    result = fut.result(timeout=self.config.timeout + 5)
                    cr.results.append(result)
                except Exception as e:
                    ep = fut_to_ep[fut]
                    cr.results.append(RequestResult(
                        endpoint=ep.path, method=ep.method,
                        status_code=-4, elapsed=0.0,
                        valid=False, validation_errors=[str(e)],
                        validation_warnings=[], error=str(e)))
        cr.duration = time.monotonic() - start
        return cr

    def run_duration(self, duration: float) -> ConcurrencyResult:
        """Run continuous load for a given duration (seconds)."""
        cr = ConcurrencyResult()
        deadline = time.monotonic() + duration
        start = time.monotonic()
        ep_idx = 0

        with ThreadPoolExecutor(max_workers=self.config.threads) as pool:
            futs = []
            while time.monotonic() < deadline:
                remaining = max(1, min(
                    self.config.threads * 2,
                    int((deadline - time.monotonic()) * 10)))
                for _ in range(remaining):
                    ep = self.registry[ep_idx % len(self.registry)]
                    ep_idx += 1
                    futs.append(pool.submit(self.runner.run_single, ep))

                # Collect completed
                done = []
                for f in futs:
                    if f.done():
                        done.append(f)
                        try:
                            cr.results.append(
                                f.result(timeout=self.config.timeout + 5))
                        except Exception as e:
                            pass
                for f in done:
                    futs.remove(f)

                if not futs:
                    time.sleep(0.01)

            # Drain remaining
            for f in as_completed(futs):
                try:
                    cr.results.append(
                        f.result(timeout=self.config.timeout + 5))
                except Exception:
                    pass

        cr.duration = time.monotonic() - start
        return cr


# ═══════════════════════════════════════════════════════════
# WebSocket Tester
# ═══════════════════════════════════════════════════════════


@dataclass
class WebSocketResult:
    connected: bool = False
    topics_subscribed: List[str] = field(default_factory=list)
    messages_received: Dict[str, int] = field(default_factory=dict)
    message_rate: float = 0.0  # msg/sec
    avg_interval: float = 0.0
    unexpected_disconnects: int = 0
    errors: List[str] = field(default_factory=list)
    passed: bool = False


class WebSocketTester:
    """Connect to /ws/live, subscribe to topics, verify message flow."""

    def __init__(self, config: Config):
        self.config = config
        self.ws_url = config.base.replace("http://", "ws://").replace(
            "https://", "wss://") + "/ws/live"

    def test(self) -> WebSocketResult:
        result = WebSocketResult()
        try:
            import websocket  # type: ignore
        except ImportError:
            result.errors.append("websocket-client not installed; skipping")
            result.passed = False
            return result

        topics = ["dashboard", "metrics", "performance", "exchange",
                  "system", "network", "feed", "queues"]

        ws: Optional[websocket.WebSocket] = None
        try:
            ws = websocket.create_connection(
                self.ws_url, timeout=self.config.timeout)
            result.connected = True
            logger.info("WebSocket connected to %s", self.ws_url)

            # Subscribe to monitoring topics
            for topic in topics:
                sub_msg = json.dumps({"subscribe": topic})
                ws.send(sub_msg)
                result.topics_subscribed.append(topic)
                logger.debug("Subscribed to '%s'", topic)

            # Collect messages for 5 seconds
            intervals: List[float] = []
            deadline = time.monotonic() + 5.0
            last_ts = time.monotonic()

            ws.settimeout(2.0)
            while time.monotonic() < deadline:
                try:
                    msg = ws.recv()
                    if not msg:
                        continue
                    now = time.monotonic()
                    intervals.append(now - last_ts)
                    last_ts = now

                    try:
                        data = json.loads(msg)
                        if isinstance(data, dict) and "type" in data:
                            t = data["type"]
                            result.messages_received[t] = \
                                result.messages_received.get(t, 0) + 1
                    except json.JSONDecodeError:
                        pass
                except websocket.WebSocketTimeoutException:
                    pass

            if intervals:
                result.avg_interval = statistics.mean(intervals)
                result.message_rate = 1.0 / result.avg_interval if \
                    result.avg_interval > 0 else 0

            # Verify at least one message per topic
            for topic in topics:
                if topic not in result.messages_received:
                    result.errors.append(
                        f"No messages received for topic '{topic}'")

            result.passed = (len(result.errors) == 0)
            logger.info("WebSocket: %d topics, %.1f msg/sec, %d errors",
                        len(result.messages_received), result.message_rate,
                        len(result.errors))

        except Exception as e:
            result.errors.append(f"WebSocket error: {e}")
            result.passed = False
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

        return result


# ═══════════════════════════════════════════════════════════
# Performance Analyzer
# ═══════════════════════════════════════════════════════════


@dataclass
class EndpointStats:
    endpoint: str = ""
    requests: int = 0
    successes: int = 0
    failures: int = 0
    timeouts: int = 0
    latencies: List[float] = field(default_factory=list)
    status_codes: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    errors: List[str] = field(default_factory=list)

    @property
    def avg_latency(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0.0

    @property
    def min_latency(self) -> float:
        return min(self.latencies) if self.latencies else 0.0

    @property
    def max_latency(self) -> float:
        return max(self.latencies) if self.latencies else 0.0

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        sorted_lats = sorted(self.latencies)
        idx = int(len(sorted_lats) * p / 100)
        return sorted_lats[min(idx, len(sorted_lats) - 1)]


@dataclass
class SummaryStats:
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    timeouts: int = 0
    latencies: List[float] = field(default_factory=list)
    duration: float = 0.0
    per_endpoint: Dict[str, EndpointStats] = field(
        default_factory=lambda: defaultdict(EndpointStats))

    @property
    def avg_latency(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0.0

    @property
    def min_latency(self) -> float:
        return min(self.latencies) if self.latencies else 0.0

    @property
    def max_latency(self) -> float:
        return max(self.latencies) if self.latencies else 0.0

    @property
    def p50(self) -> float:
        return self._percentile(50)

    @property
    def p95(self) -> float:
        return self._percentile(95)

    @property
    def p99(self) -> float:
        return self._percentile(99)

    def _percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        sorted_lats = sorted(self.latencies)
        idx = int(len(sorted_lats) * p / 100)
        return sorted_lats[min(idx, len(sorted_lats) - 1)]

    @property
    def requests_per_sec(self) -> float:
        return self.total_requests / self.duration if self.duration > 0 else 0

    @property
    def failure_rate(self) -> float:
        return (self.failed / self.total_requests * 100) if \
            self.total_requests > 0 else 0


class PerformanceAnalyzer:
    """Aggregate raw results into statistics."""

    def analyze(self, results: List[RequestResult],
                duration: float) -> SummaryStats:
        stats = SummaryStats(duration=duration)

        for r in results:
            stats.total_requests += 1
            if r.error and "Timeout" in r.error:
                stats.timeouts += 1
            if r.valid and not r.error:
                stats.successful += 1
            else:
                stats.failed += 1

            if r.elapsed > 0:
                stats.latencies.append(r.elapsed * 1000)  # ms

            # Per-endpoint
            eps = stats.per_endpoint[r.endpoint]
            eps.endpoint = r.endpoint
            eps.requests += 1
            if r.valid and not r.error:
                eps.successes += 1
            else:
                eps.failures += 1
                if r.error:
                    eps.errors.append(r.error)
                    if len(eps.errors) > 10:
                        eps.errors = eps.errors[-10:]
            if r.elapsed > 0:
                eps.latencies.append(r.elapsed * 1000)
            eps.status_codes[r.status_code] += 1
            if r.error and "Timeout" in r.error:
                eps.timeouts += 1

        return stats


# ═══════════════════════════════════════════════════════════
# Reporter
# ═══════════════════════════════════════════════════════════


class Reporter:
    """Generate console, JSON, and summary reports."""

    def __init__(self, config: Config, registry: List[EndpointDef],
                 stats: SummaryStats,
                 ws_result: Optional[WebSocketResult],
                 functional_results: List[RequestResult]):
        self.config = config
        self.registry = registry
        self.stats = stats
        self.ws_result = ws_result
        self.functional_results = functional_results

    def print_summary(self) -> None:
        """Print final summary to console."""
        s = self.stats
        ws_pass = (self.ws_result and self.ws_result.passed)
        db_pass = self._check_db_pass()
        history_pass = self._check_history_pass()
        live_pass = self._check_live_pass()

        # Score heuristic
        score = 100
        score -= min(30, s.failure_rate * 3)
        score -= min(20, s.timeouts * 2)
        if not ws_pass:
            score -= 15
        if not db_pass:
            score -= 10
        score = max(0, score)

        slowest, fastest = self._find_extremes()
        lines = [
            "",
            "=" * 50,
            "  API STRESS TEST SUMMARY",
            "=" * 50,
            "",
            f"  Total Endpoints Tested : {len(self.registry)}",
            f"  Total Requests         : {s.total_requests}",
            f"  Successful             : {s.successful}",
            f"  Failed                 : {s.failed}",
            f"  Timeouts               : {s.timeouts}",
            "",
            f"  Average Latency        : {s.avg_latency:.0f} ms",
            f"  P50                    : {s.p50:.0f} ms",
            f"  P95                    : {s.p95:.0f} ms",
            f"  P99                    : {s.p99:.0f} ms",
            f"  Min Latency            : {s.min_latency:.0f} ms",
            f"  Max Latency            : {s.max_latency:.0f} ms",
            "",
            f"  Requests/sec           : {s.requests_per_sec:.0f}",
            f"  Failure Rate           : {s.failure_rate:.1f}%",
            "",
            f"  WebSocket              : {'PASS' if ws_pass else 'FAIL'}",
            f"  Database               : {'PASS' if db_pass else 'FAIL'}",
            f"  History APIs           : {'PASS' if history_pass else 'FAIL'}",
            f"  Live APIs              : {'PASS' if live_pass else 'FAIL'}",
            "",
            f"  Slowest Endpoint       : {slowest or 'N/A'}",
            f"  Fastest Endpoint       : {fastest or 'N/A'}",
            "",
            f"  Overall Score          : {score}/100",
            "",
            "=" * 50,
        ]
        print("\n".join(lines))

    def _check_db_pass(self) -> bool:
        for ep in self.registry:
            if "history" in ep.path:
                r = [fr for fr in self.functional_results
                     if fr.endpoint == ep.path]
                if r and r[0].error and "Connection" in r[0].error:
                    return False
        return True

    def _check_history_pass(self) -> bool:
        history_results = [
            fr for fr in self.functional_results
            if any(ep.path == fr.endpoint and ep.type == EndpointType.HISTORY
                   for ep in self.registry)
        ]
        if not history_results:
            return True
        return sum(1 for r in history_results if r.valid and not r.error) > \
            len(history_results) * 0.5

    def _check_live_pass(self) -> bool:
        live_results = [
            fr for fr in self.functional_results
            if any(ep.path == fr.endpoint and ep.type == EndpointType.LIVE
                   for ep in self.registry)
        ]
        if not live_results:
            return True
        return sum(1 for r in live_results if r.valid and not r.error) > \
            len(live_results) * 0.5

    def _find_extremes(self) -> Tuple[Optional[str], Optional[str]]:
        slowest = fastest = None
        slowest_t = fastest_t = 0.0
        for path, ep in self.stats.per_endpoint.items():
            if ep.requests > 0 and ep.latencies:
                avg = ep.avg_latency
                if avg > slowest_t or slowest is None:
                    slowest = path
                    slowest_t = avg
                if avg < fastest_t or fastest is None:
                    fastest = path
                    fastest_t = avg
        return slowest, fastest

    def print_per_endpoint(self) -> None:
        print("\n  ── Per-Endpoint Statistics ──")
        header = f"{'Endpoint':<40} {'Req':>6} {'Fail':>5} {'Avg(ms)':>8} "
        header += f"{'P95':>8} {'P99':>8} {'Max':>8} {'Min':>8}"
        print(header)
        print("-" * 95)
        for path in sorted(self.stats.per_endpoint.keys()):
            ep = self.stats.per_endpoint[path]
            if ep.requests == 0:
                continue
            print(
                f"{path:<40} {ep.requests:>6} {ep.failures:>5} "
                f"{ep.avg_latency:>8.1f} {ep.percentile(95):>8.1f} "
                f"{ep.percentile(99):>8.1f} {ep.max_latency:>8.1f} "
                f"{ep.min_latency:>8.1f}"
            )

    def print_failures(self) -> None:
        failures = []
        for r in self.functional_results:
            if not r.valid or r.error:
                failures.append(r)
        if not failures:
            print("\n  ✅ All endpoints passed functional test")
            return

        print(f"\n  ❌ {len(failures)} Endpoint Failures:")
        for r in failures:
            print(f"    {r.method} {r.endpoint} → {r.status_code} "
                  f"({r.elapsed * 1000:.1f}ms)")
            if r.error:
                print(f"      Error: {r.error}")
            for e in r.validation_errors[:3]:
                print(f"      Validation: {e}")

    def to_json(self) -> str:
        """Generate a JSON report."""
        s = self.stats
        slowest, fastest = self._find_extremes()
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "config": {
                "host": self.config.host,
                "port": self.config.port,
                "threads": self.config.threads,
                "duration": self.config.duration,
                "stress": self.config.stress.name.lower(),
            },
            "summary": {
                "total_endpoints": len(self.registry),
                "total_requests": s.total_requests,
                "successful": s.successful,
                "failed": s.failed,
                "timeouts": s.timeouts,
                "avg_latency_ms": round(s.avg_latency, 2),
                "p50_ms": round(s.p50, 2),
                "p95_ms": round(s.p95, 2),
                "p99_ms": round(s.p99, 2),
                "min_latency_ms": round(s.min_latency, 2),
                "max_latency_ms": round(s.max_latency, 2),
                "requests_per_sec": round(s.requests_per_sec, 2),
                "failure_rate_pct": round(s.failure_rate, 2),
                "slowest_endpoint": slowest,
                "fastest_endpoint": fastest,
                "websocket": (self.ws_result.passed
                              if self.ws_result else False),
                "database": self._check_db_pass(),
                "history_apis": self._check_history_pass(),
                "live_apis": self._check_live_pass(),
                "score": self._calc_score(),
            },
            "per_endpoint": {},
            "functional_failures": [],
        }

        for path in sorted(s.per_endpoint.keys()):
            ep = s.per_endpoint[path]
            report["per_endpoint"][path] = {
                "requests": ep.requests,
                "successes": ep.successes,
                "failures": ep.failures,
                "timeouts": ep.timeouts,
                "avg_latency_ms": round(ep.avg_latency, 2),
                "p50_ms": round(ep.percentile(50), 2),
                "p95_ms": round(ep.percentile(95), 2),
                "p99_ms": round(ep.percentile(99), 2),
                "min_latency_ms": round(ep.min_latency, 2),
                "max_latency_ms": round(ep.max_latency, 2),
            }

        for r in self.functional_results:
            if not r.valid or r.error:
                report["functional_failures"].append({
                    "endpoint": r.endpoint,
                    "method": r.method,
                    "status_code": r.status_code,
                    "error": r.error,
                    "elapsed_ms": round(r.elapsed * 1000, 2),
                })

        if self.ws_result:
            report["websocket"] = {
                "connected": self.ws_result.connected,
                "topics_subscribed": self.ws_result.topics_subscribed,
                "messages_per_topic": self.ws_result.messages_received,
                "message_rate": round(self.ws_result.message_rate, 2),
                "avg_interval_ms": round(self.ws_result.avg_interval * 1000, 2),
                "errors": self.ws_result.errors,
                "passed": self.ws_result.passed,
            }

        return json.dumps(report, indent=2, default=str)

    def _calc_score(self) -> int:
        s = self.stats
        score = 100
        score -= min(30, s.failure_rate * 3)
        score -= min(20, s.timeouts * 2)
        if self.ws_result and not self.ws_result.passed:
            score -= 15
        if not self._check_db_pass():
            score -= 10
        return max(0, score)


# ═══════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.WARNING)


def main(argv: Optional[List[str]] = None) -> int:
    config = parse_args(argv)
    setup_logging(config.verbose)
    logger.info("Starting API stress test against %s", config.base)
    logger.info("Stress level: %s (%d threads, %ds duration)",
                config.stress.name.lower(), config.threads, config.duration)

    registry = build_registry()
    logger.info("Registry built: %d endpoints", len(registry))

    # ── HTTP Runner ──
    runner = HTTPTestRunner(config, registry)

    # ── Functional Tests ──
    print("\n  ═══ Functional Tests ═══")
    engine = ConcurrencyEngine(config, runner, registry)
    func_result = engine.run_functional()
    logger.info("Functional tests: %d/%d passed, %.1fs",
                sum(1 for r in func_result.results if r.valid and not r.error),
                len(func_result.results), func_result.duration)

    # ── Negative Tests ──
    print("\n  ═══ Negative Tests ═══")
    negative_registry = [
        EndpointDef("GET", "/api/v1/exchange/NONEXISTENT",
                    expected_status=404,
                    description="Invalid exchange"),
        EndpointDef("GET", "/api/v1/exchange/BINANCE/history",
                    expected_status=200,
                    description="History with bad timestamps",
                    query_params={"from": "99999999999999",
                                  "to": "0"}),
        EndpointDef("GET", "/api/v1/search",
                    expected_status=200,
                    description="Search empty query"),
        EndpointDef("GET", "/api/v1/metrics/history",
                    expected_status=200,
                    description="History negative interval",
                    query_params={"from": "-1", "to": "0"}),
    ]
    for nep in negative_registry:
        result = runner.run_single(nep)
        outcome = "✅" if result.valid or result.status_code == nep.expected_status else "❌"
        print(f"  {outcome} {nep.method} {nep.path} → {result.status_code} "
              f"({result.elapsed * 1000:.1f}ms)")

    # ── Load Tests ──
    print(f"\n  ═══ Load Tests ({config.threads} threads) ═══")
    load_result = engine.run_load(config.iterations)
    logger.info("Load test: %d requests in %.1fs",
                len(load_result.results), load_result.duration)

    # ── Duration Test ──
    if config.duration > 0:
        print(f"\n  ═══ Duration Test ({config.duration}s) ═══")
        dur_result = engine.run_duration(config.duration)
        logger.info("Duration test: %d requests in %.1fs",
                    len(dur_result.results), dur_result.duration)
    else:
        dur_result = load_result

    # ── WebSocket Test ──
    ws_result: Optional[WebSocketResult] = None
    if config.websocket:
        print(f"\n  ═══ WebSocket Test ═══")
        ws_tester = WebSocketTester(config)
        ws_result = ws_tester.test()
        icon = "✅" if ws_result.passed else "❌"
        print(f"  {icon} WebSocket: connected={ws_result.connected}, "
              f"topics={len(ws_result.messages_received)}, "
              f"rate={ws_result.message_rate:.1f}/s")

    # ── Analysis ──
    all_results = (
        func_result.results + load_result.results + dur_result.results
    )
    analyzer = PerformanceAnalyzer()
    stats = analyzer.analyze(all_results, dur_result.duration)

    # ── Report ──
    reporter = Reporter(config, registry, stats, ws_result,
                        func_result.results)
    reporter.print_failures()
    reporter.print_per_endpoint()
    reporter.print_summary()

    if config.output:
        json_report = reporter.to_json()
        with open(config.output, "w") as f:
            f.write(json_report)
        logger.info("JSON report written to %s", config.output)

    # Return code: 0 = all pass, 1 = failures
    return 0 if stats.failed == 0 and (
        ws_result is None or ws_result.passed) else 1


if __name__ == "__main__":
    sys.exit(main())
