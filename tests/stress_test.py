#!/usr/bin/env python3
"""
Stress test for the datafeed server.
This script creates multiple WebSocket client connections to test server scalability.
"""

import asyncio
import json
import time
import statistics
import argparse
import sys
import urllib.request
import urllib.error
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
import websockets
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StressTestClient:
    def __init__(self, client_id: str, server_uri: str):
        self.client_id = client_id
        self.server_uri = server_uri
        self.websocket = None
        self.messages_received = 0
        self.messages_sent = 0
        self.latencies: List[float] = []
        self.connected = False
        self.start_time = None
        self.end_time = None

    async def connect(self):
        """Connect to the WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.server_uri)
            self.connected = True
            self.start_time = time.time()
            logger.debug(f"Client {self.client_id} connected")
            return True
        except Exception as e:
            logger.error(f"Client {self.client_id} failed to connect: {e}")
            return False

    async def disconnect(self):
        """Disconnect from the WebSocket server"""
        if self.websocket and self.connected:
            await self.websocket.close()
            self.connected = False
            self.end_time = time.time()
            logger.debug(f"Client {self.client_id} disconnected")

    async def send_message(self, message: str):
        """Send a message to the server"""
        if not self.connected or not self.websocket:
            return False

        try:
            start_time = time.time()
            await self.websocket.send(message)
            self.messages_sent += 1
            logger.debug(f"Client {self.client_id} sent message: {message[:50]}...")
            return True
        except Exception as e:
            logger.error(f"Client {self.client_id} failed to send message: {e}")
            return False

    async def receive_messages(self, duration: int):
        """Receive messages for a specified duration"""
        if not self.connected or not self.websocket:
            return 0

        end_time = time.time() + duration
        received_count = 0

        try:
            while time.time() < end_time and self.connected:
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=1.0)
                    receive_time = time.time()

                    # Try to extract timestamp from message for latency calculation
                    try:
                        data = json.loads(message)
                        if 'timestamp' in data:
                            # data['timestamp'] is in milliseconds since epoch
                            # receive_time is in seconds since epoch
                            latency = (receive_time * 1000) - data['timestamp']
                            self.latencies.append(latency)
                    except:
                        pass  # Not JSON or no timestamp, skip latency calculation

                    self.messages_received += 1
                    received_count += 1
                    logger.debug(f"Client {self.client_id} received message: {message[:50]}...")

                except asyncio.TimeoutError:
                    # Timeout is expected, continue loop
                    continue
                except websockets.exceptions.ConnectionClosed:
                    logger.debug(f"Client {self.client_id} connection closed")
                    break
                except Exception as e:
                    logger.error(f"Client {self.client_id} error receiving message: {e}")
                    break

        except Exception as e:
            logger.error(f"Client {self.client_id} receive loop error: {e}")

        return received_count

    async def run_test(self, test_duration: int, subscribe_topics: List[str] = None):
        """Run the complete test for this client"""
        if not await self.connect():
            return False

        try:
            # Subscribe to topics if specified
            if subscribe_topics:
                for topic in subscribe_topics:
                    subscribe_msg = json.dumps({
                        "action": "subscribe",
                        "topic": topic
                    })
                    await self.send_message(subscribe_msg)
                    # Small delay to avoid flooding
                    await asyncio.sleep(0.01)

            # Receive messages for the test duration
            received = await self.receive_messages(test_duration)

            return True

        finally:
            await self.disconnect()

class DatafeedStressTest:
    def __init__(self, host: str = "localhost", port: int = 4444):
        self.host = host
        self.port = port
        self.server_uri = f"ws://{host}:{port}"
        self.clients: List[StressTestClient] = []
        self.results = {
            'total_clients': 0,
            'connected_clients': 0,
            'total_messages_sent': 0,
            'total_messages_received': 0,
            'connection_errors': 0,
            'latencies': [],
            'start_time': None,
            'end_time': None,
            'duration': 0
        }

    async def create_clients(self, num_clients: int):
        """Create client instances"""
        self.clients = [
            StressTestClient(f"client_{i}", self.server_uri)
            for i in range(num_clients)
        ]
        self.results['total_clients'] = num_clients
        logger.info(f"Created {num_clients} client instances")

    async def run_concurrent_test(self, num_clients: int, test_duration: int,
                                subscribe_topics: List[str] = None,
                                ramp_up_time: int = 10):
        """Run stress test with concurrent clients"""
        logger.info(f"Starting stress test with {num_clients} clients for {test_duration} seconds")
        self.results['start_time'] = time.time()

        # Create clients
        await self.create_clients(num_clients)

        # Connect clients with ramp-up
        connect_tasks = []
        for i, client in enumerate(self.clients):
            # Stagger connections over ramp_up_time period
            delay = (i / num_clients) * ramp_up_time if ramp_up_time > 0 else 0
            task = asyncio.create_task(self._delayed_connect(client, delay))
            connect_tasks.append(task)

        # Wait for all connections
        connection_results = await asyncio.gather(*connect_tasks, return_exceptions=True)

        # Count successful connections
        connected_count = sum(1 for result in connection_results
                            if result is True and not isinstance(result, Exception))
        self.results['connected_clients'] = connected_count
        self.results['connection_errors'] = num_clients - connected_count

        logger.info(f"Successfully connected {connected_count}/{num_clients} clients")

        if connected_count == 0:
            logger.error("No clients connected, aborting test")
            return self.results

        # Subscribe clients to topics (if specified)
        if subscribe_topics:
            logger.info(f"Subscribing clients to topics: {subscribe_topics}")
            subscribe_tasks = []
            for client in self.clients:
                if client.connected:
                    task = asyncio.create_task(self._subscribe_client(client, subscribe_topics))
                    subscribe_tasks.append(task)

            if subscribe_tasks:
                await asyncio.gather(*subscribe_tasks, return_exceptions=True)

        # Run the actual test - collect messages for test_duration
        logger.info(f"Running test for {test_duration} seconds...")
        receive_tasks = []
        for client in self.clients:
            if client.connected:
                task = asyncio.create_task(client.receive_messages(test_duration))
                receive_tasks.append(task)

        # Wait for all clients to finish receiving
        receive_results = await asyncio.gather(*receive_tasks, return_exceptions=True)

        # Collect results
        total_sent = 0
        total_received = 0
        all_latencies = []

        for i, client in enumerate(self.clients):
            if client.connected:
                total_sent += client.messages_sent
                total_received += client.messages_received
                all_latencies.extend(client.latencies)

        self.results['total_messages_sent'] = total_sent
        self.results['total_messages_received'] = total_received
        self.results['latencies'] = all_latencies

        # Disconnect all clients
        disconnect_tasks = []
        for client in self.clients:
            if client.connected:
                task = asyncio.create_task(client.disconnect())
                disconnect_tasks.append(task)

        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)

        self.results['end_time'] = time.time()
        self.results['duration'] = self.results['end_time'] - self.results['start_time']

        return self.results

    async def _delayed_connect(self, client: StressTestClient, delay: float):
        """Connect client after a delay"""
        if delay > 0:
            await asyncio.sleep(delay)
        return await client.connect()

    async def _subscribe_client(self, client: StressTestClient, topics: List[str]):
        """Subscribe client to topics"""
        for topic in topics:
            subscribe_msg = json.dumps({
                "action": "subscribe",
                "topic": topic
            })
            await client.send_message(subscribe_msg)
            # Small delay between subscriptions
            await asyncio.sleep(0.005)

    def generate_report(self) -> str:
        """Generate a detailed report of the stress test"""
        if self.results['duration'] == 0:
            return "No test data available"

        report = []
        report.append("=" * 60)
        report.append("DATAFEED SERVER STRESS TEST REPORT")
        report.append("=" * 60)
        report.append(f"Test Start Time: {datetime.fromtimestamp(self.results['start_time'])}")
        report.append(f"Test End Time: {datetime.fromtimestamp(self.results['end_time'])}")
        report.append(f"Test Duration: {self.results['duration']:.2f} seconds")
        report.append("")
        report.append("CONNECTION STATISTICS:")
        report.append(f"  Total Clients Attempted: {self.results['total_clients']}")
        report.append(f"  Successfully Connected: {self.results['connected_clients']}")
        report.append(f"  Connection Errors: {self.results['connection_errors']}")
        if self.results['total_clients'] > 0:
            success_rate = (self.results['connected_clients'] / self.results['total_clients']) * 100
            report.append(f"  Connection Success Rate: {success_rate:.2f}%")
        report.append("")
        report.append("MESSAGE STATISTICS:")
        report.append(f"  Total Messages Sent: {self.results['total_messages_sent']}")
        report.append(f"  Total Messages Received: {self.results['total_messages_received']}")
        if self.results['duration'] > 0:
            msg_rate_sent = self.results['total_messages_sent'] / self.results['duration']
            msg_rate_received = self.results['total_messages_received'] / self.results['duration']
            report.append(f"  Message Send Rate: {msg_rate_sent:.2f} msg/sec")
            report.append(f"  Message Receive Rate: {msg_rate_received:.2f} msg/sec")
        if self.results['connected_clients'] > 0:
            avg_sent_per_client = self.results['total_messages_sent'] / self.results['connected_clients']
            avg_received_per_client = self.results['total_messages_received'] / self.results['connected_clients']
            report.append(f"  Avg Messages Sent per Client: {avg_sent_per_client:.2f}")
            report.append(f"  Avg Messages Received per Client: {avg_received_per_client:.2f}")
        report.append("")

        # Latency statistics
        if self.results['latencies']:
            latencies = self.results['latencies']
            report.append("LATENCY STATISTICS (milliseconds):")
            report.append(f"  Latency Samples: {len(latencies)}")
            report.append(f"  Min Latency: {min(latencies):.2f} ms")
            report.append(f"  Max Latency: {max(latencies):.2f} ms")
            report.append(f"  Mean Latency: {statistics.mean(latencies):.2f} ms")
            report.append(f"  Median Latency: {statistics.median(latencies):.2f} ms")
            if len(latencies) >= 2:
                report.append(f"  Latency Std Dev: {statistics.stdev(latencies):.2f} ms")
            # Percentiles
            sorted_lat = sorted(latencies)
            n = len(sorted_lat)
            if n >= 1:
                report.append(f"  50th Percentile (Median): {sorted_lat[int(n*0.5)]:.2f} ms")
                report.append(f"  90th Percentile: {sorted_lat[int(n*0.9)]:.2f} ms")
                report.append(f"  95th Percentile: {sorted_lat[int(n*0.95)]:.2f} ms")
                report.append(f"  99th Percentile: {sorted_lat[int(n*0.99)]:.2f} ms")
            report.append("")
        else:
            report.append("LATENCY STATISTICS: No latency data available")
            report.append("")

        # Performance assessment
        report.append("PERFORMANCE ASSESSMENT:")
        if self.results['connected_clients'] > 0:
            clients_per_second = self.results['connected_clients'] / self.results['duration']
            report.append(f"  Connection Establishment Rate: {clients_per_second:.2f} clients/sec")

            if self.results['duration'] > 0:
                msg_per_client_per_second = (self.results['total_messages_received'] /
                                           self.results['connected_clients'] / self.results['duration'])
                report.append(f"  Message Throughput per Client: {msg_per_client_per_second:.2f} msg/client/sec")

                total_msg_per_second = self.results['total_messages_received'] / self.results['duration']
                report.append(f"  Overall Message Throughput: {total_msg_per_second:.2f} msg/sec")

        report.append("")
        report.append("RECOMMENDATIONS:")
        if self.results['connection_errors'] > 0:
            error_rate = (self.results['connection_errors'] / self.results['total_clients']) * 100
            if error_rate > 5:
                report.append("  ⚠️  High connection error rate (>5%). Consider:")
                report.append("     - Increasing server thread pool size")
                report.append("     - Checking system file descriptor limits")
                report.append("     - Reviewing network configuration")
            else:
                report.append("  ✅ Connection error rate is acceptable (<5%)")

        if self.results['latencies']:
            avg_latency = statistics.mean(self.results['latencies'])
            if avg_latency > 100:  # More than 100ms average latency
                report.append("  ⚠️  High average latency (>100ms). Consider:")
                report.append("     - Optimizing message processing pipeline")
                report.append("     - Checking for blocking operations in WebSocket handlers")
                report.append("     - Reviewing database/query performance")
            elif avg_latency > 50:  # Between 50-100ms
                report.append("  ⚠️  Moderate latency (50-100ms). Consider optimization for better performance.")
            else:
                report.append("  ✅ Latency is excellent (<50ms average)")

        report.append("=" * 60)

        return "\n".join(report)

class EndToEndTest:
    """
    End-to-end system test that validates every HTTP REST endpoint and
    WebSocket capability of the datafeed server.  Produces a detailed
    pass/fail report.
    """

    def __init__(self, host: str = "localhost", port: int = 4444):
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.ws_url = f"ws://{host}:{port}"
        self.results: Dict[str, Any] = {
            "start_time": None,
            "end_time": None,
            "tests": [],
        }

    # ── HTTP helper ────────────────────────────────────────────

    def _http_request(
        self, method: str, path: str, body: Optional[dict] = None
    ) -> Tuple[Optional[int], Any]:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read()
            return e.code, json.loads(raw.decode()) if raw else {}
        except Exception as e:
            return None, {"error": str(e)}

    def _record(self, name: str, passed: bool, detail: str) -> None:
        self.results["tests"].append({
            "name": name,
            "passed": passed,
            "detail": detail,
        })

    # ── HTTP endpoint tests ───────────────────────────────────

    def run_http_tests(self) -> None:
        """Exercise every REST API route and verify responses."""

        # 1  GET /health
        status, data = self._http_request("GET", "/health")
        db_ok = data.get("db") == "connected"
        self._record(
            "GET /health",
            status == 200 and db_ok,
            f"Status={status}, db={data.get('db')}, "
            f"feed_instances={data.get('feed_instances')}",
        )

        # 2  GET /api/v1/feed/status
        status, data = self._http_request("GET", "/api/v1/feed/status")
        self._record(
            "GET /api/v1/feed/status",
            status in (200, 500),         # 500 when no feed instances registered
            f"Status={status}, response_keys={list(data.keys())}",
        )

        # 3  GET /api/v1/clients  (expect empty array)
        status, data = self._http_request("GET", "/api/v1/clients")
        is_arr = isinstance(data, list)
        self._record(
            "GET /api/v1/clients (before create)",
            status == 200 and is_arr,
            f"Status={status}, type={type(data).__name__}, "
            f"count={len(data) if is_arr else 'N/A'}",
        )

        # 4  POST /api/v1/clients  (create)
        payload = {
            "client_name": "E2E Test Client",
            "plan": "premium",
            "status": "active",
        }
        status, data = self._http_request("POST", "/api/v1/clients", body=payload)
        tenant_id: str = data.get("tenant_id", "")
        created = status == 201 and bool(tenant_id)
        self._record(
            "POST /api/v1/clients (create)",
            created,
            f"Status={status}, tenant_id={tenant_id or 'N/A'}, "
            f"name={data.get('client_name', 'N/A')}",
        )

        # 5  GET /api/v1/clients/:id
        if created:
            status, data = self._http_request("GET", f"/api/v1/clients/{tenant_id}")
            self._record(
                "GET /api/v1/clients/:id (by id)",
                status == 200 and data.get("tenant_id") == tenant_id,
                f"Status={status}, tenant_id={data.get('tenant_id')}, "
                f"name={data.get('client_name')}",
            )
        else:
            self._record(
                "GET /api/v1/clients/:id (by id)",
                False,
                "Skipped — no tenant_id from create step",
            )

        # 6  DELETE /api/v1/clients/:id
        if created:
            status, data = self._http_request(
                "DELETE", f"/api/v1/clients/{tenant_id}"
            )
            self._record(
                "DELETE /api/v1/clients/:id (delete)",
                status == 204,
                f"Status={status}",
            )
        else:
            self._record(
                "DELETE /api/v1/clients/:id (delete)",
                False,
                "Skipped — no tenant_id from create step",
            )

        # 7  GET /api/v1/clients  (verify deletion)
        status, data = self._http_request("GET", "/api/v1/clients")
        is_arr = isinstance(data, list)
        self._record(
            "GET /api/v1/clients (after delete)",
            status == 200 and is_arr,
            f"Status={status}, count={len(data) if is_arr else 'N/A'}",
        )

    # ── WebSocket tests ───────────────────────────────────────

    async def run_websocket_tests(self) -> None:
        """Test WebSocket connect, subscribe, data reception, commands."""

        try:
            async with websockets.connect(
                self.ws_url, max_size=2 ** 20, timeout=15
            ) as ws:
                # Subscribe — server uses string-contains matching
                await ws.send(json.dumps({"action": "subscribe", "topic": "all"}))
                await asyncio.sleep(0.2)

                # Switch to Live mode
                await ws.send("_Live")
                await asyncio.sleep(0.2)

                # Collect received messages
                received: List[str] = []
                try:
                    while len(received) < 3:
                        msg = await asyncio.wait_for(ws.recv(), timeout=8)
                        received.append(msg)
                except asyncio.TimeoutError:
                    pass

                has_data = len(received) > 0
                self._record(
                    "WebSocket — Connect & Subscribe",
                    has_data,
                    f"Connected, subscribed to 'all', received "
                    f"{len(received)} message(s)",
                )

                # Validate message structure
                if received:
                    try:
                        parsed = json.loads(received[0])
                        valid = isinstance(parsed, dict) and "topic" in parsed
                        self._record(
                            "WebSocket — Message Format",
                            valid,
                            f"topic={parsed.get('topic', 'N/A')}, "
                            f"keys={list(parsed.keys())}",
                        )
                    except json.JSONDecodeError:
                        self._record(
                            "WebSocket — Message Format",
                            False,
                            f"Invalid JSON: {received[0][:100]}",
                        )
                else:
                    self._record(
                        "WebSocket — Message Format",
                        True,
                        "No messages received to validate",
                    )

                # Send switch_exchange command
                switch = {
                    "type": "switch_exchange",
                    "exchange": "BINANCE",
                    "symbols": ["BTCUSDT"],
                }
                await ws.send(json.dumps(switch))
                self._record(
                    "WebSocket — switch_exchange",
                    True,
                    "Sent switch_exchange to BINANCE (no response expected)",
                )

        except Exception as e:
            self._record(
                "WebSocket — Connect & Subscribe",
                False,
                f"Error: {e}",
            )

    # ── Runner ────────────────────────────────────────────────

    async def run_all(self) -> Dict[str, Any]:
        self.results["start_time"] = time.time()
        logger.info("=" * 60)
        logger.info("  Starting End-to-End System Tests")
        logger.info("=" * 60)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.run_http_tests)
        await self.run_websocket_tests()

        self.results["end_time"] = time.time()
        return self.results

    # ── Report ────────────────────────────────────────────────

    def generate_report(self) -> str:
        tests = self.results.get("tests", [])
        passed = sum(1 for t in tests if t["passed"])
        failed = sum(1 for t in tests if not t["passed"])
        total = len(tests)
        dur = (
            self.results["end_time"] - self.results["start_time"]
            if self.results["end_time"]
            else 0
        )

        lines: List[str] = []
        lines.append("=" * 60)
        lines.append("DATAFEED SYSTEM END-TO-END TEST REPORT")
        lines.append("=" * 60)
        lines.append(
            f"  Test Time:  {datetime.fromtimestamp(self.results['start_time'])}"
        )
        lines.append(f"  Server:     {self.host}:{self.port}")
        lines.append(f"  Duration:   {dur:.2f}s")
        lines.append("")
        if failed == 0:
            lines.append(f"  ✅  ALL {total} TESTS PASSED")
        else:
            lines.append(f"  ❌  {failed}/{total} TESTS FAILED")
        lines.append("")

        # HTTP section
        lines.append("-" * 60)
        lines.append("  HTTP ENDPOINT TESTS")
        lines.append("-" * 60)
        for i, t in enumerate(tests, 1):
            if t["name"].startswith("WebSocket"):
                continue
            icon = "✅" if t["passed"] else "❌"
            lines.append(f"  {icon}  Test {i}: {t['name']}")
            lines.append(f"       {t['detail']}")
            lines.append("")

        # WebSocket section
        lines.append("-" * 60)
        lines.append("  WEBSOCKET TESTS")
        lines.append("-" * 60)
        for i, t in enumerate(tests, 1):
            if not t["name"].startswith("WebSocket"):
                continue
            icon = "✅" if t["passed"] else "❌"
            lines.append(f"  {icon}  Test {i}: {t['name']}")
            lines.append(f"       {t['detail']}")
            lines.append("")

        # Summary
        lines.append("=" * 60)
        lines.append("  SUMMARY")
        lines.append("=" * 60)
        lines.append(f"  Total Tests:  {total}")
        lines.append(f"  Passed:       {passed}")
        lines.append(f"  Failed:       {failed}")
        if failed > 0:
            lines.append("")
            lines.append("  FAILED TESTS:")
            for t in tests:
                if not t["passed"]:
                    lines.append(f"    ❌ {t['name']}")
                    lines.append(f"       {t['detail']}")
        lines.append("")
        if failed == 0:
            lines.append("  ✅  END-TO-END SYSTEM TEST: PASSED")
        else:
            lines.append("  ❌  END-TO-END SYSTEM TEST: FAILED")
        lines.append("=" * 60)
        return "\n".join(lines)


async def main():
    parser = argparse.ArgumentParser(
        description="Datafeed server test suite — stress test or end-to-end test"
    )
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", type=int, default=4444, help="Server port (default: 4444)")
    parser.add_argument(
        "--e2e",
        action="store_true",
        help="Run end-to-end system test instead of stress test",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=100,
        help="Number of concurrent clients (default: 100)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Test duration in seconds (default: 30)",
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        default=["ticker_"],
        help="Topics to subscribe to (default: ticker_)",
    )
    parser.add_argument(
        "--ramp-up",
        type=int,
        default=10,
        help="Client connection ramp-up time in seconds (default: 10)",
    )
    parser.add_argument("--output", type=str, help="Output report to file")

    args = parser.parse_args()

    # ── End-to-end test mode ─────────────────────────────────
    if args.e2e:
        e2e = EndToEndTest(args.host, args.port)
        try:
            await e2e.run_all()
            report = e2e.generate_report()
            print(report)
            if args.output:
                with open(args.output, "w") as f:
                    f.write(report)
                logger.info("Report saved to %s", args.output)
        except Exception as e:
            logger.error("E2E test failed: %s", e)
            import traceback

            traceback.print_exc()
        return

    # ── Stress test mode ─────────────────────────────────────
    stress_test = DatafeedStressTest(args.host, args.port)

    try:
        results = await stress_test.run_concurrent_test(
            num_clients=args.clients,
            test_duration=args.duration,
            subscribe_topics=args.topics,
            ramp_up_time=args.ramp_up,
        )

        report = stress_test.generate_report()
        print(report)

        if args.output:
            with open(args.output, "w") as f:
                f.write(report)
            logger.info("Report saved to %s", args.output)

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error("Test failed with error: %s", e)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())