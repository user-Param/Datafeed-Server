#!/usr/bin/env python3
"""
WebSocket Price Feed Stress Test

Launches concurrent clients against the datafeed WebSocket, verifies
each client receives live price data (topic="ticker_").  Fails if any
client gets zero prices or if aggregate throughput drops below threshold.

Usage:
    python tests/ws_price_feed_stress_test.py
    python tests/ws_price_feed_stress_test.py --clients 50 --duration 30
    python tests/ws_price_feed_stress_test.py --local --clients 10 --duration 15
    python tests/ws_price_feed_stress_test.py --ramp 5    # ramp up 5 clients/sec
"""

import asyncio
import json
import sys
import time
import argparse
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone

try:
    import websockets
except ImportError:
    print("ERROR: 'websockets' not installed.  pip install websockets")
    sys.exit(1)

CLEAR = "\033[2J\033[H"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
GREY = "\033[90m"
RESET = "\033[0m"


@dataclass
class ClientResult:
    id: int
    connected: bool = False
    connect_time: float = 0.0
    prices_received: int = 0
    total_messages: int = 0
    first_price_time: float = 0.0
    last_price_time: float = 0.0
    symbols_seen: set = field(default_factory=set)
    error: str = ""

    @property
    def received_prices(self) -> bool:
        return self.prices_received > 0

    @property
    def price_rate(self) -> float:
        if self.first_price_time == 0 or self.last_price_time == 0:
            return 0.0
        dur = self.last_price_time - self.first_price_time
        return self.prices_received / dur if dur > 0 else 0.0


@dataclass
class Summary:
    total_clients: int = 0
    connected: int = 0
    with_prices: int = 0
    without_prices: int = 0
    total_prices: int = 0
    total_messages: int = 0
    duration: float = 0.0
    client_results: list = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total_clients == 0:
            return 0.0
        return self.with_prices / self.total_clients * 100

    @property
    def aggregate_price_rate(self) -> float:
        return self.total_prices / self.duration if self.duration > 0 else 0.0


async def run_client(
    client_id: int,
    url: str,
    timeout: float,
    stop_event: asyncio.Event,
) -> ClientResult:
    result = ClientResult(id=client_id)
    try:
        ws = await asyncio.wait_for(
            websockets.connect(url, ping_interval=10, ping_timeout=5, max_size=2 ** 24),
            timeout=min(timeout, 10),
        )
        result.connected = True
        result.connect_time = time.time()

        await ws.send("subscribe all")

        while not stop_event.is_set():
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                result.total_messages += 1
                try:
                    data = json.loads(str(msg))
                except json.JSONDecodeError:
                    continue
                if data.get("topic") == "ticker_" and "symbol" in data:
                    result.prices_received += 1
                    sym = str(data.get("symbol", ""))
                    if sym:
                        result.symbols_seen.add(sym)
                    now = time.time()
                    if result.first_price_time == 0:
                        result.first_price_time = now
                    result.last_price_time = now
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed:
                break

        await ws.close()
    except asyncio.TimeoutError:
        result.error = "connect timeout"
    except Exception as e:
        result.error = str(e)[:120]

    return result


def fmt(sec: float) -> str:
    m = int(sec // 60)
    s = int(sec % 60)
    return f"{m:02d}:{s:02d}"


async def main():
    parser = argparse.ArgumentParser(
        description="WebSocket Price Feed Stress Test"
    )
    parser.add_argument("--url", default="wss://www.datafeed.fun")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--clients", type=int, default=20, help="number of concurrent clients")
    parser.add_argument("--duration", type=int, default=20, help="test duration (seconds)")
    parser.add_argument("--ramp", type=int, default=0, help="clients to start per second (0 = all at once)")
    parser.add_argument("--pass-rate", type=float, default=80,
                        help="minimum %% of clients that must receive prices to pass")
    parser.add_argument("--min-price-rate", type=float, default=0.5,
                        help="minimum aggregate prices/sec to pass")
    args = parser.parse_args()

    url = "ws://localhost:4444" if args.local else args.url
    n_clients = args.clients
    duration = args.duration
    ramp = args.ramp

    print(f"{CLEAR}")
    print(f"  {BOLD}WS PRICE FEED STRESS TEST{RESET}")
    print(f"  {'=' * 50}")
    print(f"  URL:        {url}")
    print(f"  Clients:    {n_clients}" + (f"  (ramp: {ramp}/s)" if ramp else "  (all at once)"))
    print(f"  Duration:   {duration}s")
    print(f"  Pass rate:  >= {args.pass_rate}% of clients with prices")
    print(f"  Min rate:   >= {args.min_price_rate} prices/sec aggregate")
    print()

    stop_event = asyncio.Event()
    clients: list[asyncio.Task] = []
    start = time.time()

    if ramp > 0:
        remaining = list(range(n_clients))
        while remaining and time.time() - start < duration:
            batch = remaining[:ramp]
            remaining = remaining[ramp:]
            for cid in batch:
                t = asyncio.create_task(run_client(cid, url, duration, stop_event))
                clients.append(t)
            await asyncio.sleep(1.0)
    else:
        for cid in range(n_clients):
            t = asyncio.create_task(run_client(cid, url, duration, stop_event))
            clients.append(t)

    elapsed = 0
    while elapsed < duration:
        elapsed = time.time() - start
        done_count = sum(1 for c in clients if c.done())
        remaining_bar = max(0, duration - elapsed)
        bar_len = 30
        fill = int((elapsed / duration) * bar_len)
        bar = "█" * fill + "░" * (bar_len - fill)
        print(
            f"\r  [{bar}] {fmt(elapsed)} / {fmt(duration)}  "
            f"clients_done={done_count}/{n_clients}  ",
            end="",
            flush=True,
        )
        await asyncio.sleep(0.5)

    stop_event.set()
    results = await asyncio.gather(*clients, return_exceptions=True)

    duration_actual = time.time() - start

    summary = Summary(
        total_clients=n_clients,
        duration=duration_actual,
    )

    for r in results:
        if isinstance(r, ClientResult):
            summary.client_results.append(r)
            if r.connected:
                summary.connected += 1
            if r.received_prices:
                summary.with_prices += 1
            else:
                summary.without_prices += 1
            summary.total_prices += r.prices_received
            summary.total_messages += r.total_messages

    passed = (
        summary.success_rate >= args.pass_rate
        and summary.aggregate_price_rate >= args.min_price_rate
    )

    print(f"\n\n  {'=' * 50}")
    print(f"  {BOLD}RESULTS{RESET}")
    print(f"  {'=' * 50}")
    print(f"  Duration:          {duration_actual:.1f}s")
    print(f"  Total clients:     {summary.total_clients}")
    print(f"  Connected:         {summary.connected}")
    print(f"  Got prices:        {summary.with_prices}")
    print(f"  No prices:         {summary.without_prices}")
    print(f"  Total price msgs:  {summary.total_prices}")
    print(f"  Total all msgs:    {summary.total_messages}")
    print(f"  Aggregate rate:    {summary.aggregate_price_rate:.1f} prices/sec")
    print(f"  Client success:    {summary.success_rate:.0f}%  (threshold: >= {args.pass_rate}%)")

    if summary.client_results:
        rates = [r.price_rate for r in summary.client_results if r.received_prices]
        if rates:
            print(f"  Per-client rates:  avg={statistics.mean(rates):.1f}  "
                  f"min={min(rates):.1f}  max={max(rates):.1f}  "
                  f"p50={statistics.median(rates):.1f}")

        failed = [r for r in summary.client_results if not r.received_prices]
        if failed:
            print(f"\n  {YELLOW}Clients without prices:{RESET}")
            for r in failed[:10]:
                err = f"  error: {r.error}" if r.error else ""
                print(f"    client #{r.id:3d}  connected={r.connected}  "
                      f"msgs={r.total_messages}{err}")
            if len(failed) > 10:
                print(f"    ... and {len(failed) - 10} more")

    print(f"\n  {'=' * 50}")
    if passed:
        print(f"  {BOLD}{GREEN}RESULT: PASS{RESET}")
    else:
        print(f"  {BOLD}{RED}RESULT: FAIL{RESET}")
    print(f"  {'=' * 50}\n")

    return 0 if passed else 1


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\n  Interrupted\n")
        sys.exit(130)
