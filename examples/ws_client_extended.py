#!/usr/bin/env python3
"""
Extended Datafeed Client with exchange switching, backtest, and diagnostics.
"""
import asyncio, json, sys, argparse, time
import websockets


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="wss://www.datafeed.fun")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--exchange", default=None,
                        help="Switch exchange: BINANCE, JUPITER, BIRDEYE")
    parser.add_argument("--symbols", nargs="+",
                        default=["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    args = parser.parse_args()

    url = "ws://localhost:4444" if args.local else args.url
    print(f"[*] Connecting to {url}")

    async with websockets.connect(url, ping_interval=10, ping_timeout=5, max_size=2**24) as ws:
        print(f"[+] Connected  |  Server: {url}")

        # Subscribe to all topics
        await ws.send("subscribe all")
        await asyncio.sleep(0.3)

        # Optionally switch exchange
        if args.exchange:
            ex_upper = args.exchange.upper()
            switch = {
                "type": "switch_exchange",
                "exchange": ex_upper,
                "symbols": args.symbols,
            }
            print(f"[*] Switching exchange to {ex_upper}")
            await ws.send(json.dumps(switch))
            await asyncio.sleep(0.3)

        # Receive loop
        print(f"[*] Receiving... (Ctrl+C to stop)\n")
        start = time.time()
        counts = {"market": 0, "monitor": 0, "other": 0}
        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                print(f"[STATS] {elapsed:.0f}s  |  "
                      f"market={counts['market']}  monitor={counts['monitor']}  "
                      f"other={counts['other']}  "
                      f"rate={counts['market']/max(elapsed,1):.1f}/s")
                continue

            try:
                data = json.loads(str(msg))
            except json.JSONDecodeError:
                counts["other"] += 1
                print(f"[RAW] {msg[:120]}")
                continue

            topic = data.get("topic", "")
            mtype = data.get("type", "")

            if topic == "ticker_" and "symbol" in data:
                counts["market"] += 1
                if counts["market"] <= 5:
                    print(f"[MK] {data['symbol']:8s}  ${data['price']:<10.2f}  "
                          f"bid=${data['bid']:<10.2f}  ask=${data['ask']:<10.2f}")
            elif mtype in ("dashboard", "metrics", "performance",
                           "exchange", "system", "network", "feed", "queues"):
                counts["monitor"] += 1
            else:
                counts["other"] += 1
                print(f"[{topic or mtype}] {msg[:120]}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] Done")
