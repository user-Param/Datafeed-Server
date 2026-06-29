#!/usr/bin/env python3
"""
Datafeed WebSocket Client — Minimal Working Example

Connect, subscribe, and receive live market data.

Usage:
    python ws_client.py
    python ws_client.py --url wss://www.datafeed.fun
    python ws_client.py --local

Protocol:
    1. Connect to wss://www.datafeed.fun (or ws://localhost:4444)
    2. Send a message containing 'subscribe' and topic keywords
       e.g. "subscribe ticker" or "subscribe all"
    3. Receive market data on topic "ticker_"
    4. Receive monitoring data on topics: dashboard, metrics, etc. (every 1s)

Topic Keywords:
    ticker     → subscribe to "ticker_" (live prices)
    price      → subscribe to "price_"
    bid        → subscribe to "bid_"
    ask        → subscribe to "ask_"
    dashboard  → subscribe to "dashboard"
    metrics    → subscribe to "metrics"
    performance→ subscribe to "performance"
    exchange   → subscribe to "exchange"
    all        → subscribe to ALL topics

Exchange Management (optional):
    {"type":"switch_exchange","exchange":"BINANCE","symbols":["BTCUSDT","ETHUSDT","SOLUSDT"]}
    Supports: BINANCE, JUPITER, BIRDEYE
"""
import asyncio, json, sys, argparse
import websockets


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="wss://www.datafeed.fun")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    url = "ws://localhost:4444" if args.local else args.url
    print(f"[*] Connecting to {url}")

    async with websockets.connect(url, ping_interval=10, ping_timeout=5, max_size=2**24) as ws:
        print("[+] Connected")

        # --- Step 1: Subscribe to market data ---
        # The server uses substring matching. "subscribe ticker" subscribes to "ticker_".
        # "subscribe all" subscribes to every topic.
        sub_msg = "subscribe all"
        print(f"[*] Sending: {sub_msg}")
        await ws.send(sub_msg)
        await asyncio.sleep(0.3)

        # --- Step 2 (optional): Switch exchange ---
        # Default is BINANCE. Supports: BINANCE, JUPITER, BIRDEYE
        # switch = {"type":"switch_exchange","exchange":"BINANCE","symbols":["BTCUSDT","ETHUSDT","SOLUSDT"]}
        # await ws.send(json.dumps(switch))
        # await asyncio.sleep(0.3)

        # --- Step 3: Receive data ---
        print("[*] Receiving market data (press Ctrl+C to stop)...\n", flush=True)
        while True:
            msg = await ws.recv()
            try:
                data = json.loads(str(msg))
            except json.JSONDecodeError:
                continue

            topic = data.get("topic", "")
            mtype = data.get("type", "")

            if topic == "ticker_" and "symbol" in data:
                print(f"[PRICE] {data['symbol']:8s}  ${data['price']:<10.2f}  "
                      f"bid=${data['bid']:<10.2f}  ask=${data['ask']:<10.2f}",
                      flush=True)
            elif mtype == "dashboard":
                print(f"[DASH] cpu={data.get('cpu_usage')}% "
                      f"mem={data.get('memory_rss', 0)//1024//1024}MB",
                      flush=True)
            elif mtype in ("metrics", "performance", "exchange", "system",
                           "network", "feed", "queues"):
                pass
            else:
                print(f"[{topic or mtype}] {msg[:120]}", flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] Done")
