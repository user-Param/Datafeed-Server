"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";
import { useEffect, useMemo, useState } from "react";

interface Tick {
  symbol: string;
  price: string;
  bid: string;
  ask: string;
  chg: string;
}

export default function Ticker() {
  const { exchanges } = useDatafeed();
  const [, setTick] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setTick((n) => n + 1), 100);
    return () => clearInterval(id);
  }, []);

  const items = useMemo(() => {
    if (!exchanges || exchanges.length === 0) {
      const fallback: Tick[] = [];
      for (let i = 0; i < 40; i++) {
        fallback.push({ symbol: "BTCUSDT", price: "70000", bid: "69990", ask: "70010", chg: "+0.5%" });
      }
      return fallback;
    }

    return exchanges.flatMap((ex) => {
      const block: Tick = {
        symbol: ex.name,
        price: "",
        bid: "-",
        ask: "-",
        chg: "-",
      };
      return [block, block, block];
    });
  }, [exchanges]);

  const content = useMemo(() => [...items, ...items], [items]);

  const speed = useMemo(() => Math.max(25, items.length * 0.25), [items.length]);

  return (
    <div className="h-7 w-full overflow-hidden bg-black relative text-xs">
      <div
        className="flex h-full items-center whitespace-nowrap text-white text-xs font-mono"
        style={{ animation: `scroll ${speed}s linear infinite` }}
      >
        {content.map((item, i) => (
          <span key={i} className="inline-flex items-center mx-4 shrink-0 gap-2 text-xs">
            <span className=" font-semibold text-xs">{item.symbol}</span>
            <span className=" text-xs">{item.price}</span>
            <div className="flex flex-col">
              <span className=" text-[7px]">{item.bid}</span>
            <span className=" text-[7px]">{item.ask}</span>
            </div>
            <span className=" text-[6px]">{item.chg}</span>
            <span className=" text-xs">|</span>
          </span>
        ))}
      </div>
      <style>{`
        @keyframes scroll {
          0% { transform: translateX(0); }
          100% { transform: translateX(-50%); }
        }
      `}</style>
    </div>
  );
}