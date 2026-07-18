"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

export default function Throughput() {
  const { throughput } = useDatafeed();

  const metrics = [
    { label: "Messages/s", value: throughput.messages_per_sec  },
    { label: "Ticks/s", value: throughput.ticks_per_sec  },
    { label: "Trades/s", value: throughput.trades_per_sec  },
    { label: "Packets/s", value: throughput.packets_per_sec  },
    { label: "Bytes/s", value: throughput.bytes_per_sec  },
    { label: "Broadcasts/s", value: throughput.broadcasts_per_sec },
    { label: "Subscriptions/s", value: throughput.subscriptions_per_sec  },
    { label: "OB Updates/s", value: throughput.orderbook_updates_per_sec },
  ];

  const maxValue = Math.max(...metrics.map((m) => m.value), 0.1);

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      <div className="text-center">
        <span className="text-3xl font-bold font-mono">
          {throughput.messages_per_sec.toFixed(1)}
        </span>
        <span className="text-gray-400 ml-1">msg/s</span>
      </div>
      <div className="space-y-1">
        {metrics.map(({ label, value }) => {
          const pct = Math.min((value / maxValue) * 100, 100);
          return (
            <div key={label}>
              <div className="flex justify-between mb-0.5">
                <span>{label}</span>
                <span className="font-mono">{value.toFixed(2)}</span>
              </div>
              <div className="w-full bg-gray-700 rounded-full h-1.5">
                <div
                  className="h-1.5 transition-all duration-500 bg-red-500"
                  style={{ width: `${pct}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>
      {throughput.cumulative && (
        <div className="grid grid-cols-2 gap-1 text-[10px] mt-1">
          <div className="bg-gray-800/50 rounded p-1">
            <span className="text-gray-400">Total Msgs</span>
            <span className="float-right font-mono">{throughput.cumulative.total_messages.toLocaleString()}</span>
          </div>
          <div className="bg-gray-800/50 rounded p-1">
            <span className="text-gray-400">Total Ticks</span>
            <span className="float-right font-mono">{throughput.cumulative.total_ticks.toLocaleString()}</span>
          </div>
          <div className="bg-gray-800/50 rounded p-1">
            <span className="text-gray-400">Total Bytes</span>
            <span className="float-right font-mono">{throughput.cumulative.total_bytes.toLocaleString()}</span>
          </div>
          <div className="bg-gray-800/50 rounded p-1">
            <span className="text-gray-400">Total Packets</span>
            <span className="float-right font-mono">{throughput.cumulative.total_packets.toLocaleString()}</span>
          </div>
        </div>
      )}
    </div>
  );
}
