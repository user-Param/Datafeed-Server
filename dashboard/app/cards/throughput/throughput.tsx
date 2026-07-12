"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

export default function Throughput() {
  const { throughput } = useDatafeed();

  const metrics = throughput
    ? [
        { label: "Messages/s", value: throughput.messages_per_sec, color: "text-blue-400" },
        { label: "Ticks/s", value: throughput.ticks_per_sec, color: "text-green-400" },
        { label: "Trades/s", value: throughput.trades_per_sec, color: "text-yellow-400" },
        { label: "Packets/s", value: throughput.packets_per_sec, color: "text-purple-400" },
        { label: "Bytes/s", value: throughput.bytes_per_sec, color: "text-pink-400" },
        { label: "Broadcasts/s", value: throughput.broadcasts_per_sec, color: "text-indigo-400" },
        { label: "Subscriptions/s", value: throughput.subscriptions_per_sec, color: "text-orange-400" },
        { label: "OB Updates/s", value: throughput.orderbook_updates_per_sec, color: "text-teal-400" },
      ]
    : [];

  const maxValue = Math.max(...metrics.map((m) => m.value), 0.1);

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      {!throughput ? (
        <div className="text-gray-400 text-center mt-8">Waiting for data...</div>
      ) : (
        <>
          <div className="text-center">
            <span className="text-3xl font-bold font-mono text-blue-400">
              {throughput.messages_per_sec.toFixed(1)}
            </span>
            <span className="text-gray-400 ml-1">msg/s</span>
          </div>
          <div className="space-y-1">
            {metrics.map(({ label, value, color }) => {
              const pct = Math.min((value / maxValue) * 100, 100);
              return (
                <div key={label}>
                  <div className="flex justify-between mb-0.5">
                    <span>{label}</span>
                    <span className={`font-mono ${color}`}>{value.toFixed(2)}</span>
                  </div>
                  <div className="w-full bg-gray-700 rounded-full h-1.5">
                    <div
                      className={`h-1.5 rounded-full transition-all duration-500 ${color.replace("text", "bg")}`}
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
        </>
      )}
    </div>
  );
}
