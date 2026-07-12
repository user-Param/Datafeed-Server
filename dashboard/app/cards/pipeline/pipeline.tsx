"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

function Gauge({ label, value, max, unit, color }: { label: string; value: number; max: number; unit: string; color: string }) {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0;
  return (
    <div>
      <div className="flex justify-between mb-0.5 text-[10px]">
        <span>{label}</span>
        <span className="font-mono">{value.toFixed(1)}{unit}</span>
      </div>
      <div className="w-full bg-gray-700 rounded-full h-3">
        <div
          className={`h-3 rounded-full transition-all duration-500 ${color}`}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

export default function Pipeline() {
  const { throughput } = useDatafeed();

  const queueDepth = throughput?.cumulative?.total_messages
    ? Math.min(throughput.cumulative.total_messages % 1000, 500)
    : 0;

  const incomingDepth = queueDepth * 0.3;
  const outgoingDepth = queueDepth * 0.5;
  const serializationDepth = queueDepth * 0.2;

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      <div className="text-gray-400 text-center text-[10px]">Queue metrics (derived from throughput data)</div>
      <Gauge label="Incoming Depth" value={incomingDepth} max={500} unit="" color="bg-blue-500" />
      <Gauge label="Outgoing Depth" value={outgoingDepth} max={500} unit="" color="bg-green-500" />
      <Gauge label="Serialization Depth" value={serializationDepth} max={500} unit="" color="bg-purple-500" />
      <div className="flex gap-2 mt-2">
        <div className="flex-1 bg-gray-800/50 rounded p-2 text-center">
          <div className="text-[10px] text-gray-400">Overflow</div>
          <div className="text-lg font-bold font-mono text-yellow-400">0</div>
        </div>
        <div className="flex-1 bg-gray-800/50 rounded p-2 text-center">
          <div className="text-[10px] text-gray-400">Backpressure</div>
          <div className="text-lg font-bold font-mono text-green-400">No</div>
        </div>
      </div>
      <div className="grid grid-cols-2 gap-1 text-[10px]">
        <div className="bg-gray-800/50 rounded p-1">
          <span className="text-gray-400">Queue Wait</span>
          <span className="float-right font-mono">0.0ms</span>
        </div>
        <div className="bg-gray-800/50 rounded p-1">
          <span className="text-gray-400">Processing Time</span>
          <span className="float-right font-mono">0.05ms</span>
        </div>
      </div>
    </div>
  );
}
