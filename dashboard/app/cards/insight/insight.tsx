"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

function KpiCard({ label, value, unit, color }: { label: string; value: string | number; unit?: string; color: string }) {
  return (
    <div className="bg-gray-800/50 rounded p-2">
      <div className="text-[9px] text-gray-400 mb-0.5">{label}</div>
      <div className={`text-sm font-bold font-mono ${color}`}>
        {typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : value}
        {unit && <span className="text-[9px] text-gray-400 ml-0.5">{unit}</span>}
      </div>
    </div>
  );
}

export default function Insight() {
  const { analytics } = useDatafeed();

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      {!analytics ? (
        <div className="text-gray-400 text-center mt-8">Waiting for analytics...</div>
      ) : (
        <>
          <div className="text-[10px] text-gray-400 mb-0.5">Latency</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Average Latency" value={analytics.average_latency} unit="ms" color="text-blue-400" />
            <KpiCard label="Worst Latency" value={analytics.worst_latency} unit="ms" color="text-red-400" />
          </div>
          <div className="text-[10px] text-gray-400 mb-0.5 mt-1">Throughput</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Peak Throughput" value={analytics.peak_throughput} unit="msg/s" color="text-green-400" />
            <KpiCard label="Average Throughput" value={analytics.average_throughput} unit="msg/s" color="text-teal-400" />
          </div>
          <div className="text-[10px] text-gray-400 mb-0.5 mt-1">System</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Peak CPU" value={analytics.peak_cpu} unit="%" color="text-yellow-400" />
            <KpiCard label="Average CPU" value={analytics.average_cpu} unit="%" color="text-orange-400" />
            <KpiCard label="Peak Memory" value={(analytics.peak_memory / 1024 / 1024).toFixed(1)} unit="MB" color="text-purple-400" />
            <KpiCard label="Average Memory" value={(analytics.average_memory / 1024 / 1024).toFixed(1)} unit="MB" color="text-pink-400" />
          </div>
          <div className="text-[10px] text-gray-400 mb-0.5 mt-1">Exchange</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Most Active" value={analytics.most_active_exchange || "N/A"} color="text-indigo-400" />
            <KpiCard label="Total Uptime" value={analytics.exchange_uptime_seconds} unit="s" color="text-cyan-400" />
          </div>
          <div className="text-[10px] text-gray-400 mb-0.5 mt-1">Database</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Insert Latency" value={analytics.database_performance?.insert_latency_ms} unit="ms" color="text-blue-400" />
            <KpiCard label="Query Latency" value={analytics.database_performance?.query_latency_ms} unit="ms" color="text-green-400" />
            <KpiCard label="Writes/s" value={analytics.database_performance?.writes_per_sec} color="text-yellow-400" />
            <KpiCard label="Reads/s" value={analytics.database_performance?.reads_per_sec} color="text-purple-400" />
          </div>
          <div className="mt-1 text-right">
            <span className={`text-sm font-bold ${analytics.health_score > 80 ? "text-green-400" : analytics.health_score > 50 ? "text-yellow-400" : "text-red-400"}`}>
              Health: {analytics.health_score}/100
            </span>
          </div>
        </>
      )}
    </div>
  );
}
