"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

function KpiCard({ label, value, unit, color }: { label: string; value: string | number; unit?: string; color: string }) {
  return (
    <div className=" p-2">
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
    <div className="h-full w-full p-2 flex flex-col text-xs overflow-auto">
      {!analytics ? (
        <div className="text-gray-400 text-center mt-8">Waiting for analytics...</div>
      ) : (
        <>
          <div className="text-[10px] text-gray-400 mb-0.5">Latency</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Average Latency" value={analytics.average_latency} unit="ms" />
            <KpiCard label="Worst Latency" value={analytics.worst_latency} unit="ms"  />
          </div>
          <div className="text-[10px] text-gray-400 mb-0.5 mt-1">Throughput</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Peak Throughput" value={analytics.peak_throughput} unit="msg/s"  />
            <KpiCard label="Average Throughput" value={analytics.average_throughput} unit="msg/s"  />
          </div>
          <div className="text-[10px] text-gray-400 mb-0.5 mt-1">System</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Peak CPU" value={analytics.peak_cpu} unit="%"  />
            <KpiCard label="Average CPU" value={analytics.average_cpu} unit="%"  />
            <KpiCard label="Peak Memory" value={(analytics.peak_memory / 1024 / 1024).toFixed(1)} unit="MB"  />
            <KpiCard label="Average Memory" value={(analytics.average_memory / 1024 / 1024).toFixed(1)} unit="MB"  />
          </div>
          <div className="text-[10px] text-gray-400 mb-0.5 mt-1">Exchange</div>
          <div className="grid grid-cols-2 gap-1">
            <KpiCard label="Most Active" value={analytics.most_active_exchange || "N/A"}  />
            <KpiCard label="Total Uptime" value={analytics.exchange_uptime_seconds} unit="s"  />
          </div>
          <div className="mt-1 text-right">
          </div>
        </>
      )}
    </div>
  );
}
