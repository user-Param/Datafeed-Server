"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

export default function Health() {
  const { health, feedHealth, alerts } = useDatafeed();

  const statusColor =
    health.status === "healthy"
      ? "text-green-400"
      : health.status === "degraded"
        ? "text-yellow-400"
        : "text-red-400";

  const statusDot =
    health.status === "healthy"
      ? "up"
      : health.status === "degraded"
        ? "down"
        : "down";

  const dbColor = health.db === "connected" ? "text-green-400" : "text-red-400";
  const healthScore = feedHealth?.health_score ?? (health.status === "healthy" ? 100 : 0);
  const circumference = 2 * Math.PI * 36;
  const offset = circumference - (healthScore / 100) * circumference;

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      <div className="flex items-center gap-3">
        <div className="relative w-20 h-20">
          <svg className="w-20 h-20 -rotate-90" viewBox="0 0 80 80">
            <circle cx="40" cy="40" r="36" fill="none" stroke="#1f2937" strokeWidth="6" />
            <circle
              cx="40" cy="40" r="36"
              fill="none"
              stroke={healthScore > 80 ? "#22c55e" : healthScore > 50 ? "#eab308" : "#ef4444"}
              strokeWidth="6"
              strokeDasharray={circumference}
              strokeDashoffset={offset}
              strokeLinecap="round"
            />
          </svg>
          <div className="absolute inset-0 flex items-center justify-center">
            <span className="text-xl font-bold">{healthScore}</span>
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-1">
            <span className={statusColor}>{statusDot}</span>
            <span className="capitalize">{health.status}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className={dbColor}>{health.db === "connected" ? "up" : "down"}</span>
            <span>DB: {health.db}</span>
          </div>
          <span className="text-gray-400">Feed Instances: {health.feed_instances}</span>
        </div>
      </div>
      <div className="grid grid-cols-2 gap-1 text-[10px]">
        <div className="p-1">
          <span className="text-gray-400">Corrupted</span>
          <span className="float-right font-mono">{feedHealth.corrupted_packets}</span>
        </div>
        <div className="p-1">
          <span className="text-gray-400">Drops</span>
          <span className="float-right font-mono">{feedHealth.packet_drops}</span>
        </div>
        <div className="p-1">
          <span className="text-gray-400">Parse Failures</span>
          <span className="float-right font-mono">{feedHealth.parse_failures}</span>
        </div>
        <div className="p-1">
          <span className="text-gray-400">Sequence Gaps</span>
          <span className="float-right font-mono">{feedHealth.sequence_gaps}</span>
        </div>
      </div>
      <div className="flex-1">
        <div className="text-gray-400 text-[10px] mb-1">Recent Alerts ({alerts.length})</div>
        <div className="space-y-0.5 overflow-auto max-h-20">
          {alerts.length === 0 && (
            <div className="text-gray-500 text-[10px]">No active alerts</div>
          )}
          {alerts.slice(0, 5).map((a, i) => (
            <div key={a.id || i} className="bg-red-900/20 border border-red-800/50 rounded px-1.5 py-0.5 text-[10px] flex justify-between">
              <span>{a.message}</span>
              <span className="text-gray-400">{a.severity}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
