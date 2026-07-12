"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";
import React, { useMemo, useEffect, useRef, useState } from "react";

// Map string keys to display names and colors
const categoryMap: Record<string, { label: string; color: string }> = {
  exchange: { label: "Exchange", color: "bg-red-500" },
  parsing: { label: "Parsing", color: "bg-red-500" },
  normalization: { label: "Normalization", color: "bg-red-500" },
  processing: { label: "Processing", color: "bg-red-500" },
  serialization: { label: "Serialization", color: "bg-red-500" },
  broadcast: { label: "Broadcast", color: "bg-red-500" },
  socket_send: { label: "Socket Send", color: "bg-red-500" },
};

interface LatencyStats {
  average: number;
  minimum: number;
  maximum: number;
  p50: number;
  p95: number;
  p99: number;
  sample_count: number;
}

interface PerformanceEntry {
  key: string;
  label: string;
  color: string;
  stats: LatencyStats;
}

export default function Performance() {
  const { performance } = useDatafeed();
  
  // Store the last valid data to prevent "no data" flicker
  const lastValidDataRef = useRef<any>(null);
  const [hasData, setHasData] = useState(false);
  const [entries, setEntries] = useState<PerformanceEntry[]>([]);

  // Process data when performance changes
  useEffect(() => {
    if (!performance) {
      // If we already have data, keep it - don't show "no data"
      if (hasData) return;
      return;
    }

    // Check if this is valid data (has at least one category with sample_count > 0)
    let hasValidData = false;
    const validEntries: PerformanceEntry[] = [];

    Object.entries(categoryMap).forEach(([key, { label, color }]) => {
      const stats = performance[key] as LatencyStats | undefined;
      if (stats && stats.sample_count !== undefined && stats.sample_count > 0) {
        hasValidData = true;
        validEntries.push({
          key,
          label,
          color,
          stats,
        });
      }
    });

    if (hasValidData) {
      // Store valid data
      lastValidDataRef.current = { performance, entries: validEntries };
      setEntries(validEntries);
      setHasData(true);
    } else if (lastValidDataRef.current) {
      // If no valid data but we have cached data, keep using cached data
      // This prevents the "no data" flicker
      setEntries(lastValidDataRef.current.entries);
      setHasData(true);
    }
  }, [performance, hasData]);

  // Calculate max average for bar chart scaling
  const maxAvg = useMemo(() => {
    if (entries.length === 0) return 1;
    return Math.max(...entries.map((e) => e.stats.average), 1);
  }, [entries]);

  // Show loading only on initial mount
  if (!hasData && entries.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400">
        Waiting for data...
      </div>
    );
  }

  if (entries.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400 flex-col gap-2">
        <span>No performance data available</span>
        <span className="text-xs text-gray-600">
          Keys received: {performance ? Object.keys(performance).join(", ") : "none"}
        </span>
      </div>
    );
  }

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      {/* Bar Chart Section */}
      <div className="space-y-1">
        {entries.map(({ key, label, color, stats }) => {
          const pct = Math.min((stats.average / maxAvg) * 100, 100);
          return (
            <div key={key}>
              <div className="flex justify-between mb-0.5">
                <span>{label}</span>
                <span className="font-mono">{stats.average.toFixed(3)}ms</span>
              </div>
              <div className="w-full bg-gray-700 h-2">
                <div
                  className={`${color} h-2 transition-all duration-300`}
                  style={{ width: `${pct}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>

      {/* Table Section */}
      <div className="overflow-auto mt-1 flex-1">
        <table className="w-full text-[10px] border-collapse">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-1">Category</th>
              <th className="text-right py-1">p50</th>
              <th className="text-right py-1">p95</th>
              <th className="text-right py-1">p99</th>
              <th className="text-right py-1">Count</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(({ key, label, stats }) => (
              <tr key={key} className="border-b border-gray-800 hover:bg-gray-800/50">
                <td className="py-1">{label}</td>
                <td className="text-right font-mono">{stats.p50.toFixed(3)}</td>
                <td className="text-right font-mono">{stats.p95.toFixed(3)}</td>
                <td className="text-right font-mono">{stats.p99.toFixed(3)}</td>
                <td className="text-right font-mono">{stats.sample_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}