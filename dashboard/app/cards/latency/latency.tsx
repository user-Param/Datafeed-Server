"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";
import React, { useEffect, useState } from "react";
import type { LatencyStats } from "@/app/lib/datafeed-context";

// Optional: Map internal keys to human-readable names
const categoryDisplayNames: Record<string, string> = {
  exchange: "Exchange",
  parsing: "Parsing",
  normalization: "Normalization",
  processing: "Processing",
  serialization: "Serialization",
  broadcast: "Broadcast",
  socket_send: "Socket Send",
  general: "General",
};

export default function Latency() {
  const { performance } = useDatafeed();
  const [entries, setEntries] = useState<
    { key: string; name: string; stats: LatencyStats }[]
  >([]);

  useEffect(() => {
    if (!performance || Object.keys(performance).length === 0) {
      return;
    }

    const newEntries = Object.entries(performance)
      .filter(
        ([key, value]) =>
          key !== "type" &&
          value &&
          typeof value === "object" &&
          "sample_count" in value
      )
      .map(([key, value]) => ({
        key,
        name: categoryDisplayNames[key] || key.charAt(0).toUpperCase() + key.slice(1),
        stats: value as LatencyStats,
      }));

    if (newEntries.length > 0) {
      setEntries(newEntries);
    }
  }, [performance]);

  if (entries.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400">
        No latency data available
      </div>
    );
  }

  return (
    <div className="h-full w-full p-2 overflow-auto">
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-2 text-gray-400">
        {entries.map((item) => (
          <div key={item.key} className="p-2 transition-colors">
            <div className="text-xs">{item.name}</div>
            <div className="text-sm font-mono">
              Avg:{" "}
              <span className="text-white">
                {item.stats.average?.toFixed(2) ?? "—"}
              </span>
            </div>
            <div className="text-xs flex gap-2 flex-wrap">
              <span>P50: {item.stats.p50?.toFixed(2) ?? "—"}</span>
              <span>P95: {item.stats.p95?.toFixed(2) ?? "—"}</span>
              <span>P99: {item.stats.p99?.toFixed(2) ?? "—"}</span>
            </div>
            <div className="text-xs mt-1">Samples: {item.stats.sample_count ?? 0}</div>
          </div>
        ))}
      </div>
    </div>
  );
}