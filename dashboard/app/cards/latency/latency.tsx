"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";
import React, { useEffect, useState, useRef, useMemo } from "react";
import type { LatencyStats } from "@/app/lib/datafeed-context";


const categoryNames: Record<number, string> = {
  0: "Exchange",
  1: "Parsing",
  2: "Normalization",
  3: "Processing",
  4: "Serialization",
  5: "Broadcast",
  6: "Socket Send",
  7: "General",
};

const getLatencyColor = (ms: number): string => {
  if (ms < 1) return "text-green-400";
  if (ms < 10) return "text-yellow-400";
  if (ms < 50) return "text-orange-400";
  return "text-red-400";
};

interface LatencyEntry {
  id: number;
  name: string;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  count: number;
}

export default function Latency() {
  const { performance } = useDatafeed();
  const [entries, setEntries] = useState<LatencyEntry[]>([]);
  const [hasData, setHasData] = useState(false);
  const stablePerformanceRef = useRef(performance);

  // Store the latest valid performance data
  useEffect(() => {
    if (performance && Object.keys(performance).length > 1) {
      stablePerformanceRef.current = performance;
      setHasData(true);
    }
  }, [performance]);

  // Process data only when we have valid performance data
  useEffect(() => {
    const dataToProcess = performance || stablePerformanceRef.current;
    
    // Check if we have valid data (more than just the 'type' field)
    if (!dataToProcess || Object.keys(dataToProcess).length <= 1) {
      // If we already have data, keep it - don't show "waiting"
      if (hasData) return;
      setEntries([]);
      return;
    }

    const newEntries = Object.entries(dataToProcess)
      .filter(([key]) => !isNaN(Number(key)) && key !== "type")
      .map(([key, value]) => ({
        id: Number(key),
        name: categoryNames[Number(key)] || `Category ${key}`,
        ...(value as LatencyStats),
      }));

    // Only update if entries actually changed
    const hasChanged = JSON.stringify(newEntries) !== JSON.stringify(entries);
    if (hasChanged && newEntries.length > 0) {
      setEntries(newEntries);
      setHasData(true);
    }
  }, [performance, entries, hasData]);

  // Show loading only on initial mount or if we've never had data
  if (!hasData && entries.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400">
        Waiting for data...
      </div>
    );
  }

  // If entries is empty but we have data flag, show a message
  if (entries.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400">
        No latency data available
      </div>
    );
  }

  return (
    <div className="h-full w-full p-2 overflow-auto">
      <div className="flex justify-between items-center">
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-2">
        {entries.map((item) => (
          <div
            key={item.id}
            className=" p-2 transition-colors"
          >
            <div className="text-xs ">{item.name}</div>
            <div className="text-sm font-mono">
              Avg:{" "}
              <span className={getLatencyColor(item.avg)}>
                {item.avg?.toFixed(2) ?? "—"}
              </span>
            </div>
            <div className="text-xs flex gap-2 flex-wrap">
              <span>P50: {item.p50?.toFixed(2) ?? "—"}</span>
              <span>P95: {item.p95?.toFixed(2) ?? "—"}</span>
              <span>P99: {item.p99?.toFixed(2) ?? "—"}</span>
            </div>
            <div className="text-xs mt-1">Samples: {item.count ?? 0}</div>
          </div>
        ))}
      </div>
    </div>
  );
}