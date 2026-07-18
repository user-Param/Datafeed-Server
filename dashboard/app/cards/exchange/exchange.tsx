"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";
import React, { useEffect, useState, useMemo } from "react";

interface ExchangeData {
  name: string;
  status: string;
  uptime: number;
  latency: number;
  messages_received: number;
  messages_dropped: number;
  parse_errors: number;
}

interface ExchangeEntry {
  name: string;
  status: "connected" | "disconnected" | "stale" | "unknown";
  uptime: number;
  latency: number;
  messages_received: number;
  messages_dropped: number;
  parse_errors: number;
}

const getStatusColor = (status: string): string => {
  switch (status) {
    case "connected": return "bg-green-400";
    case "stale": return "bg-yellow-400";
    case "disconnected": return "bg-red-400";
    default: return "bg-gray-400";
  }
};

export default function Exchange() {
  const { exchanges } = useDatafeed();
  
  const [entries, setEntries] = useState<ExchangeEntry[]>([]);

  useEffect(() => {
    let validEntries: ExchangeEntry[] = [];

    // Case 1: Array of exchanges
    if (Array.isArray(exchanges)) {
      validEntries = exchanges
        .filter((ex) => ex && typeof ex === 'object')
        .map((ex) => ({
          name: ex.name || ex.exchange || "Unknown",
          status: (ex.status || ex.connection_status || "unknown") as ExchangeEntry["status"],
          uptime: ex.uptime || ex.uptime_seconds || 0,
          latency: ex.latency || ex.latency_ms || ex.exchange_latency_ms || 0,
          messages_received: ex.messages_received || 0,
          messages_dropped: ex.messages_dropped || 0,
          parse_errors: ex.parse_errors || 0,
        }));
    } 
    // Case 2: Object with exchange names as keys
    else if (typeof exchanges === 'object') {
      validEntries = Object.entries(exchanges)
        .filter(([_, ex]) => ex && typeof ex === 'object')
        .map(([name, ex]) => ({
          name: name,
          status: (ex.status || ex.connection_status || "unknown") as ExchangeEntry["status"],
          uptime: ex.uptime || ex.uptime_seconds || 0,
          latency: ex.latency || ex.latency_ms || ex.exchange_latency_ms || 0,
          messages_received: ex.messages_received || 0,
          messages_dropped: ex.messages_dropped || 0,
          parse_errors: ex.parse_errors || 0,
        }));
    }

    if (validEntries.length > 0) {
      setEntries(validEntries);
    }
  }, [exchanges]);

  const summary = useMemo(() => {
    if (entries.length === 0) return null;
    
    const connected = entries.filter(e => e.status === "connected").length;
    const stale = entries.filter(e => e.status === "stale").length;
    const disconnected = entries.filter(e => e.status === "disconnected").length;
    
    return {
      total: entries.length,
      connected,
      stale,
      disconnected,
      healthy: connected === entries.length,
    };
  }, [entries]);

  

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      {summary && (
        <div className="flex gap-4 items-center border-b border-gray-700 pb-2">
          <span className="text-gray-400">Summary:</span>
          <span className="text-green-400">{summary.connected} connected</span>
          {summary.stale > 0 && <span className="text-yellow-400">{summary.stale} stale</span>}
          {summary.disconnected > 0 && <span className="text-red-400">{summary.disconnected} disconnected</span>}
          <span className={`ml-auto ${summary.healthy ? "text-green-400" : "text-yellow-400"}`}>
            {summary.healthy ? "✅ All healthy" : "⚠️ Issues detected"}
          </span>
        </div>
      )}

      <div className="overflow-auto flex-1">
        <table className="w-full text-[10px] border-collapse">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-1">Exchange</th>
              <th className="text-center py-1">Status</th>
              <th className="text-right py-1">Uptime</th>
              <th className="text-right py-1">Latency</th>
              <th className="text-right py-1">Received</th>
              <th className="text-right py-1">Dropped</th>
              <th className="text-right py-1">Errors</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((ex, i) => (
              <tr key={`${ex.name}-${i}`} className="border-b border-gray-800 hover:bg-gray-800/50">
                <td className="py-1 font-medium">{ex.name}</td>
                <td className="text-center">
                  <span
                    className={`inline-block w-2 h-2 rounded-full ${getStatusColor(ex.status)}`}
                  />
                </td>
                <td className="text-right font-mono">{ex.uptime?.toFixed(0) || "-"}s</td>
                <td className="text-right font-mono">{ex.latency?.toFixed(2) || "-"}ms</td>
                <td className="text-right font-mono">{ex.messages_received?.toLocaleString() || "-"}</td>
                <td className="text-right font-mono">{ex.messages_dropped ?? "-"}</td>
                <td className="text-right font-mono">{ex.parse_errors ?? "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}