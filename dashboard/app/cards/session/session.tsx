"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

export default function Session() {
  const { session } = useDatafeed();

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      {!session ? (
        <div className="text-gray-400 text-center mt-8">Waiting for session data...</div>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-2">
            <div className=" p-2 text-center">
              <div className="text-[10px] text-gray-400">Active Sessions</div>
              <div className="text-xl font-bold font-mono">{session.active_sessions}</div>
            </div>
            <div className=" p-2 text-center">
              <div className="text-[10px] text-gray-400">Active Clients</div>
              <div className="text-xl font-bold font-mono">{session.active_clients}</div>
            </div>
            <div className=" p-2 text-center">
              <div className="text-[10px] text-gray-400">Subscriptions</div>
              <div className="text-xl font-bold font-mono">{session.active_subscriptions}</div>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-1 text-[10px]">
            <div className=" p-1">
              <span className="text-gray-400">Total Connections</span>
              <span className="float-right font-mono">{session.total_connections}</span>
            </div>
            <div className=" p-1">
              <span className="text-gray-400">Total Disconnections</span>
              <span className="float-right font-mono">{session.total_disconnections}</span>
            </div>
            <div className=" p-1">
              <span className="text-gray-400">Auth Failures</span>
              <span className="float-right font-mono">{session.authentication_failures}</span>
            </div>
            <div className=" p-1">
              <span className="text-gray-400">Reconnects</span>
              <span className="float-right font-mono">{session.reconnect_count}</span>
            </div>
            <div className=" p-1">
              <span className="text-gray-400">Avg Duration</span>
              <span className="float-right font-mono">{session.avg_session_duration_ms.toFixed(1)}ms</span>
            </div>
            <div className=" p-1">
              <span className="text-gray-400">Longest Duration</span>
              <span className="float-right font-mono">{session.longest_session_duration_ms.toFixed(1)}ms</span>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
