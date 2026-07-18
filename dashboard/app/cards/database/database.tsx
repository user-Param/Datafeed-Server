"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

function MiniChart({ value }: { value: number }) {
  const h = 20;
  const w = 60;
  return (
    <svg width={w} height={h} className="inline-block ml-1">
      <rect x="0" y={h - Math.min(value, 20)} width="4" height={Math.min(value, 20)} fill="#3b82f6" rx="1" />
      <rect x="6" y={h - Math.min(value * 0.8, 20)} width="4" height={Math.min(value * 0.8, 20)} fill="#3b82f6" rx="1" />
      <rect x="12" y={h - Math.min(value * 1.2, 20)} width="4" height={Math.min(value * 1.2, 20)} fill="#3b82f6" rx="1" />
      <rect x="18" y={h - Math.min(value * 0.9, 20)} width="4" height={Math.min(value * 0.9, 20)} fill="#3b82f6" rx="1" />
      <rect x="24" y={h - Math.min(value * 1.1, 20)} width="4" height={Math.min(value * 1.1, 20)} fill="#3b82f6" rx="1" />
      <rect x="30" y={h - Math.min(value * 0.7, 20)} width="4" height={Math.min(value * 0.7, 20)} fill="#3b82f6" rx="1" />
      <rect x="36" y={h - Math.min(value, 20)} width="4" height={Math.min(value, 20)} fill="#3b82f6" rx="1" />
    </svg>
  );
}

export default function Database() {
  const { database } = useDatafeed();

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      <div className="flex items-center gap-2">
        <div className=" p-2 text-center flex-1">
          <div className="text-[10px] text-gray-400">Active Connections</div>
          <div className="text-xl font-bold font-mono">{database.active_connections}</div>
        </div>
        <div className=" p-2 text-center flex-1">
          <div className="text-[10px] text-gray-400">Queue Waiting</div>
          <div className="text-xl font-bold font-mono">{database.queue_waiting}</div>
        </div>
      </div>
      <div className="grid grid-cols-4 gap-1 text-[10px]">
        <div className="flex flex-col p-1">
          <span className="text-gray-400">Insert Latency</span>
          <span className="float-right font-mono">{database.insert_latency_ms.toFixed(2)}ms
            <MiniChart value={database.insert_latency_ms} />
          </span>
        </div>
        <div className="flex flex-col p-1">
          <span className="text-gray-400">Query Latency</span>
          <span className="float-right font-mono">{database.query_latency_ms.toFixed(2)}ms
            <MiniChart value={database.query_latency_ms} />
          </span>
        </div>
        <div className="flex flex-col p-1">
          <span className="text-gray-400">Writes/s</span>
          <span className="float-right font-mono">{database.writes_per_sec.toFixed(1)}
            <MiniChart value={database.writes_per_sec} />
          </span>
        </div>
        <div className="flex flex-col p-1">
          <span className="text-gray-400">Reads/s</span>
          <span className="float-right font-mono">{database.reads_per_sec.toFixed(1)}
            <MiniChart value={database.reads_per_sec} />
          </span>
        </div>
      </div>
      <div className="grid grid-cols-2 gap-1 text-[10px]">
        <div className=" p-1">
          <span className="text-gray-400">Transactions</span>
          <span className="float-right font-mono">{database.transaction_count}</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">Conn Failures</span>
          <span className="float-right font-mono">{database.connection_failures}</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">Failed Writes</span>
          <span className="float-right font-mono">{database.failed_writes}</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">Success Writes</span>
          <span className="float-right font-mono">{database.successful_writes}</span>
        </div>
      </div>
    </div>
  );
}
