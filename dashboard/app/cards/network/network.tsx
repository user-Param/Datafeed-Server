"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

export default function Network() {
  const { throughput } = useDatafeed();

  const bytesPerSec = throughput?.bytes_per_sec ?? 0;
  const packetsPerSec = throughput?.packets_per_sec ?? 0;

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      <div className="grid grid-cols-2 gap-2">
        <div className=" p-3 text-center">
          <div className="text-[10px] text-gray-400">Bandwidth</div>
          <div className="text-xl font-bold font-mono">
            {(bytesPerSec / 1024).toFixed(1)}
            <span className="text-[10px] text-gray-400 ml-0.5">KB/s</span>
          </div>
        </div>
        <div className=" p-3 text-center">
          <div className="text-[10px] text-gray-400">Packets/s</div>
          <div className="text-xl font-bold font-mono">
            {packetsPerSec.toFixed(1)}
          </div>
        </div>
      </div>
      <div className="grid grid-cols-2 gap-1 text-[10px]">
        <div className=" p-1">
          <span className="text-gray-400">Socket RTT</span>
          <span className="float-right font-mono">—</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">TCP Reconnects</span>
          <span className="float-right font-mono">0</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">Socket Errors</span>
          <span className="float-right font-mono">0</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">Read Errors</span>
          <span className="float-right font-mono">0</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">Write Errors</span>
          <span className="float-right font-mono">0</span>
        </div>
        <div className=" p-1">
          <span className="text-gray-400">TLS Failures</span>
          <span className="float-right font-mono">0</span>
        </div>
      </div>
    </div>
  );
}
