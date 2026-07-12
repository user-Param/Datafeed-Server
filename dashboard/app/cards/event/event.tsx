"use client";

import { useDatafeed } from "@/app/lib/datafeed-context";

export default function Event() {
  const { audit } = useDatafeed();

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      {audit.length === 0 ? (
        <div className="text-gray-400 text-center mt-8">No audit events available</div>
      ) : (
        <>
          <div className="flex gap-2 text-[10px]">
            <span className="text-gray-400">{audit.length} events</span>
          </div>
          <div className="overflow-auto flex-1">
            <table className="w-full text-[10px] border-collapse">
              <thead>
                <tr className="text-gray-400 border-b border-gray-700">
                  <th className="text-left py-1">Time</th>
                  <th className="text-left py-1">Action</th>
                  <th className="text-left py-1">Actor</th>
                  <th className="text-left py-1">Target</th>
                  <th className="text-center py-1">Result</th>
                </tr>
              </thead>
              <tbody>
                {audit.slice(0, 50).map((evt, i) => (
                  <tr key={evt.id || i} className="border-b border-gray-800 hover:bg-gray-800/50">
                    <td className="py-1 text-gray-400 whitespace-nowrap">
                      {evt.timestamp ? new Date(evt.timestamp).toLocaleTimeString() : "-"}
                    </td>
                    <td className="py-1">{evt.action}</td>
                    <td className="py-1">{evt.actor}</td>
                    <td className="py-1">{evt.target}</td>
                    <td className="text-center">
                      <span className={evt.result === "success" ? "text-green-400" : "text-red-400"}>
                        {evt.result}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
