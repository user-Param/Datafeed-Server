"use client";

import { useDatafeed, type ThresholdItem } from "@/app/lib/datafeed-context";

export default function Config() {
  const { config, thresholds } = useDatafeed();

  return (
    <div className="h-full w-full p-2 flex flex-col gap-2 text-xs overflow-auto">
      {!config ? (
        <div className="text-gray-400 text-center mt-8">Waiting for config...</div>
      ) : (
        <>
          <div className="text-[10px] text-gray-400 mb-0.5">Runtime Configuration</div>
          <div className="grid grid-cols-2 gap-1 text-[10px]">
            <div className="bg-gray-800/50 rounded p-1 col-span-2">
              <span className="text-gray-400">Version</span>
              <span className="float-right font-mono">{config.version}</span>
            </div>
            <div className="bg-gray-800/50 rounded p-1">
              <span className="text-gray-400">Adapter</span>
              <span className="float-right font-mono">{config.adapter_version}</span>
            </div>
            <div className="bg-gray-800/50 rounded p-1">
              <span className="text-gray-400">Schema</span>
              <span className="float-right font-mono">v{config.schema_version}</span>
            </div>
            <div className="bg-gray-800/50 rounded p-1 col-span-2">
              <span className="text-gray-400">Deployment ID</span>
              <span className="float-right font-mono text-[9px]">{config.deployment_id}</span>
            </div>
          </div>
          {config.runtime && Object.keys(config.runtime).length > 0 && (
            <>
              <div className="text-[10px] text-gray-400 mb-0.5 mt-1">Runtime</div>
              <div className="grid grid-cols-2 gap-1 text-[10px]">
                {Object.entries(config.runtime).map(([key, val]) => (
                  <div key={key} className="bg-gray-800/50 rounded p-1">
                    <span className="text-gray-400 capitalize">{key.replace(/_/g, " ")}</span>
                    <span className="float-right font-mono">{String(val)}</span>
                  </div>
                ))}
              </div>
            </>
          )}
          {thresholds.length > 0 && (
            <>
              <div className="text-[10px] text-gray-400 mb-0.5 mt-1">Thresholds</div>
              <div className="overflow-auto">
                <table className="w-full text-[10px] border-collapse">
                  <thead>
                    <tr className="text-gray-400 border-b border-gray-700">
                      <th className="text-left py-1">Metric</th>
                      <th className="text-right py-1">Warning</th>
                      <th className="text-right py-1">Critical</th>
                    </tr>
                  </thead>
                  <tbody>
                    {thresholds.map((t: ThresholdItem, i: number) => (
                      <tr key={t.metric_name || i} className="border-b border-gray-800">
                        <td className="py-1">{t.metric_name || t.metric}</td>
                        <td className="text-right font-mono">{t.warning_threshold ?? t.warning ?? "-"}</td>
                        <td className="text-right font-mono">{t.critical_threshold ?? t.critical ?? "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </>
      )}
    </div>
  );
}
