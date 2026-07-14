"use client";

import { ChevronLeft, ChevronRight } from "lucide-react";

interface SidebarProps {
  expanded: boolean;
  onToggle: () => void;
}

export default function Sidebar({ expanded, onToggle }: SidebarProps) {
  return (
    <div className="bg-[#101010] text-white h-screen flex flex-col py-4">
      {/* Toggle button at the top */}
      <button
        onClick={onToggle}
        className="p-2 hover:bg-[#161616] rounded transition-colors mb-4"
        aria-label="Toggle sidebar"
      >
        {expanded ? <ChevronLeft size={20} /> : <ChevronRight size={20} />}
      </button>

      {/* Sidebar content – conditionally show labels */}
      <div className="flex flex-col w-full">
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Dashboard</span> : <span className="text-xs">dash</span>}
        </div>
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Settings</span> : <span className="text-xs">sett</span>}
        </div>
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Settings</span> : <span className="text-xs">sett</span>}
        </div>
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Settings</span> : <span className="text-xs">sett</span>}
        </div>
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Settings</span> : <span className="text-xs">sett</span>}
        </div>
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Settings</span> : <span className="text-xs">sett</span>}
        </div>
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Settings</span> : <span className="text-xs">sett</span>}
        </div>
        <div className="w-full px-2 py-1">
          {expanded ? <span className="text-sm px-2 bg-[#161616]">Settings</span> : <span className="text-xs">sett</span>}
        </div>

        {/* Add more items as needed */}
      </div>
    </div>
  );
}