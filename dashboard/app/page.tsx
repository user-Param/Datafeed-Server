"use client";

import Navbar from "./component/navbar";
import Sidebar from "./component/sidebar";
import Ticker from "./component/ticker";
import { useState, useCallback } from 'react';
import DashboardGrid from './component/dashboardgrid';
import Card from './cards/card';
import Chart from "./cards/charts/chart";
import Performance from "./cards/performance/performance";
import Latency from "./cards/latency/latency"
import Health from "./cards/health/health"
import Throughput from "./cards/throughput/throughput"
import Exchange from "./cards/exchange/exchange"
import Pipeline from "./cards/pipeline/pipeline"
import Pannel from "./cards/pannel/pannel"
import Network from "./cards/network/network"
import Database from "./cards/database/database"
import Event from "./cards/event/event"
import Insight from "./cards/insight/insight"
import Config from "./cards/config/config"
import Session from "./cards/session/session"
import { DatafeedProvider } from "./lib/datafeed-context";


interface CardItem {
  id: string;
  title: string;
  content: React.ReactNode;
}

export default function Home() {

  const [sidebarExpanded, setSidebarExpanded] = useState(false);

  const toggleSidebar = () => setSidebarExpanded((prev) => !prev);


  const [cards, setCards] = useState<CardItem[]>([
    { id: 'chart', title: 'Charts', content: <Chart /> },
    { id: 'health', title: 'Feed Health', content: <Health /> },
    { id: 'throughput', title: 'Throughput', content: <Throughput /> },
    { id: 'latency', title: 'Latency', content: <Latency /> },
    { id: 'performance', title: 'Performance', content: <Performance /> },
    { id: 'exchange', title: 'Exchange', content: <Exchange /> },
    { id: 'pipeline', title: 'Pipeline', content: <Pipeline /> },
    { id: 'network', title: 'Network', content: <Network /> },
    { id: 'database', title: 'Database', content: <Database /> },
    { id: 'event', title: 'Event', content: <Event /> },
    { id: 'insight', title: 'Insight', content: <Insight /> },
    { id: 'config', title: 'Config', content: <Config /> },
    { id: 'session', title: 'Session', content: <Session /> },
    { id: 'pannel', title: 'Pannel', content: <Pannel />},

  ]);
  const [minimized, setMinimized] = useState<Record<string, boolean>>({});

    const [refreshTriggers, setRefreshTriggers] = useState<Record<string, number>>({});


  const handleRemove = (id: string) => {
    setCards((prev) => prev.filter((card) => card.id !== id));
  };

  const handleToggleMinimize = (id: string) => {
    setMinimized((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const handleRefresh = useCallback((id: string) => {
    setRefreshTriggers((prev) => ({
      ...prev,
      [id]: (prev[id] || 0) + 1,
    }));
  }, []);
  
  return (
    <DatafeedProvider>
    <div>
      <Navbar />
      <div className="h-full w-full flex">
        <div
            className={`transition-all duration-300 ${
              sidebarExpanded ? "w-56" : "w-12"
            } flex-shrink-0`}
          >
            <Sidebar expanded={sidebarExpanded} onToggle={toggleSidebar} />
          </div>
        <div className="w-[97%]"><Ticker />
          <DashboardGrid>
        {cards.map((card) => (
          <Card
            key={card.id}
            id={card.id}
            title={card.title}
            onRemove={handleRemove}
            onToggleMinimize={handleToggleMinimize}
            onRefresh={handleRefresh}   

            isMinimized={minimized[card.id] || false}
          >
            <div key={refreshTriggers[card.id] || 0}>
                    {card.content}
                  </div>
          </Card>
        ))}
      </DashboardGrid>
        </div>
      </div>
    </div>
    </DatafeedProvider>
  );
}
