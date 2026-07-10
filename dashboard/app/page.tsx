"use client";

import Navbar from "./component/navbar";
import Sidebar from "./component/sidebar";
import Ticker from "./component/ticker";
import { useState } from 'react';
import DashboardGrid from './component/dashboardgrid';
import Card from './cards/card';
import Chart from "./cards/charts/chart";
import Performance from "./cards/performance/performance";
import Latency from "./cards/latency/latency"
import Health from "./cards/health/health"
import Throughput from "./cards/throughput/throughput"
import Exchange from "./cards/exchange/exchange"
import Pipeline from "./cards/pipeline/pipeline"
import Network from "./cards/network/network"
import Database from "./cards/database/database"
import Event from "./cards/event/event"
import Insight from "./cards/insight/insight"
import Config from "./cards/config/config"
import Session from "./cards/session/session"


interface CardItem {
  id: string;
  title: string;
  content: React.ReactNode;
}

export default function Home() {
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

  ]);
  const [minimized, setMinimized] = useState<Record<string, boolean>>({});

  const handleRemove = (id: string) => {
    setCards((prev) => prev.filter((card) => card.id !== id));
  };

  const handleToggleMinimize = (id: string) => {
    setMinimized((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const addCard = () => {
    const newId = `card-${Date.now()}`;
    setCards((prev) => [
      ...prev,
      {
        id: newId,
        title: `New Card ${prev.length + 1}`,
        content: (
          <div className="h-full w-full flex items-center justify-center bg-gray-800/40 rounded">
            <span className="text-gray-400 text-sm">New Widget</span>
          </div>
        ),
      },
    ]);
  };
  return (
    <>
    <div className="flex flex-row">
      <Sidebar />
      <div className="flex flex-col w-full">
        <Navbar />
        <Ticker />
        

        <div className="h-screen w-full bg-gray-950 text-white p-4">
      
      <DashboardGrid>
        {cards.map((card) => (
          <Card
            key={card.id}
            id={card.id}
            title={card.title}
            onRemove={handleRemove}
            onToggleMinimize={handleToggleMinimize}
            isMinimized={minimized[card.id] || false}
          >
            {card.content}
          </Card>
        ))}
      </DashboardGrid>
    </div>









      </div>
    </div>




    
      
      
    </>
  );
}
