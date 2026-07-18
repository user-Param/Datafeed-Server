"use client";

import React, { createContext, useContext, useEffect, useState, useRef, useCallback } from "react";

const HOST = "16.170.110.184:4444";
const BASE_URL = `http://${HOST}`;
const WS_URL = `ws://${HOST}`;

export interface LatencyStats {
  average: number;
  maximum: number;
  minimum: number;
  p50: number;
  p95: number;
  p99: number;
  sample_count: number;
}

export interface PerformanceData {
  [category: string]: LatencyStats;
}

export interface ThroughputData {
  messages_per_sec: number;
  packets_per_sec: number;
  ticks_per_sec: number;
  trades_per_sec: number;
  bytes_per_sec: number;
  orderbook_updates_per_sec: number;
  broadcasts_per_sec: number;
  subscriptions_per_sec: number;
  cumulative: {
    total_messages: number;
    total_ticks: number;
    total_trades: number;
    total_packets: number;
    total_bytes: number;
    total_broadcasts: number;
    total_orderbook_updates: number;
    total_subscriptions: number;
  };
  database_reads_per_sec: number;
  database_writes_per_sec: number;
}

export interface HealthData {
  db: string;
  feed_instances: number;
  status: string;
}

export interface FeedHealthData {
  health_score: number;
  status: number;
  stale_feed: boolean;
  corrupted_packets: number;
  duplicate_packets: number;
  invalid_messages: number;
  missing_ticks: number;
  out_of_order_packets: number;
  packet_drops: number;
  parse_failures: number;
  sequence_gaps: number;
}

export interface ConfigData {
  version: string;
  adapter_version: string;
  deployment_id: string;
  schema_version: number;
  runtime?: Record<string, string>;
}

export interface AnalyticsData {
  average_latency: number;
  worst_latency: number;
  peak_throughput: number;
  average_throughput: number;
  peak_cpu: number;
  average_cpu: number;
  peak_memory: number;
  average_memory: number;
  most_active_exchange: string;
  exchange_uptime_seconds: number;
  health_score: number;
  database_performance: {
    insert_latency_ms: number;
    query_latency_ms: number;
    reads_per_sec: number;
    writes_per_sec: number;
  };
}

export interface DatabaseData {
  active_connections: number;
  connection_failures: number;
  failed_writes: number;
  insert_latency_ms: number;
  query_latency_ms: number;
  queue_waiting: number;
  reads_per_sec: number;
  successful_writes: number;
  transaction_count: number;
  writes_per_sec: number;
}

export interface SessionData {
  active_sessions: number;
  active_clients: number;
  active_subscriptions: number;
  authentication_failures: number;
  avg_session_duration_ms: number;
  longest_session_duration_ms: number;
  reconnect_count: number;
  total_connections: number;
  total_disconnections: number;
}

export interface AlertItem {
  id?: string;
  message: string;
  severity: string;
  timestamp?: string;
}

export interface AuditEvent {
  id?: string;
  timestamp?: string;
  action: string;
  actor: string;
  target: string;
  result: string;
}

export interface ExchangeData {
  name: string;
  status: string;
  uptime: number;
  latency: number;
  messages_received: number;
  messages_dropped: number;
  parse_errors: number;
  connected_at?: string;
}

export interface ThresholdItem {
  metric_name?: string;
  metric?: string;
  warning_threshold?: number;
  warning?: number;
  critical_threshold?: number;
  critical?: number;
}

interface DatafeedState {
  performance: PerformanceData | Record<string, never>;
  throughput: ThroughputData;
  health: HealthData;
  feedHealth: FeedHealthData;
  config: ConfigData;
  analytics: AnalyticsData;
  database: DatabaseData;
  session: SessionData;
  alerts: AlertItem[];
  audit: AuditEvent[];
  exchanges: ExchangeData[];
  thresholds: ThresholdItem[];
  connected: boolean;
  lastUpdate: number;
  tickerData: Record<string, { symbol: string; price: number; bid: number; ask: number; timestamp: number }>;
}

interface DatafeedContextValue extends DatafeedState {
  refresh: () => void;
}

const defaultState: DatafeedState = {
  performance: {},
  throughput: {
    messages_per_sec: 0,
    packets_per_sec: 0,
    ticks_per_sec: 0,
    trades_per_sec: 0,
    bytes_per_sec: 0,
    orderbook_updates_per_sec: 0,
    broadcasts_per_sec: 0,
    subscriptions_per_sec: 0,
    cumulative: {
      total_messages: 0,
      total_ticks: 0,
      total_trades: 0,
      total_packets: 0,
      total_bytes: 0,
      total_broadcasts: 0,
      total_orderbook_updates: 0,
      total_subscriptions: 0,
    },
    database_reads_per_sec: 0,
    database_writes_per_sec: 0,
  },
  health: {
    db: "unknown",
    feed_instances: 0,
    status: "unknown",
  },
  feedHealth: {
    health_score: 0,
    status: 0,
    stale_feed: false,
    corrupted_packets: 0,
    duplicate_packets: 0,
    invalid_messages: 0,
    missing_ticks: 0,
    out_of_order_packets: 0,
    packet_drops: 0,
    parse_failures: 0,
    sequence_gaps: 0,
  },
  config: {
    version: "unknown",
    adapter_version: "unknown",
    deployment_id: "unknown",
    schema_version: 0,
  },
  analytics: {
    average_latency: 0,
    worst_latency: 0,
    peak_throughput: 0,
    average_throughput: 0,
    peak_cpu: 0,
    average_cpu: 0,
    peak_memory: 0,
    average_memory: 0,
    most_active_exchange: "unknown",
    exchange_uptime_seconds: 0,
    health_score: 0,
    database_performance: {
      insert_latency_ms: 0,
      query_latency_ms: 0,
      reads_per_sec: 0,
      writes_per_sec: 0,
    },
  },
  database: {
    active_connections: 0,
    connection_failures: 0,
    failed_writes: 0,
    insert_latency_ms: 0,
    query_latency_ms: 0,
    queue_waiting: 0,
    reads_per_sec: 0,
    successful_writes: 0,
    transaction_count: 0,
    writes_per_sec: 0,
  },
  session: {
    active_sessions: 0,
    active_clients: 0,
    active_subscriptions: 0,
    authentication_failures: 0,
    avg_session_duration_ms: 0,
    longest_session_duration_ms: 0,
    reconnect_count: 0,
    total_connections: 0,
    total_disconnections: 0,
  },
  alerts: [],
  audit: [],
  exchanges: [],
  thresholds: [],
  connected: false,
  lastUpdate: 0,
  tickerData: {},
};

const DatafeedContext = createContext<DatafeedContextValue>({
  ...defaultState,
  refresh: () => {},
});



export function useDatafeed() {
  return useContext(DatafeedContext);
}

async function fetchJSON<T = unknown>(url: string, label: string): Promise<T | null> {
  try {
    console.log(`[Datafeed] REST fetch: ${label} -> ${url}`);
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) {
      console.error(`[Datafeed] REST fetch failed: ${label} -> ${url}`);
      return null;
    }
    const data = (await res.json()) as T;
    //console.log(`[Datafeed] REST ${label} success:`, data);
    return data;
  } catch {
    //console.error(`[Datafeed] REST fetch error: ${label} -> ${url}`);
    return null;
  }
}

export function DatafeedProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<DatafeedState>(defaultState);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollTimer = useRef<ReturnType<typeof setInterval> | null>(null);

  const update = useCallback((partial: Partial<DatafeedState>) => {
    setState((prev) => {
      const newState = { ...prev, lastUpdate: Date.now() };
      Object.keys(partial).forEach((key) => {
        const value = partial[key as keyof DatafeedState];
        // Only update if value is not null or undefined
        // This preserves default values when API calls fail
        if (value !== null && value !== undefined) {
          (newState as any)[key] = value;
        }
      });
      return newState;
    });
  }, []);

  const fetchAll = useCallback(async () => {
    console.log("[Datafeed] Starting full REST poll...");
    const [
      performance,
      throughput,
      health,
      feedHealth,
      config,
      analytics,
      database,
      session,
      alerts,
      audit,
      exchangesRaw,
      thresholds,
    ] = await Promise.all([
      fetchJSON<PerformanceData>(`${BASE_URL}/api/v1/performance/live`),
      fetchJSON<ThroughputData>(`${BASE_URL}/api/v1/throughput/live`),
      fetchJSON<HealthData>(`${BASE_URL}/health`),
      fetchJSON<FeedHealthData>(`${BASE_URL}/api/v1/feed/live`),
      fetchJSON<ConfigData>(`${BASE_URL}/api/v1/config`),
      fetchJSON<AnalyticsData>(`${BASE_URL}/api/v1/analytics`),
      fetchJSON<DatabaseData>(`${BASE_URL}/api/v1/database/live`),
      fetchJSON<SessionData>(`${BASE_URL}/api/v1/sessions/live`),
      fetchJSON<AlertItem[]>(`${BASE_URL}/api/v1/alerts`),
      fetchJSON<AuditEvent[]>(`${BASE_URL}/api/v1/audit`),
      fetchJSON<string[]>(`${BASE_URL}/api/v1/exchanges`),
      fetchJSON<ThresholdItem[]>(`${BASE_URL}/api/v1/thresholds`),
    ]);

    const exchanges = Array.isArray(exchangesRaw)
      ? exchangesRaw.map((name) => ({
          name,
          status: "unknown",
          uptime: 0,
          latency: 0,
          messages_received: 0,
          messages_dropped: 0,
          parse_errors: 0,
        }))
      : [];


    console.log("[Datafeed] REST poll completed.");
    update({
      performance: performance || {},
      throughput: throughput || {
        messages_per_sec: 0,
        packets_per_sec: 0,
        ticks_per_sec: 0,
        trades_per_sec: 0,
        bytes_per_sec: 0,
        orderbook_updates_per_sec: 0,
        broadcasts_per_sec: 0,
        subscriptions_per_sec: 0,
        cumulative: {
          total_messages: 0,
          total_ticks: 0,
          total_trades: 0,
          total_packets: 0,
          total_bytes: 0,
          total_broadcasts: 0,
          total_orderbook_updates: 0,
          total_subscriptions: 0,
        },
        database_reads_per_sec: 0,
        database_writes_per_sec: 0,
      },
      health: health || {
        db: "unknown",
        feed_instances: 0,
        status: "unknown",
      },
      feedHealth: feedHealth || {
        health_score: 0,
        status: 0,
        stale_feed: false,
        corrupted_packets: 0,
        duplicate_packets: 0,
        invalid_messages: 0,
        missing_ticks: 0,
        out_of_order_packets: 0,
        packet_drops: 0,
        parse_failures: 0,
        sequence_gaps: 0,
      },
      config: config || {
        version: "unknown",
        adapter_version: "unknown",
        deployment_id: "unknown",
        schema_version: 0,
      },
      analytics: analytics || {
        average_latency: 0,
        worst_latency: 0,
        peak_throughput: 0,
        average_throughput: 0,
        peak_cpu: 0,
        average_cpu: 0,
        peak_memory: 0,
        average_memory: 0,
        most_active_exchange: "unknown",
        exchange_uptime_seconds: 0,
        health_score: 0,
        database_performance: {
          insert_latency_ms: 0,
          query_latency_ms: 0,
          reads_per_sec: 0,
          writes_per_sec: 0,
        },
      },
      database: database || {
        active_connections: 0,
        connection_failures: 0,
        failed_writes: 0,
        insert_latency_ms: 0,
        query_latency_ms: 0,
        queue_waiting: 0,
        reads_per_sec: 0,
        successful_writes: 0,
        transaction_count: 0,
        writes_per_sec: 0,
      },
      session: session || {
        active_sessions: 0,
        active_clients: 0,
        active_subscriptions: 0,
        authentication_failures: 0,
        avg_session_duration_ms: 0,
        longest_session_duration_ms: 0,
        reconnect_count: 0,
        total_connections: 0,
        total_disconnections: 0,
      },
      alerts: Array.isArray(alerts) ? alerts : [],
      audit: Array.isArray(audit) ? audit : [],
      exchanges,
      thresholds: Array.isArray(thresholds) ? thresholds : [],
      connected: true,
    });
  }, [update]);

  const refresh = useCallback(() => {
    fetchAll();
  }, [fetchAll]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
        console.log("[Datafeed] Provider mounted, initializing data fetch.");

    fetchAll();
    pollTimer.current = setInterval(fetchAll, 2000);

    function connectWs() {
            console.log(`[Datafeed] WebSocket connecting to ${WS_URL}...`);

      try {
        const ws = new WebSocket(WS_URL);
        ws.onopen = () => {
          console.log("[Datafeed] WebSocket connected.");
          const topics = ["performance", "throughput", "exchange", "queues", "network", "database", "sessions", "health", "system", "ticker_"];
          topics.forEach((topic) => {
            // Try both formats; we'll send the most common one
            const subMsg = JSON.stringify({ type: "subscribe", topic });
            ws.send(subMsg);
            console.log(`[Datafeed] WS sent subscription: ${subMsg}`);
          });

          const switchMsg = JSON.stringify({
            type: "switch_exchange",
            exchange: "BINANCE",
            symbols: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOTUSDT"]
          });
          ws.send(switchMsg);
          console.log(`[Datafeed] WS sent switch_exchange: ${switchMsg}`);
        };
        function tryExtractTicker(source: Record<string, unknown> | null | undefined): { symbol: string; price: number; bid: number; ask: number; timestamp: number } | null {
          const raw = source || {};
          // Normalise field names — support both verbose (symbol, price, bid, ask) and abbreviated (s, c, b, a) conventions
          const symbol = (raw.symbol || raw.s || "") as string;
          const price  = parseFloat(raw.price  as string) || parseFloat(raw.c as string) || parseFloat(raw.lastPrice as string) || (typeof raw.price === "number" ? raw.price as number : NaN);
          const bid    = parseFloat(raw.bid as string) || parseFloat(raw.b as string) || (typeof raw.bid === "number" ? raw.bid as number : 0);
          const ask    = parseFloat(raw.ask as string) || parseFloat(raw.a as string) || (typeof raw.ask === "number" ? raw.ask as number : 0);
          if (symbol && !isNaN(price)) {
            return { symbol, price, bid, ask, timestamp: Date.now() };
          }
          return null;
        }

ws.onmessage = (event: MessageEvent) => {
  console.log("[Datafeed] WS raw message:", event.data);

  try {
    const msg = JSON.parse(event.data) as Record<string, unknown>;
    console.log("[Datafeed] WS parsed message:", msg);

    // -- TICKER DETECTION (run first, regardless of topic) --
    // The server may send ticker data directly or nested under msg.data,
    // and field names may vary (symbol/s, price/c/lastPrice, bid/b, ask/a).
    const tickerPayload = tryExtractTicker(msg) || tryExtractTicker(msg.data as Record<string, unknown> | undefined);
    if (tickerPayload) {
      setState((prev) => ({
        ...prev,
        tickerData: {
          ...prev.tickerData,
          [tickerPayload.symbol]: tickerPayload,
        },
        lastUpdate: Date.now(),
      }));
      return; // handled – skip topic dispatch
    }

    // -- TOPIC-BASED DISPATCH --
    const topic = (msg.topic || msg.type || msg.channel || msg.event || "unknown") as string;
    const data = msg.data !== undefined ? msg.data : msg;

    console.log(`[Datafeed] WS message received: topic="${topic}"`, data);

    if (topic === "performance" || topic === "perf") {
      update({ performance: data as PerformanceData || {} });
    } else if (topic === "throughput" || topic === "thru") {
      update({ throughput: data as ThroughputData || {
        messages_per_sec: 0,
        packets_per_sec: 0,
        ticks_per_sec: 0,
        trades_per_sec: 0,
        bytes_per_sec: 0,
        orderbook_updates_per_sec: 0,
        broadcasts_per_sec: 0,
        subscriptions_per_sec: 0,
        cumulative: {
          total_messages: 0,
          total_ticks: 0,
          total_trades: 0,
          total_packets: 0,
          total_bytes: 0,
          total_broadcasts: 0,
          total_orderbook_updates: 0,
          total_subscriptions: 0,
        },
        database_reads_per_sec: 0,
        database_writes_per_sec: 0,
      }});
    } else if (topic === "exchange" || topic === "exchanges") {
      const exchangeList: ExchangeData[] = [];
          for (const key in data) {
            if (key === "type") continue;
            const ex = (data as Record<string, unknown>)[key];
            if (typeof ex === "object" && ex !== null) {
              exchangeList.push({
                name: key,
                status: (ex as Record<string, unknown>).connected ? ((ex as Record<string, unknown>).stale ? "stale" : "connected") : "disconnected",
                uptime: (ex as Record<string, unknown>).uptime_seconds as number || 0,
                latency: (ex as Record<string, unknown>).latency_ms as number || 0,
                messages_received: (ex as Record<string, unknown>).messages_received as number || 0,
                messages_dropped: (ex as Record<string, unknown>).messages_dropped as number || 0,
                parse_errors: (ex as Record<string, unknown>).parse_errors as number || 0,
              });
            }
          }
              update({ exchanges: exchangeList });
    } else if (topic === "database") {
      update({ database: data as DatabaseData || {
        active_connections: 0,
        connection_failures: 0,
        failed_writes: 0,
        insert_latency_ms: 0,
        query_latency_ms: 0,
        queue_waiting: 0,
        reads_per_sec: 0,
        successful_writes: 0,
        transaction_count: 0,
        writes_per_sec: 0,
      }});
    } else if (topic === "sessions" || topic === "session") {
      update({ session: data as SessionData || {
        active_sessions: 0,
        active_clients: 0,
        active_subscriptions: 0,
        authentication_failures: 0,
        avg_session_duration_ms: 0,
        longest_session_duration_ms: 0,
        reconnect_count: 0,
        total_connections: 0,
        total_disconnections: 0,
      }});
    } else if (topic === "health") {
      update({ health: data as HealthData || {
        db: "unknown",
        feed_instances: 0,
        status: "unknown",
      }});
    } else if (topic === "feed" || topic === "feedhealth") {
      update({ feedHealth: data as FeedHealthData || {
        health_score: 0,
        status: 0,
        stale_feed: false,
        corrupted_packets: 0,
        duplicate_packets: 0,
        invalid_messages: 0,
        missing_ticks: 0,
        out_of_order_packets: 0,
        packet_drops: 0,
        parse_failures: 0,
        sequence_gaps: 0,
      }});
    } else if (topic === "queues" || topic === "queue") {
      console.log("[Datafeed] Queue data received:", data);
    } else if (topic === "network") {
      console.log("[Datafeed] Network data received:", data);
    } else if (topic === "system") {
      console.log("[Datafeed] System data received:", data);
    } else {
      console.log(`[Datafeed] Unhandled WS topic: ${topic}`, data);
    }
  } catch (err) {
    console.error("[Datafeed] Error parsing WebSocket message:", err, "Raw:", event.data);
  }
};
        ws.onclose = () => {
          console.warn("[Datafeed] WebSocket closed. Reconnecting in 5s...");
          wsRef.current = null;
          reconnectTimer.current = setTimeout(connectWs, 5000);
        };
        ws.onerror = (err) => {
          console.error("[Datafeed] WebSocket error:", err);
          ws.close(); // Will trigger reconnect via onclose.
        };
        wsRef.current = ws;
      } catch (err) {
        console.error("[Datafeed] WebSocket connection error:", err);
        // Retry anyway.
        reconnectTimer.current = setTimeout(connectWs, 5000);
      }
    }

    connectWs();

    return () => {
      console.log("[Datafeed] Provider unmounting, cleaning up...");
      if (pollTimer.current) clearInterval(pollTimer.current);
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (wsRef.current){ 
        wsRef.current.close()
        wsRef.current = null;
      };
    };
  }, [fetchAll, update]);

  return (
    <DatafeedContext.Provider value={{ ...state, refresh }}>
      {children}
    </DatafeedContext.Provider>
  );
}
