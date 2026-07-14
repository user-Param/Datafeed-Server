export type MenuItem = {
  id: string;
  title: string;
  route: string;
  children?: MenuItem[];
};

export const sidebarConfig: MenuItem[] = [
  {
    id: 'dashboard-group',
    title: 'Dashboard',
    route: '/dashboard',
    children: [
      { id: 'dashboard', title: 'Dashboard', route: '/dashboard' },
      { id: 'command-center', title: 'Command Center', route: '/command-center' },
      { id: 'workspace', title: 'Workspace', route: '/workspace' },
    ],
  },
  {
    id: 'market-feed-group',
    title: 'Market Feed',
    route: '/market-feeds',
    children: [
      { id: 'exchanges', title: 'Exchanges', route: '/exchanges' },
      { id: 'symbols', title: 'Symbols', route: '/symbols' },
      { id: 'feed-pipeline', title: 'Feed Pipeline', route: '/feed-pipeline' },
      { id: 'subscriptions', title: 'Subscriptions', route: '/subscriptions' },
      { id: 'snapshots', title: 'Snapshots', route: '/snapshots' },
    ],
  },
  {
    id: 'performance-group',
    title: 'Performance',
    route: '/performance',
    children: [
      { id: 'latency', title: 'Latency', route: '/latency' },
      { id: 'throughput', title: 'Throughput', route: '/throughput' },
      { id: 'queues', title: 'Queues', route: '/queues' },
      { id: 'network', title: 'Network', route: '/network' },
      { id: 'cpu-memory', title: 'CPU & Memory', route: '/cpu-memory' },
    ],
  },
  {
    id: 'infrastructure-group',
    title: 'Infrastructure',
    route: '/infrastructure',
    children: [
      { id: 'database', title: 'Database', route: '/database' },
      { id: 'sessions', title: 'Sessions', route: '/sessions' },
      { id: 'workers', title: 'Workers', route: '/workers' },
      { id: 'storage', title: 'Storage', route: '/storage' },
      { id: 'servers', title: 'Servers', route: '/servers' },
    ],
  },
  {
    id: 'analytics-group',
    title: 'Analytics',
    route: '/analytics',
    children: [
      { id: 'insights', title: 'Insights', route: '/insights' },
      { id: 'statistics', title: 'Statistics', route: '/statistics' },
      { id: 'historical-replay', title: 'Historical Replay', route: '/historical-replay' },
      { id: 'reports', title: 'Reports', route: '/reports' },
      { id: 'metrics-explorer', title: 'Metrics Explorer', route: '/metrics-explorer' },
    ],
  },
  {
    id: 'monitoring-group',
    title: 'Monitoring',
    route: '/monitoring',
    children: [
      { id: 'health', title: 'Health', route: '/health' },
      { id: 'alerts', title: 'Alerts', route: '/alerts' },
      { id: 'audit-log', title: 'Audit Log', route: '/audit-log' },
      { id: 'logs', title: 'Logs', route: '/logs' },
      { id: 'event-stream', title: 'Event Stream', route: '/event-stream' },
    ],
  },
  {
    id: 'configuration-group',
    title: 'Configuration',
    route: '/configuration',
    children: [
      { id: 'runtime-config', title: 'Runtime Configuration', route: '/runtime-config' },
      { id: 'thresholds', title: 'Thresholds', route: '/thresholds' },
      { id: 'routing', title: 'Routing', route: '/routing' },
      { id: 'feature-flags', title: 'Feature Flags', route: '/feature-flags' },
    ],
  },
  {
    id: 'developer-group',
    title: 'Developer',
    route: '/developer',
    children: [
      { id: 'api-explorer', title: 'API Explorer', route: '/api-explorer' },
      { id: 'websocket-inspector', title: 'WebSocket Inspector', route: '/websocket-inspector' },
      { id: 'raw-metrics', title: 'Raw Metrics', route: '/raw-metrics' },
    ],
  },
];