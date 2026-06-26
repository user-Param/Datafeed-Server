-- Schema for datafeed PostgreSQL database

-- 0) Historical market price data (used by DAdapter / backtest_source)
CREATE TABLE IF NOT EXISTS market_data (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(50)     NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    bid         DOUBLE PRECISION NOT NULL DEFAULT 0,
    ask         DOUBLE PRECISION NOT NULL DEFAULT 0,
    quantity    INTEGER         NOT NULL DEFAULT 0,
    timeframe   VARCHAR(10),                          -- e.g. '1m','5m','1h','1d'
    timestamp   BIGINT          NOT NULL,             -- milliseconds since epoch
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_data_symbol_ts    ON market_data(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_market_data_timeframe    ON market_data(timeframe);

-- 1) Client / tenant identity
CREATE TABLE clients (
    tenant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_name VARCHAR(255) NOT NULL,
    plan VARCHAR(50),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    auth_subject VARCHAR(255), -- assuming this is for authentication subject (like user ID or API key reference)
    ip_address INET,
    user_agent TEXT
);

-- 2) Session state
CREATE TABLE sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id UUID,
    connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    disconnected_at TIMESTAMPTZ,
    disconnect_reason VARCHAR(100),
    auth_status VARCHAR(20), -- e.g., 'authenticated', 'unauthenticated', 'pending'
    reconnect_count INTEGER DEFAULT 0,
    heartbeat_interval INTERVAL,
    protocol VARCHAR(10) CHECK (protocol IN ('ws', 'http', 'internal')),
    instance_id VARCHAR(50),
    tenant_id UUID REFERENCES clients(tenant_id) ON DELETE SET NULL
);

-- 3) Subscription state
CREATE TABLE subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(50) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    stream_type VARCHAR(20), -- e.g., 'ticker', 'trade', 'orderbook'
    mode VARCHAR(20), -- e.g., 'realtime', 'snapshot'
    filters_json JSONB,
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    removed_at TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    tenant_id UUID REFERENCES clients(tenant_id) ON DELETE CASCADE,
    session_id UUID REFERENCES sessions(session_id) ON DELETE SET NULL
);

-- 4) Runtime feed health
CREATE TABLE feed_instances (
    instance_id VARCHAR(50) PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL, -- e.g., 'BINANCE', 'JUPITER', 'BIRDEYE'
    adapter_type VARCHAR(20), -- e.g., 'exchange', 'database', 'backtest'
    feed_status VARCHAR(20), -- e.g., 'connected', 'disconnected', 'reconnecting', 'stale'
    last_tick_at TIMESTAMPTZ,
    stale_seconds INTERVAL,
    reconnect_attempts INTEGER DEFAULT 0,
    message_rate_in DOUBLE PRECISION,
    message_rate_out DOUBLE PRECISION,
    queue_depth INTEGER,
    backpressure_active BOOLEAN DEFAULT FALSE,
    serialization_ms DOUBLE PRECISION,
    parse_error_count INTEGER DEFAULT 0,
    gap_count INTEGER DEFAULT 0,
    duplicate_count INTEGER DEFAULT 0,
    out_of_order_count INTEGER DEFAULT 0,
    tenant_id UUID REFERENCES clients(tenant_id) ON DELETE SET NULL
);

-- 5) Performance metrics snapshots (time-series)
CREATE TABLE feed_metrics_snapshots (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Legacy latency columns
    p50_latency_ms DOUBLE PRECISION,
    p95_latency_ms DOUBLE PRECISION,
    p99_latency_ms DOUBLE PRECISION,
    avg_latency_ms DOUBLE PRECISION,
    drop_rate DOUBLE PRECISION,
    packet_loss_rate DOUBLE PRECISION,
    msgs_sent BIGINT,
    msgs_received BIGINT,
    bytes_sent BIGINT,
    bytes_received BIGINT,
    cpu_usage DOUBLE PRECISION,
    memory_usage BIGINT,
    thread_count INTEGER,
    event_loop_lag_ms DOUBLE PRECISION,
    uptime_seconds BIGINT,

    -- Per-category latency percentiles (most commonly queried)
    exchange_p50_ms DOUBLE PRECISION,
    exchange_p95_ms DOUBLE PRECISION,
    exchange_p99_ms DOUBLE PRECISION,
    parsing_p50_ms DOUBLE PRECISION,
    parsing_p95_ms DOUBLE PRECISION,
    parsing_p99_ms DOUBLE PRECISION,
    normalization_p50_ms DOUBLE PRECISION,
    normalization_p95_ms DOUBLE PRECISION,
    normalization_p99_ms DOUBLE PRECISION,
    processing_p50_ms DOUBLE PRECISION,
    processing_p95_ms DOUBLE PRECISION,
    processing_p99_ms DOUBLE PRECISION,
    broadcast_p50_ms DOUBLE PRECISION,
    broadcast_p95_ms DOUBLE PRECISION,
    broadcast_p99_ms DOUBLE PRECISION,
    serialization_p50_ms DOUBLE PRECISION,
    serialization_p95_ms DOUBLE PRECISION,
    serialization_p99_ms DOUBLE PRECISION,
    socket_send_p50_ms DOUBLE PRECISION,
    socket_send_p95_ms DOUBLE PRECISION,
    socket_send_p99_ms DOUBLE PRECISION,

    -- Full per-category latency details (JSONB: all p50/p95/p99/avg/min/max/count per category)
    latency_stats_jsonb JSONB,

    -- Throughput rates (per second)
    messages_per_sec DOUBLE PRECISION,
    packets_per_sec DOUBLE PRECISION,
    bytes_per_sec DOUBLE PRECISION,
    ticks_per_sec DOUBLE PRECISION,
    trades_per_sec DOUBLE PRECISION,
    orderbook_updates_per_sec DOUBLE PRECISION,
    broadcasts_per_sec DOUBLE PRECISION,
    subscriptions_per_sec DOUBLE PRECISION,
    database_writes_per_sec DOUBLE PRECISION,
    database_reads_per_sec DOUBLE PRECISION,

    -- Cumulative totals
    total_messages BIGINT,
    total_packets BIGINT,
    total_bytes BIGINT,
    total_ticks BIGINT,
    total_trades BIGINT,
    total_orderbook_updates BIGINT,
    total_broadcasts BIGINT,
    total_subscriptions BIGINT,
    total_database_writes BIGINT,
    total_database_reads BIGINT,

    -- Queue metrics
    incoming_queue_depth INTEGER,
    outgoing_queue_depth INTEGER,
    serialization_queue_depth INTEGER,
    max_incoming_queue_depth INTEGER,
    max_outgoing_queue_depth INTEGER,
    max_serialization_queue_depth INTEGER,
    queue_overflow_count INTEGER,
    queue_wait_time_ms DOUBLE PRECISION,
    queue_processing_time_ms DOUBLE PRECISION,
    queue_backpressure BOOLEAN,

    -- Feed health
    packet_drops BIGINT,
    duplicate_packets BIGINT,
    out_of_order_packets BIGINT,
    sequence_gaps BIGINT,
    missing_ticks BIGINT,
    invalid_messages BIGINT,
    corrupted_packets BIGINT,
    parse_failures BIGINT,
    stale_feed BOOLEAN,
    feed_health_score INTEGER,
    feed_health_status VARCHAR(20),

    -- Session metrics
    active_clients INTEGER,
    active_sessions INTEGER,
    active_subscriptions INTEGER,
    total_connections INTEGER,
    total_disconnections INTEGER,
    reconnect_count INTEGER,
    authentication_failures INTEGER,
    avg_session_duration_ms DOUBLE PRECISION,
    longest_session_duration_ms DOUBLE PRECISION,

    -- Network metrics
    tcp_reconnects INTEGER,
    socket_errors INTEGER,
    read_errors INTEGER,
    write_errors INTEGER,
    tls_handshake_failures INTEGER,
    network_bytes_transmitted BIGINT,
    network_bytes_received BIGINT,
    socket_rtt_ms DOUBLE PRECISION,
    network_bandwidth_bps DOUBLE PRECISION,
    network_connection_failures INTEGER,

    -- Database metrics
    db_successful_writes BIGINT,
    db_failed_writes BIGINT,
    db_insert_latency_ms DOUBLE PRECISION,
    db_query_latency_ms DOUBLE PRECISION,
    db_active_connections INTEGER,
    db_connection_failures INTEGER,
    db_transaction_count BIGINT,
    db_writes_per_sec DOUBLE PRECISION,
    db_reads_per_sec DOUBLE PRECISION,
    db_queue_waiting INTEGER,

    -- Extended system metrics
    peak_rss BIGINT,
    virtual_memory BIGINT,
    heap_usage BIGINT,
    memory_growth_rate DOUBLE PRECISION
);

-- 6) Audit / action log
CREATE TABLE feed_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_type VARCHAR(20), -- 'user', 'system', 'client'
    actor_id VARCHAR(255), -- could be user ID, client ID, or system component
    action_type VARCHAR(50) NOT NULL, -- subscribe, unsubscribe, switch_exchange, reconnect, replay_start
    target_type VARCHAR(50), -- e.g., 'subscription', 'client', 'feed_instance'
    target_id VARCHAR(255), -- ID of the target
    result VARCHAR(20), -- 'success', 'failure'
    error_code VARCHAR(50),
    trace_id UUID,
    correlation_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB -- for additional context
);

-- 7) API request log
CREATE TABLE api_requests (
    request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code INTEGER,
    latency_ms DOUBLE PRECISION,
    request_size INTEGER,
    response_size INTEGER,
    client_id UUID REFERENCES clients(tenant_id) ON DELETE SET NULL,
    session_id UUID REFERENCES sessions(session_id) ON DELETE SET NULL,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE SET NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 8) Exchange health
CREATE TABLE exchange_health (
    id BIGSERIAL PRIMARY KEY,
    exchange_name VARCHAR(20) NOT NULL,
    endpoint VARCHAR(255),
    status VARCHAR(20), -- 'online', 'offline', 'degraded'
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    error_count INTEGER DEFAULT 0,
    rate_limit_hits INTEGER DEFAULT 0,
    latency_ms DOUBLE PRECISION,
    symbols_active INTEGER,
    feed_lag_ms DOUBLE PRECISION,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 9) Backtest / replay state
CREATE TABLE backtest_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    symbol VARCHAR(50),
    exchange VARCHAR(20),

    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,

    replay_speed INTEGER DEFAULT 1,

    status VARCHAR(20),

    progress DOUBLE PRECISION DEFAULT 0,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ);

-- 10) Config / versioning
CREATE TABLE config_versions (
    id BIGSERIAL PRIMARY KEY,
    config_version VARCHAR(50),
    build_sha CHAR(40),
    adapter_version VARCHAR(50),
    deployment_id VARCHAR(100),
    feature_flags JSONB,
    schema_version INTEGER,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==============================================================================
-- 11) Exchange metrics history — one row per exchange per snapshot interval
-- ==============================================================================
CREATE TABLE exchange_metrics_history (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    exchange_name VARCHAR(50) NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    connected BOOLEAN NOT NULL DEFAULT FALSE,
    uptime_seconds DOUBLE PRECISION,
    reconnect_count INTEGER DEFAULT 0,
    heartbeat_failures INTEGER DEFAULT 0,
    websocket_disconnects INTEGER DEFAULT 0,
    messages_received BIGINT DEFAULT 0,
    messages_dropped BIGINT DEFAULT 0,
    parse_errors INTEGER DEFAULT 0,
    feed_lag_ms DOUBLE PRECISION,
    exchange_latency_ms DOUBLE PRECISION,
    stale BOOLEAN DEFAULT FALSE
);

-- ==============================================================================
-- 12) Queue history — queue depth history over time
-- ==============================================================================
CREATE TABLE queue_history (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    incoming_depth INTEGER NOT NULL DEFAULT 0,
    outgoing_depth INTEGER NOT NULL DEFAULT 0,
    serialization_depth INTEGER NOT NULL DEFAULT 0,
    max_incoming_depth INTEGER DEFAULT 0,
    max_outgoing_depth INTEGER DEFAULT 0,
    max_serialization_depth INTEGER DEFAULT 0,
    overflow_count INTEGER DEFAULT 0,
    backpressure BOOLEAN DEFAULT FALSE,
    wait_time_ms DOUBLE PRECISION,
    processing_time_ms DOUBLE PRECISION
);

-- ==============================================================================
-- 13) System metrics history — CPU, memory, threads and uptime over time
-- ==============================================================================
CREATE TABLE system_metrics_history (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cpu_usage_percent DOUBLE PRECISION,
    memory_rss BIGINT,
    peak_rss BIGINT,
    virtual_memory BIGINT,
    heap_usage BIGINT,
    memory_growth_rate DOUBLE PRECISION,
    thread_count INTEGER,
    uptime_seconds DOUBLE PRECISION
);

-- ==============================================================================
-- 14) Network metrics history — bandwidth, RTT and socket errors over time
-- ==============================================================================
CREATE TABLE network_metrics_history (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tcp_reconnects INTEGER DEFAULT 0,
    socket_errors INTEGER DEFAULT 0,
    read_errors INTEGER DEFAULT 0,
    write_errors INTEGER DEFAULT 0,
    tls_handshake_failures INTEGER DEFAULT 0,
    bytes_transmitted BIGINT DEFAULT 0,
    bytes_received BIGINT DEFAULT 0,
    socket_rtt_ms DOUBLE PRECISION,
    bandwidth_bps DOUBLE PRECISION,
    connection_failures INTEGER DEFAULT 0
);

-- ==============================================================================
-- 15) Database metrics history — database performance history
-- ==============================================================================
CREATE TABLE database_metrics_history (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    successful_writes BIGINT DEFAULT 0,
    failed_writes BIGINT DEFAULT 0,
    insert_latency_ms DOUBLE PRECISION,
    query_latency_ms DOUBLE PRECISION,
    active_connections INTEGER DEFAULT 0,
    connection_failures INTEGER DEFAULT 0,
    transaction_count BIGINT DEFAULT 0,
    writes_per_sec DOUBLE PRECISION,
    reads_per_sec DOUBLE PRECISION,
    queue_waiting INTEGER DEFAULT 0
);

-- ==============================================================================
-- 16) Alerts — triggered when metrics cross thresholds
-- ==============================================================================
CREATE TABLE alerts (
    alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE SET NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    source VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    current_value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    message TEXT,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

-- ==============================================================================
-- 17) Metric thresholds — configurable warning/critical thresholds per metric
-- ==============================================================================
CREATE TABLE metric_thresholds (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    metric_name VARCHAR(100) NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'feed',
    warning_threshold DOUBLE PRECISION,
    critical_threshold DOUBLE PRECISION,
    operator VARCHAR(10) NOT NULL DEFAULT 'gt' CHECK (operator IN ('gt', 'lt', 'gte', 'lte', 'eq')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    cooldown_seconds INTEGER DEFAULT 300,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instance_id, metric_name, source)
);

-- ==============================================================================
-- Indexes for performance
-- ==============================================================================

-- Existing indexes (kept intact)
CREATE INDEX IF NOT EXISTS idx_sessions_tenant_id ON sessions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_sessions_instance_id ON sessions(instance_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_tenant_id ON subscriptions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_session_id ON subscriptions(session_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_symbol ON subscriptions(symbol);
CREATE INDEX IF NOT EXISTS idx_feed_instances_tenant_id ON feed_instances(tenant_id);
CREATE INDEX IF NOT EXISTS idx_feed_metrics_snapshots_instance_id ON feed_metrics_snapshots(instance_id);
CREATE INDEX IF NOT EXISTS idx_feed_metrics_snapshots_measured_at ON feed_metrics_snapshots(measured_at);
CREATE INDEX IF NOT EXISTS idx_feed_events_occurred_at ON feed_events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_api_requests_timestamp ON api_requests(timestamp);
CREATE INDEX IF NOT EXISTS idx_api_requests_client_id ON api_requests(client_id);
CREATE INDEX IF NOT EXISTS idx_exchange_health_exchange_name ON exchange_health(exchange_name);
CREATE INDEX IF NOT EXISTS idx_backtest_jobs_status ON backtest_jobs(status);
CREATE INDEX IF NOT EXISTS idx_config_versions_applied_at ON config_versions(applied_at);

-- New indexes for history tables
CREATE INDEX IF NOT EXISTS idx_exchange_metrics_history_instance_id ON exchange_metrics_history(instance_id);
CREATE INDEX IF NOT EXISTS idx_exchange_metrics_history_snapshot_time ON exchange_metrics_history(snapshot_time);
CREATE INDEX IF NOT EXISTS idx_exchange_metrics_history_exchange_name ON exchange_metrics_history(exchange_name);
CREATE INDEX IF NOT EXISTS idx_queue_history_instance_id ON queue_history(instance_id);
CREATE INDEX IF NOT EXISTS idx_queue_history_measured_at ON queue_history(measured_at);
CREATE INDEX IF NOT EXISTS idx_system_metrics_history_instance_id ON system_metrics_history(instance_id);
CREATE INDEX IF NOT EXISTS idx_system_metrics_history_measured_at ON system_metrics_history(measured_at);
CREATE INDEX IF NOT EXISTS idx_network_metrics_history_instance_id ON network_metrics_history(instance_id);
CREATE INDEX IF NOT EXISTS idx_network_metrics_history_measured_at ON network_metrics_history(measured_at);
CREATE INDEX IF NOT EXISTS idx_database_metrics_history_instance_id ON database_metrics_history(instance_id);
CREATE INDEX IF NOT EXISTS idx_database_metrics_history_measured_at ON database_metrics_history(measured_at);

-- Alert indexes
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_instance_id ON alerts(instance_id);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged ON alerts(acknowledged);
CREATE INDEX IF NOT EXISTS idx_alerts_source ON alerts(source);

-- Metric threshold indexes
CREATE INDEX IF NOT EXISTS idx_metric_thresholds_instance_id ON metric_thresholds(instance_id);
CREATE INDEX IF NOT EXISTS idx_metric_thresholds_metric_name ON metric_thresholds(metric_name);
CREATE INDEX IF NOT EXISTS idx_metric_thresholds_enabled ON metric_thresholds(enabled);

-- ==============================================================================
-- 18) Weekly rollup — aggregated from 15-minute summaries
-- ==============================================================================
CREATE TABLE IF NOT EXISTS weekly_metrics_summary (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(50) REFERENCES feed_instances(instance_id) ON DELETE CASCADE,
    week_start TIMESTAMPTZ NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,

    -- Same structure as feed_metrics_snapshots but with weekly-averaged values
    p50_latency_ms DOUBLE PRECISION,
    p95_latency_ms DOUBLE PRECISION,
    p99_latency_ms DOUBLE PRECISION,
    avg_latency_ms DOUBLE PRECISION,
    drop_rate DOUBLE PRECISION,
    packet_loss_rate DOUBLE PRECISION,
    msgs_sent BIGINT,
    msgs_received BIGINT,
    bytes_sent BIGINT,
    bytes_received BIGINT,
    cpu_usage DOUBLE PRECISION,
    memory_usage BIGINT,
    thread_count INTEGER,
    event_loop_lag_ms DOUBLE PRECISION,
    uptime_seconds BIGINT,

    exchange_p50_ms DOUBLE PRECISION, exchange_p95_ms DOUBLE PRECISION, exchange_p99_ms DOUBLE PRECISION,
    parsing_p50_ms DOUBLE PRECISION, parsing_p95_ms DOUBLE PRECISION, parsing_p99_ms DOUBLE PRECISION,
    normalization_p50_ms DOUBLE PRECISION, normalization_p95_ms DOUBLE PRECISION, normalization_p99_ms DOUBLE PRECISION,
    processing_p50_ms DOUBLE PRECISION, processing_p95_ms DOUBLE PRECISION, processing_p99_ms DOUBLE PRECISION,
    broadcast_p50_ms DOUBLE PRECISION, broadcast_p95_ms DOUBLE PRECISION, broadcast_p99_ms DOUBLE PRECISION,
    serialization_p50_ms DOUBLE PRECISION, serialization_p95_ms DOUBLE PRECISION, serialization_p99_ms DOUBLE PRECISION,
    socket_send_p50_ms DOUBLE PRECISION, socket_send_p95_ms DOUBLE PRECISION, socket_send_p99_ms DOUBLE PRECISION,

    latency_stats_jsonb JSONB,

    messages_per_sec DOUBLE PRECISION, packets_per_sec DOUBLE PRECISION, bytes_per_sec DOUBLE PRECISION,
    ticks_per_sec DOUBLE PRECISION, trades_per_sec DOUBLE PRECISION,
    orderbook_updates_per_sec DOUBLE PRECISION, broadcasts_per_sec DOUBLE PRECISION,
    subscriptions_per_sec DOUBLE PRECISION, database_writes_per_sec DOUBLE PRECISION,
    database_reads_per_sec DOUBLE PRECISION,

    total_messages BIGINT, total_packets BIGINT, total_bytes BIGINT,
    total_ticks BIGINT, total_trades BIGINT, total_orderbook_updates BIGINT, total_broadcasts BIGINT,
    total_subscriptions BIGINT, total_database_writes BIGINT, total_database_reads BIGINT,

    incoming_queue_depth INTEGER, outgoing_queue_depth INTEGER, serialization_queue_depth INTEGER,
    max_incoming_queue_depth INTEGER, max_outgoing_queue_depth INTEGER, max_serialization_queue_depth INTEGER,
    queue_overflow_count INTEGER, queue_wait_time_ms DOUBLE PRECISION,
    queue_processing_time_ms DOUBLE PRECISION, queue_backpressure BOOLEAN,

    packet_drops BIGINT, duplicate_packets BIGINT, out_of_order_packets BIGINT,
    sequence_gaps BIGINT, missing_ticks BIGINT, invalid_messages BIGINT,
    corrupted_packets BIGINT, parse_failures BIGINT, stale_feed BOOLEAN,
    feed_health_score INTEGER, feed_health_status VARCHAR(20),

    active_clients INTEGER, active_sessions INTEGER, active_subscriptions INTEGER,
    total_connections INTEGER, total_disconnections INTEGER, reconnect_count INTEGER,
    authentication_failures INTEGER, avg_session_duration_ms DOUBLE PRECISION,
    longest_session_duration_ms DOUBLE PRECISION,

    tcp_reconnects INTEGER, socket_errors INTEGER, read_errors INTEGER, write_errors INTEGER,
    tls_handshake_failures INTEGER, network_bytes_transmitted BIGINT, network_bytes_received BIGINT,
    socket_rtt_ms DOUBLE PRECISION, network_bandwidth_bps DOUBLE PRECISION,
    network_connection_failures INTEGER,

    db_successful_writes BIGINT, db_failed_writes BIGINT,
    db_insert_latency_ms DOUBLE PRECISION, db_query_latency_ms DOUBLE PRECISION,
    db_active_connections INTEGER, db_connection_failures INTEGER, db_transaction_count BIGINT,
    db_writes_per_sec DOUBLE PRECISION, db_reads_per_sec DOUBLE PRECISION, db_queue_waiting INTEGER,

    peak_rss BIGINT, virtual_memory BIGINT, heap_usage BIGINT, memory_growth_rate DOUBLE PRECISION,

    UNIQUE (instance_id, week_start)
);

CREATE INDEX IF NOT EXISTS idx_weekly_summary_instance_id ON weekly_metrics_summary(instance_id);
CREATE INDEX IF NOT EXISTS idx_weekly_summary_week_start ON weekly_metrics_summary(week_start);

-- Note: Historical snapshots represent the raw state of the system at each sampling interval.
-- feed_metrics_snapshots stores 15-minute aggregated summaries (not per-second raw data).
-- Derived values (e.g., uptime, rates) are stored at snapshot time and not recalculated.
-- Per-category latency percentiles beyond the commonly queried ones are stored in latency_stats_jsonb.
-- 15-minute summaries older than 90 days are automatically deleted after weekly rollup.