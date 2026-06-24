-- Schema for datafeed PostgreSQL database

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
    cpu_usage DOUBLE PRECISION, -- percentage
    memory_usage BIGINT, -- in bytes
    thread_count INTEGER,
    event_loop_lag_ms DOUBLE PRECISION,
    uptime_seconds BIGINT
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

-- Indexes for performance
CREATE INDEX idx_sessions_tenant_id ON sessions(tenant_id);
CREATE INDEX idx_sessions_instance_id ON sessions(instance_id);
CREATE INDEX idx_subscriptions_tenant_id ON subscriptions(tenant_id);
CREATE INDEX idx_subscriptions_session_id ON subscriptions(session_id);
CREATE INDEX idx_subscriptions_symbol ON subscriptions(symbol);
CREATE INDEX idx_feed_instances_tenant_id ON feed_instances(tenant_id);
CREATE INDEX idx_feed_metrics_snapshots_instance_id ON feed_metrics_snapshots(instance_id);
CREATE INDEX idx_feed_metrics_snapshots_measured_at ON feed_metrics_snapshots(measured_at);
CREATE INDEX idx_feed_events_occurred_at ON feed_events(occurred_at);
CREATE INDEX idx_api_requests_timestamp ON api_requests(timestamp);
CREATE INDEX idx_api_requests_client_id ON api_requests(client_id);
CREATE INDEX idx_exchange_health_exchange_name ON exchange_health(exchange_name);
CREATE INDEX idx_backtest_jobs_status ON backtest_jobs(status);
CREATE INDEX idx_config_versions_applied_at ON config_versions(applied_at);

-- Note: The user mentioned that certain metrics should be derived, not stored.
-- We have not stored computed columns like uptime, message_count, etc., as per advice.
-- Instead, we store raw events and snapshots, and these can be computed via queries.

-- Example of a view for derived metrics (optional, can be created by application):
-- CREATE VIEW client_active_sessions AS
-- SELECT c.tenant_id, c.client_name, COUNT(s.session_id) AS active_sessions
-- FROM clients c
-- LEFT JOIN sessions s ON c.tenant_id = s.tenant_id AND s.disconnected_at IS NULL
-- GROUP BY c.tenant_id, c.client_name;