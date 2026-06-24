-- Seed data for datafeed PostgreSQL database

-- Insert a default client for testing/development
INSERT INTO clients (client_name, plan, status, auth_subject)
VALUES
('Default Client', 'development', 'active', 'dev-user-001')
ON CONFLICT DO NOTHING;

-- Insert initial exchange health records for supported exchanges
INSERT INTO exchange_health (exchange_name, endpoint, status, last_success_at)
VALUES
('BINANCE', 'wss://stream.binance.com:9443/stream', 'online', NOW()),
('JUPITER', 'https://api.jup.ag/price/v3', 'online', NOW()),
('BIRDEYE', 'https://public-api.birdeye.so/defi/price', 'online', NOW())
ON CONFLICT DO NOTHING;

-- Insert initial configuration version
INSERT INTO config_versions (config_version, build_sha, adapter_version, deployment_id, feature_flags, schema_version)
VALUES
('1.0.0', 'dev-build-001', '1.0.0', 'dev-deployment-001', '{"debug": true, "metrics_enabled": true}', 1)
ON CONFLICT DO NOTHING;

-- Note: Additional seed data can be added here as needed for specific environments
-- For example:
-- - Initial feature flags
-- - Default subscription templates
-- - Rate limit configurations
-- - Alert thresholds

-- Example: Insert a default feature flag configuration
-- INSERT INTO config_versions (config_version, build_sha, adapter_version, deployment_id, feature_flags, schema_version)
-- VALUES
-- ('1.0.1', 'dev-build-002', '1.0.0', 'dev-deployment-002', '{"debug": true, "metrics_enabled": true, "new_feature": false}', 2)
-- ON CONFLICT DO NOTHING;