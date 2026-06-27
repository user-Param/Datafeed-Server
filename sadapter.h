#ifndef SADAPTER_H
#define SADAPTER_H

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <memory>
#include <mutex>

// ─── nlohmann/json std::optional compatibility ──────────────────────────────
// Older nlohmann/json versions (< 3.6.0) do not support std::optional directly.
// This specialization is activated only when nlohmann/json.hpp has been included.
#ifdef NLOHMANN_JSON_VERSION_MAJOR
namespace nlohmann {
template<typename T>
struct adl_serializer<std::optional<T>> {
    static void to_json(json& j, const std::optional<T>& opt) {
        if (opt.has_value())
            j = *opt;
        else
            j = nullptr;
    }
};
}
#endif

namespace datafeed {

// Define structs that match the database tables
// Using simple types for independence from pqxx in the header
// Timestamps are stored as uint64_t (milliseconds since epoch)
// Intervals are stored as int64_t (seconds)

struct Client {
    std::string tenant_id;
    std::string client_name;
    std::string plan;
    std::string status;
    uint64_t created_at;      // milliseconds since epoch
    uint64_t updated_at;      // milliseconds since epoch
    std::optional<uint64_t> last_seen_at; // milliseconds since epoch
    std::optional<std::string> auth_subject;
    std::optional<std::string> ip_address; // stored as string (INET)
    std::optional<std::string> user_agent;
};

struct Session {
    std::string session_id;
    std::optional<std::string> connection_id;
    uint64_t connected_at;    // milliseconds since epoch
    std::optional<uint64_t> disconnected_at; // milliseconds since epoch
    std::optional<std::string> disconnect_reason;
    std::optional<std::string> auth_status;
    std::optional<int64_t> reconnect_count; // stored as integer
    std::optional<int64_t> heartbeat_interval; // seconds
    std::string protocol;     // ws, http, internal
    std::string instance_id;
    std::optional<std::string> tenant_id; // foreign key to clients.tenant_id
};

struct Subscription {
    std::string subscription_id;
    std::string symbol;
    std::string topic;
    std::optional<std::string> stream_type;
    std::optional<std::string> mode;
    std::optional<std::string> filters_json; // JSONB as string
    std::optional<int64_t> priority;
    uint64_t created_at;      // milliseconds since epoch
    std::optional<uint64_t> removed_at; // milliseconds since epoch
    bool is_active;
    std::optional<std::string> tenant_id; // foreign key to clients.tenant_id
    std::optional<std::string> session_id; // foreign key to sessions.session_id
};

struct FeedInstance {
    std::string instance_id;
    std::string exchange;     // e.g., BINANCE, JUPITER, BIRDEYE
    std::optional<std::string> adapter_type;
    std::string feed_status;  // e.g., connected, disconnected, reconnecting, stale
    std::optional<uint64_t> last_tick_at; // milliseconds since epoch
    std::optional<int64_t> stale_seconds; // seconds
    int64_t reconnect_attempts;
    std::optional<double> message_rate_in;
    std::optional<double> message_rate_out;
    std::optional<int64_t> queue_depth;
    std::optional<bool> backpressure_active;
    std::optional<double> serialization_ms;
    std::optional<int64_t> parse_error_count;
    std::optional<int64_t> gap_count;
    std::optional<int64_t> duplicate_count;
    std::optional<int64_t> out_of_order_count;
    std::optional<std::string> tenant_id; // foreign key to clients.tenant_id
};

struct FeedMetricsSnapshot {
    int64_t id;               // BIGSERIAL
    std::string instance_id;  // foreign key to feed_instances.instance_id
    uint64_t measured_at;     // milliseconds since epoch

    // Legacy fields
    std::optional<double> p50_latency_ms;
    std::optional<double> p95_latency_ms;
    std::optional<double> p99_latency_ms;
    std::optional<double> avg_latency_ms;
    std::optional<double> drop_rate;
    std::optional<double> packet_loss_rate;
    std::optional<int64_t> msgs_sent;
    std::optional<int64_t> msgs_received;
    std::optional<int64_t> bytes_sent;
    std::optional<int64_t> bytes_received;
    std::optional<double> cpu_usage;      // percentage
    std::optional<int64_t> memory_usage;  // bytes
    std::optional<int64_t> thread_count;
    std::optional<double> event_loop_lag_ms;
    std::optional<int64_t> uptime_seconds;

    // Per-category latency percentiles
    std::optional<double> exchange_p50_ms;
    std::optional<double> exchange_p95_ms;
    std::optional<double> exchange_p99_ms;
    std::optional<double> parsing_p50_ms;
    std::optional<double> parsing_p95_ms;
    std::optional<double> parsing_p99_ms;
    std::optional<double> normalization_p50_ms;
    std::optional<double> normalization_p95_ms;
    std::optional<double> normalization_p99_ms;
    std::optional<double> processing_p50_ms;
    std::optional<double> processing_p95_ms;
    std::optional<double> processing_p99_ms;
    std::optional<double> broadcast_p50_ms;
    std::optional<double> broadcast_p95_ms;
    std::optional<double> broadcast_p99_ms;
    std::optional<double> serialization_p50_ms;
    std::optional<double> serialization_p95_ms;
    std::optional<double> serialization_p99_ms;
    std::optional<double> socket_send_p50_ms;
    std::optional<double> socket_send_p95_ms;
    std::optional<double> socket_send_p99_ms;

    // Full per-category latency details (JSONB as string)
    std::optional<std::string> latency_stats_jsonb;

    // Throughput rates (per second)
    std::optional<double> messages_per_sec;
    std::optional<double> packets_per_sec;
    std::optional<double> bytes_per_sec;
    std::optional<double> ticks_per_sec;
    std::optional<double> trades_per_sec;
    std::optional<double> orderbook_updates_per_sec;
    std::optional<double> broadcasts_per_sec;
    std::optional<double> subscriptions_per_sec;
    std::optional<double> database_writes_per_sec;
    std::optional<double> database_reads_per_sec;

    // Cumulative totals
    std::optional<int64_t> total_messages;
    std::optional<int64_t> total_packets;
    std::optional<int64_t> total_bytes;
    std::optional<int64_t> total_ticks;
    std::optional<int64_t> total_trades;
    std::optional<int64_t> total_orderbook_updates;
    std::optional<int64_t> total_broadcasts;
    std::optional<int64_t> total_subscriptions;
    std::optional<int64_t> total_database_writes;
    std::optional<int64_t> total_database_reads;

    // Queue metrics
    std::optional<int64_t> incoming_queue_depth;
    std::optional<int64_t> outgoing_queue_depth;
    std::optional<int64_t> serialization_queue_depth;
    std::optional<int64_t> max_incoming_queue_depth;
    std::optional<int64_t> max_outgoing_queue_depth;
    std::optional<int64_t> max_serialization_queue_depth;
    std::optional<int64_t> queue_overflow_count;
    std::optional<double> queue_wait_time_ms;
    std::optional<double> queue_processing_time_ms;
    std::optional<bool> queue_backpressure;

    // Feed health
    std::optional<int64_t> packet_drops;
    std::optional<int64_t> duplicate_packets;
    std::optional<int64_t> out_of_order_packets;
    std::optional<int64_t> sequence_gaps;
    std::optional<int64_t> missing_ticks;
    std::optional<int64_t> invalid_messages;
    std::optional<int64_t> corrupted_packets;
    std::optional<int64_t> parse_failures;
    std::optional<bool> stale_feed;
    std::optional<int64_t> feed_health_score;
    std::optional<std::string> feed_health_status;

    // Session metrics
    std::optional<int64_t> active_clients;
    std::optional<int64_t> active_sessions;
    std::optional<int64_t> active_subscriptions;
    std::optional<int64_t> total_connections;
    std::optional<int64_t> total_disconnections;
    std::optional<int64_t> reconnect_count;
    std::optional<int64_t> authentication_failures;
    std::optional<double> avg_session_duration_ms;
    std::optional<double> longest_session_duration_ms;

    // Network metrics
    std::optional<int64_t> tcp_reconnects;
    std::optional<int64_t> socket_errors;
    std::optional<int64_t> read_errors;
    std::optional<int64_t> write_errors;
    std::optional<int64_t> tls_handshake_failures;
    std::optional<int64_t> network_bytes_transmitted;
    std::optional<int64_t> network_bytes_received;
    std::optional<double> socket_rtt_ms;
    std::optional<double> network_bandwidth_bps;
    std::optional<int64_t> network_connection_failures;

    // Database metrics
    std::optional<int64_t> db_successful_writes;
    std::optional<int64_t> db_failed_writes;
    std::optional<double> db_insert_latency_ms;
    std::optional<double> db_query_latency_ms;
    std::optional<int64_t> db_active_connections;
    std::optional<int64_t> db_connection_failures;
    std::optional<int64_t> db_transaction_count;
    std::optional<double> db_writes_per_sec;
    std::optional<double> db_reads_per_sec;
    std::optional<int64_t> db_queue_waiting;

    // Extended system metrics
    std::optional<int64_t> peak_rss;
    std::optional<int64_t> virtual_memory;
    std::optional<int64_t> heap_usage;
    std::optional<double> memory_growth_rate;
};

struct FeedEvent {
    std::string event_id;
    std::optional<std::string> actor_type;
    std::optional<std::string> actor_id;
    std::string action_type;  // subscribe, unsubscribe, switch_exchange, reconnect, replay_start
    std::optional<std::string> target_type;
    std::optional<std::string> target_id;
    std::optional<std::string> result; // success, failure
    std::optional<std::string> error_code;
    std::optional<std::string> trace_id;
    std::optional<std::string> correlation_id;
    uint64_t occurred_at;     // milliseconds since epoch
    std::optional<std::string> metadata; // JSONB as string
};

struct ApiRequest {
    std::string request_id;
    std::string endpoint;
    std::string method;
    std::optional<int64_t> status_code;
    std::optional<double> latency_ms;
    std::optional<int64_t> request_size;
    std::optional<int64_t> response_size;
    std::optional<std::string> client_id; // foreign key to clients.tenant_id
    std::optional<std::string> session_id; // foreign key to sessions.session_id
    std::optional<std::string> instance_id; // foreign key to feed_instances.instance_id
    uint64_t timestamp;       // milliseconds since epoch
};

struct ExchangeHealth {
    int64_t id;               // BIGSERIAL
    std::string exchange_name;
    std::optional<std::string> endpoint;
    std::string status;       // online, offline, degraded
    std::optional<uint64_t> last_success_at; // milliseconds since epoch
    std::optional<uint64_t> last_error_at;  // milliseconds since epoch
    std::optional<int64_t> error_count;
    std::optional<int64_t> rate_limit_hits;
    std::optional<double> latency_ms;
    std::optional<int64_t> symbols_active;
    std::optional<int64_t> feed_lag_ms;
    uint64_t checked_at;      // milliseconds since epoch
};

struct BacktestJob {
    std::string job_id;
    std::optional<std::string> symbol;
    std::optional<std::string> exchange;
    std::optional<uint64_t> start_time; // milliseconds since epoch
    std::optional<uint64_t> end_time;   // milliseconds since epoch
    std::optional<int64_t> replay_speed;
    std::string status;
    std::optional<double> progress;
    std::optional<uint64_t> created_at;   // milliseconds since epoch
    std::optional<uint64_t> completed_at; // milliseconds since epoch
};

struct ConfigVersion {
    int64_t id;               // BIGSERIAL
    std::string config_version;
    std::string build_sha;    // assuming SHA-1 (40 hex chars)
    std::string adapter_version;
    std::string deployment_id;
    std::optional<std::string> feature_flags; // JSONB as string
    int64_t schema_version;
    uint64_t applied_at;      // milliseconds since epoch
};

// ─── New table structs (monitoring extension) ──────────────────────────────────

struct ExchangeMetricsEntry {
    int64_t id;               // BIGSERIAL
    std::string instance_id;
    std::string exchange_name;
    uint64_t snapshot_time;   // milliseconds since epoch
    bool connected;
    std::optional<double> uptime_seconds;
    std::optional<int64_t> reconnect_count;
    std::optional<int64_t> heartbeat_failures;
    std::optional<int64_t> websocket_disconnects;
    std::optional<int64_t> messages_received;
    std::optional<int64_t> messages_dropped;
    std::optional<int64_t> parse_errors;
    std::optional<double> feed_lag_ms;
    std::optional<double> exchange_latency_ms;
    bool stale;
};

struct QueueEntry {
    int64_t id;               // BIGSERIAL
    std::string instance_id;
    uint64_t measured_at;     // milliseconds since epoch
    int64_t incoming_depth;
    int64_t outgoing_depth;
    int64_t serialization_depth;
    std::optional<int64_t> max_incoming_depth;
    std::optional<int64_t> max_outgoing_depth;
    std::optional<int64_t> max_serialization_depth;
    std::optional<int64_t> overflow_count;
    bool backpressure;
    std::optional<double> wait_time_ms;
    std::optional<double> processing_time_ms;
};

struct SystemMetricsEntry {
    int64_t id;               // BIGSERIAL
    std::string instance_id;
    uint64_t measured_at;     // milliseconds since epoch
    std::optional<double> cpu_usage_percent;
    std::optional<int64_t> memory_rss;
    std::optional<int64_t> peak_rss;
    std::optional<int64_t> virtual_memory;
    std::optional<int64_t> heap_usage;
    std::optional<double> memory_growth_rate;
    std::optional<int64_t> thread_count;
    std::optional<double> uptime_seconds;
};

struct NetworkMetricsEntry {
    int64_t id;               // BIGSERIAL
    std::string instance_id;
    uint64_t measured_at;     // milliseconds since epoch
    std::optional<int64_t> tcp_reconnects;
    std::optional<int64_t> socket_errors;
    std::optional<int64_t> read_errors;
    std::optional<int64_t> write_errors;
    std::optional<int64_t> tls_handshake_failures;
    std::optional<int64_t> bytes_transmitted;
    std::optional<int64_t> bytes_received;
    std::optional<double> socket_rtt_ms;
    std::optional<double> bandwidth_bps;
    std::optional<int64_t> connection_failures;
};

struct DatabaseMetricsEntry {
    int64_t id;               // BIGSERIAL
    std::string instance_id;
    uint64_t measured_at;     // milliseconds since epoch
    std::optional<int64_t> successful_writes;
    std::optional<int64_t> failed_writes;
    std::optional<double> insert_latency_ms;
    std::optional<double> query_latency_ms;
    std::optional<int64_t> active_connections;
    std::optional<int64_t> connection_failures;
    std::optional<int64_t> transaction_count;
    std::optional<double> writes_per_sec;
    std::optional<double> reads_per_sec;
    std::optional<int64_t> queue_waiting;
};

struct Alert {
    std::string alert_id;     // UUID
    std::optional<std::string> instance_id;
    std::string severity;     // info, warning, critical
    std::string source;
    std::string metric_name;
    std::optional<double> current_value;
    std::optional<double> threshold;
    std::optional<std::string> message;
    bool acknowledged;
    uint64_t created_at;      // milliseconds since epoch
    std::optional<uint64_t> resolved_at; // milliseconds since epoch
};

struct MetricThreshold {
    int64_t id;               // BIGSERIAL
    std::string instance_id;
    std::string metric_name;
    std::string source;
    std::optional<double> warning_threshold;
    std::optional<double> critical_threshold;
    std::string op;            // gt, lt, gte, lte, eq
    bool enabled;
    std::optional<int64_t> cooldown_seconds;
    uint64_t created_at;      // milliseconds since epoch
    uint64_t updated_at;      // milliseconds since epoch
};

struct WeeklyMetricsSummary {
    int64_t id;
    std::string instance_id;
    uint64_t week_start;       // milliseconds since epoch (start of ISO week)
    int64_t sample_count;

    // Identical fields as FeedMetricsSnapshot
    std::optional<double> p50_latency_ms, p95_latency_ms, p99_latency_ms, avg_latency_ms;
    std::optional<double> drop_rate, packet_loss_rate;
    std::optional<int64_t> msgs_sent, msgs_received, bytes_sent, bytes_received;
    std::optional<double> cpu_usage;
    std::optional<int64_t> memory_usage, thread_count;
    std::optional<double> event_loop_lag_ms;
    std::optional<int64_t> uptime_seconds;

    std::optional<double> exchange_p50_ms, exchange_p95_ms, exchange_p99_ms;
    std::optional<double> parsing_p50_ms, parsing_p95_ms, parsing_p99_ms;
    std::optional<double> normalization_p50_ms, normalization_p95_ms, normalization_p99_ms;
    std::optional<double> processing_p50_ms, processing_p95_ms, processing_p99_ms;
    std::optional<double> broadcast_p50_ms, broadcast_p95_ms, broadcast_p99_ms;
    std::optional<double> serialization_p50_ms, serialization_p95_ms, serialization_p99_ms;
    std::optional<double> socket_send_p50_ms, socket_send_p95_ms, socket_send_p99_ms;
    std::optional<std::string> latency_stats_jsonb;

    std::optional<double> messages_per_sec, packets_per_sec, bytes_per_sec;
    std::optional<double> ticks_per_sec, trades_per_sec, orderbook_updates_per_sec;
    std::optional<double> broadcasts_per_sec, subscriptions_per_sec;
    std::optional<double> database_writes_per_sec, database_reads_per_sec;

    std::optional<int64_t> total_messages, total_packets, total_bytes;
    std::optional<int64_t> total_ticks, total_trades, total_orderbook_updates, total_broadcasts;
    std::optional<int64_t> total_subscriptions, total_database_writes, total_database_reads;

    std::optional<int64_t> incoming_queue_depth, outgoing_queue_depth, serialization_queue_depth;
    std::optional<int64_t> max_incoming_queue_depth, max_outgoing_queue_depth, max_serialization_queue_depth;
    std::optional<int64_t> queue_overflow_count;
    std::optional<double> queue_wait_time_ms, queue_processing_time_ms;
    std::optional<bool> queue_backpressure;

    std::optional<int64_t> packet_drops, duplicate_packets, out_of_order_packets;
    std::optional<int64_t> sequence_gaps, missing_ticks, invalid_messages;
    std::optional<int64_t> corrupted_packets, parse_failures;
    std::optional<bool> stale_feed;
    std::optional<int64_t> feed_health_score;
    std::optional<std::string> feed_health_status;

    std::optional<int64_t> active_clients, active_sessions, active_subscriptions;
    std::optional<int64_t> total_connections, total_disconnections, reconnect_count;
    std::optional<int64_t> authentication_failures;
    std::optional<double> avg_session_duration_ms, longest_session_duration_ms;

    std::optional<int64_t> tcp_reconnects, socket_errors, read_errors, write_errors;
    std::optional<int64_t> tls_handshake_failures, network_bytes_transmitted, network_bytes_received;
    std::optional<double> socket_rtt_ms, network_bandwidth_bps;
    std::optional<int64_t> network_connection_failures;

    std::optional<int64_t> db_successful_writes, db_failed_writes;
    std::optional<double> db_insert_latency_ms, db_query_latency_ms;
    std::optional<int64_t> db_active_connections, db_connection_failures, db_transaction_count;
    std::optional<double> db_writes_per_sec, db_reads_per_sec;
    std::optional<int64_t> db_queue_waiting;

    std::optional<int64_t> peak_rss, virtual_memory, heap_usage;
    std::optional<double> memory_growth_rate;
};

class SAdapter {
public:
    explicit SAdapter(const std::string& connection_string);
    ~SAdapter();

    // Connection management
    bool connect();
    void disconnect();
    bool is_connected() const;

    // Client operations
    std::optional<Client> create_client(const Client& client);
    std::optional<Client> get_client_by_id(const std::string& tenant_id);
    std::vector<Client> get_clients_by_condition(const std::string& where_clause);
    bool update_client(const Client& client);
    bool delete_client(const std::string& tenant_id);

    // Session operations
    std::optional<Session> create_session(const Session& session);
    std::optional<Session> get_session_by_id(const std::string& session_id);
    std::vector<Session> get_sessions_by_condition(const std::string& where_clause);
    bool update_session(const Session& session);
    bool delete_session(const std::string& session_id);

    // Subscription operations
    std::optional<Subscription> create_subscription(const Subscription& subscription);
    std::optional<Subscription> get_subscription_by_id(const std::string& subscription_id);
    std::vector<Subscription> get_subscriptions_by_condition(const std::string& where_clause);
    bool update_subscription(const Subscription& subscription);
    bool delete_subscription(const std::string& subscription_id);

    // FeedInstance operations
    std::optional<FeedInstance> create_feed_instance(const FeedInstance& instance);
    std::optional<FeedInstance> get_feed_instance_by_id(const std::string& instance_id);
    std::vector<FeedInstance> get_feed_instances_by_condition(const std::string& where_clause);
    bool update_feed_instance(const FeedInstance& instance);
    bool delete_feed_instance(const std::string& instance_id);

    // FeedMetricsSnapshot operations
    std::optional<FeedMetricsSnapshot> create_feed_metrics_snapshot(const FeedMetricsSnapshot& snapshot);
    std::optional<FeedMetricsSnapshot> get_feed_metrics_snapshot_by_id(int64_t id);
    std::vector<FeedMetricsSnapshot> get_feed_metrics_snapshots_by_condition(const std::string& where_clause);
    bool update_feed_metrics_snapshot(const FeedMetricsSnapshot& snapshot);
    bool delete_feed_metrics_snapshot(int64_t id);

    // FeedEvent operations
    std::optional<FeedEvent> create_feed_event(const FeedEvent& event);
    std::optional<FeedEvent> get_feed_event_by_id(const std::string& event_id);
    std::vector<FeedEvent> get_feed_events_by_condition(const std::string& where_clause);
    bool update_feed_event(const FeedEvent& event);
    bool delete_feed_event(const std::string& event_id);

    // ApiRequest operations
    std::optional<ApiRequest> create_api_request(const ApiRequest& request);
    std::optional<ApiRequest> get_api_request_by_id(const std::string& request_id);
    std::vector<ApiRequest> get_api_requests_by_condition(const std::string& where_clause);
    bool update_api_request(const ApiRequest& request);
    bool delete_api_request(const std::string& request_id);

    // ExchangeHealth operations
    std::optional<ExchangeHealth> create_exchange_health(const ExchangeHealth& health);
    std::optional<ExchangeHealth> get_exchange_health_by_id(int64_t id);
    std::vector<ExchangeHealth> get_exchange_healths_by_condition(const std::string& where_clause);
    bool update_exchange_health(const ExchangeHealth& health);
    bool delete_exchange_health(int64_t id);

    // BacktestJob operations
    std::optional<BacktestJob> create_backtest_job(const BacktestJob& job);
    std::optional<BacktestJob> get_backtest_job_by_id(const std::string& job_id);
    std::vector<BacktestJob> get_backtest_jobs_by_condition(const std::string& where_clause);
    bool update_backtest_job(const BacktestJob& job);
    bool delete_backtest_job(const std::string& job_id);

    // ConfigVersion operations
    std::optional<ConfigVersion> create_config_version(const ConfigVersion& config);
    std::optional<ConfigVersion> get_config_version_by_id(int64_t id);
    std::vector<ConfigVersion> get_config_versions_by_condition(const std::string& where_clause);
    bool update_config_version(const ConfigVersion& config);
    bool delete_config_version(int64_t id);

    // ─── New table operations (monitoring extension) ──────────────────────────

    // ExchangeMetricsHistory
    std::optional<ExchangeMetricsEntry> create_exchange_metrics_entry(const ExchangeMetricsEntry& entry);
    std::vector<ExchangeMetricsEntry> get_exchange_metrics_by_condition(const std::string& where_clause);
    bool delete_exchange_metrics_entry(int64_t id);

    // QueueHistory
    std::optional<QueueEntry> create_queue_entry(const QueueEntry& entry);
    std::vector<QueueEntry> get_queue_entries_by_condition(const std::string& where_clause);
    bool delete_queue_entry(int64_t id);

    // SystemMetricsHistory
    std::optional<SystemMetricsEntry> create_system_metrics_entry(const SystemMetricsEntry& entry);
    std::vector<SystemMetricsEntry> get_system_metrics_by_condition(const std::string& where_clause);
    bool delete_system_metrics_entry(int64_t id);

    // NetworkMetricsHistory
    std::optional<NetworkMetricsEntry> create_network_metrics_entry(const NetworkMetricsEntry& entry);
    std::vector<NetworkMetricsEntry> get_network_metrics_by_condition(const std::string& where_clause);
    bool delete_network_metrics_entry(int64_t id);

    // DatabaseMetricsHistory
    std::optional<DatabaseMetricsEntry> create_database_metrics_entry(const DatabaseMetricsEntry& entry);
    std::vector<DatabaseMetricsEntry> get_database_metrics_by_condition(const std::string& where_clause);
    bool delete_database_metrics_entry(int64_t id);

    // Alerts
    std::optional<Alert> create_alert(const Alert& alert);
    std::optional<Alert> get_alert_by_id(const std::string& alert_id);
    std::vector<Alert> get_alerts_by_condition(const std::string& where_clause);
    bool update_alert(const Alert& alert);
    bool delete_alert(const std::string& alert_id);

    // MetricThresholds
    std::optional<MetricThreshold> create_metric_threshold(const MetricThreshold& threshold);
    std::optional<MetricThreshold> get_metric_threshold_by_id(int64_t id);
    std::vector<MetricThreshold> get_metric_thresholds_by_condition(const std::string& where_clause);
    bool update_metric_threshold(const MetricThreshold& threshold);
    bool delete_metric_threshold(int64_t id);

    // WeeklyMetricsSummary — 15-minute rollup persistence
    std::optional<WeeklyMetricsSummary> create_weekly_metrics_summary(const WeeklyMetricsSummary& summary);
    std::vector<WeeklyMetricsSummary> get_weekly_summaries_by_condition(const std::string& where_clause);
    bool delete_weekly_metrics_summary(int64_t id);

    // Delete old 15-minute snapshots (retention)
    int64_t delete_old_snapshots(uint64_t before_epoch_ms);

    // Helper functions for converting between application types and database types
    static std::string timestamp_to_string(uint64_t ms);
    static uint64_t string_to_timestamp(const std::string& str);

    static std::string interval_to_string(int64_t seconds);
    static int64_t string_to_interval(const std::string& str);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl_;
};

} // namespace datafeed

#endif // SADAPTER_H
