#ifndef SADAPTER_H
#define SADAPTER_H

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <memory>

// Forward declaration to avoid including pqxx headers in the public interface
namespace pqxx {
    class connection;
    class work;
    class result;
}

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
    std::optional<std::string> dataset_id;
    std::optional<std::string> date_range;
    std::optional<std::string> symbols; // comma-separated or JSON array?
    std::optional<std::string> mode;
    std::string status;
    std::optional<double> progress;
    std::optional<uint64_t> started_at; // milliseconds since epoch
    std::optional<uint64_t> finished_at; // milliseconds since epoch
    std::optional<std::string> result_summary;
    std::optional<std::string> result_artifact_url;
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

private:
    std::string connection_string_;
    std::unique_ptr<pqxx::connection> conn_;

    // Helper functions for converting between application types and database types
    static std::string timestamp_to_string(uint64_t ms);
    static uint64_t string_to_timestamp(const std::string& str);

    static std::string interval_to_string(int64_t seconds);
    static int64_t string_to_interval(const std::string& str);
};

} // namespace datafeed

#endif // SADAPTER_H