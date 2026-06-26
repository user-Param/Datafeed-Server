#ifndef METRICS_COLLECTOR_H
#define METRICS_COLLECTOR_H

#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "latency_tracker.h"
#include "throughput_tracker.h"
#include "system_monitor.h"
#include "exchange_monitor.h"
#include "queue_monitor.h"
#include "feed_health_monitor.h"
#include "session_monitor.h"
#include "network_monitor.h"
#include "database_monitor.h"

struct FeedMetricsSnapshot {
    // Latency metrics (milliseconds) - per category
    struct LatencyCategoryStats {
        double average;
        double min;
        double max;
        double p50;
        double p95;
        double p99;
        uint64_t count;
    };
    std::unordered_map<LatencyTracker::LatencyCategory, LatencyCategoryStats> latency_stats;

    // Throughput metrics (per second)
    double messages_received_per_sec;
    double messages_sent_per_sec;
    double bytes_received_per_sec;
    double bytes_sent_per_sec;
    double packets_received_per_sec;
    double packets_sent_per_sec;

    // Cumulative totals
    uint64_t total_messages_received;
    uint64_t total_messages_sent;
    uint64_t total_bytes_received;
    uint64_t total_bytes_sent;
    uint64_t total_packets_received;
    uint64_t total_packets_sent;

    // Specialized throughput rates
    double ticks_per_sec;
    double trades_per_sec;
    double orderbook_updates_per_sec;
    double subscriptions_per_sec;
    double broadcasts_per_sec;
    double database_writes_per_sec;
    double database_reads_per_sec;
    uint64_t total_ticks;
    uint64_t total_trades;
    uint64_t total_orderbook_updates;
    uint64_t total_subscriptions;
    uint64_t total_broadcasts;
    uint64_t total_database_writes;
    uint64_t total_database_reads;

    // System metrics
    double cpu_usage_percent;
    uint64_t memory_rss;
    uint64_t peak_rss;
    uint64_t virtual_memory;
    uint64_t heap_usage;
    double memory_growth_rate;
    uint32_t thread_count;
    double uptime_seconds;

    // Exchange metrics
    std::unordered_map<std::string, ExchangeMonitor::ExchangeStats> exchange_stats;

    // Queue metrics
    uint64_t incoming_queue_depth;
    uint64_t outgoing_queue_depth;
    uint64_t serialization_queue_depth;
    uint64_t max_incoming_queue_depth;
    uint64_t max_outgoing_queue_depth;
    uint64_t max_serialization_queue_depth;
    uint64_t queue_overflow_count;
    bool queue_backpressure;
    double queue_wait_time_ms;
    double queue_processing_time_ms;

    // Feed health metrics
    uint64_t packet_drops;
    uint64_t duplicate_packets;
    uint64_t out_of_order_packets;
    uint64_t sequence_gaps;
    uint64_t missing_ticks;
    uint64_t invalid_messages;
    uint64_t corrupted_packets;
    uint64_t parse_failures;
    bool stale_feed;
    uint32_t feed_health_score;
    FeedHealthMonitor::HealthStatus feed_health_status;

    // Session metrics
    uint64_t active_clients;
    uint64_t active_sessions;
    uint64_t active_subscriptions;
    uint64_t total_connections;
    uint64_t total_disconnections;
    uint64_t authentication_failures;
    uint64_t reconnect_count;
    double average_session_duration_ms;
    double longest_session_duration_ms;

    // Network metrics
    uint64_t tcp_reconnects;
    uint64_t socket_errors;
    uint64_t read_errors;
    uint64_t write_errors;
    uint64_t tls_handshake_failures;
    uint64_t bytes_transmitted;
    uint64_t network_bytes_received;
    double socket_rtt_ms;
    double network_bandwidth_bps;
    uint64_t connection_failures;

    // Database metrics
    uint64_t successful_writes;
    uint64_t failed_writes;
    double insert_latency_ms;
    double query_latency_ms;
    uint64_t active_db_connections;
    uint64_t db_connection_failures;
    uint64_t transaction_count;
    double writes_per_sec;
    double reads_per_sec;
    uint64_t db_queue_waiting;
};

class MetricsCollector {
public:
    MetricsCollector();
    ~MetricsCollector() = default;

    // --- Existing callbacks (unchanged) ---
    void onMessageReceived(size_t bytes = 0);
    void onMessageSent(size_t bytes = 0);

    std::chrono::steady_clock::time_point startLatencyMeasurement();
    void endLatencyMeasurement(std::chrono::steady_clock::time_point start_time);
    std::chrono::steady_clock::time_point startLatencyMeasurement(LatencyTracker::LatencyCategory category);
    void endLatencyMeasurement(std::chrono::steady_clock::time_point start_time, LatencyTracker::LatencyCategory category);

    void onClientConnected();
    void onClientDisconnected();
    void onSessionCreated();
    void onSessionRemoved();
    void onSubscriptionCreated();
    void onSubscriptionRemoved();
    void onPacketDropped();
    void onDuplicatePacket();
    void onOutOfOrderPacket();
    void onParseError();
    void onReconnect();

    // --- ExchangeMonitor forwarding ---
    void onExchangeConnected(const std::string& exchange);
    void onExchangeDisconnected(const std::string& exchange);
    void onExchangeHeartbeat(const std::string& exchange);
    void onExchangeHeartbeatFailure(const std::string& exchange);
    void onExchangeMessageReceived(const std::string& exchange);
    void onExchangeMessageDropped(const std::string& exchange);
    void onExchangeParseError(const std::string& exchange);
    void onExchangeWebSocketDisconnect(const std::string& exchange);
    void onExchangeReconnect(const std::string& exchange);
    void updateExchangeFeedLag(const std::string& exchange, double lag_ms);
    void updateExchangeLatency(const std::string& exchange, double latency_ms);

    // --- QueueMonitor forwarding ---
    void incrementIncomingQueue();
    void decrementIncomingQueue();
    void setIncomingQueueDepth(uint64_t depth);
    void incrementOutgoingQueue();
    void decrementOutgoingQueue();
    void setOutgoingQueueDepth(uint64_t depth);
    void incrementSerializationQueue();
    void decrementSerializationQueue();
    void setSerializationQueueDepth(uint64_t depth);
    void onQueueOverflow();
    void setQueueBackpressure(bool active);
    void recordQueueWaitTime(double ms);
    void recordQueueProcessingTime(double ms);

    // --- FeedHealthMonitor forwarding ---
    void onFeedPacketDrop();
    void onFeedDuplicatePacket();
    void onFeedOutOfOrderPacket();
    void onFeedSequenceGap();
    void onFeedMissingTick();
    void onFeedInvalidMessage();
    void onFeedCorruptedPacket();
    void onFeedParseFailure();
    void markFeedStale(bool stale);

    // --- SessionMonitor forwarding ---
    void onSessionClientConnected(const std::string& client_id = "");
    void onSessionClientDisconnected(const std::string& client_id = "");
    void onSessionAuthenticationFailure(const std::string& client_id = "");
    void onSessionReconnect(const std::string& client_id = "");

    // --- NetworkMonitor forwarding ---
    void onNetworkTcpReconnect();
    void onNetworkSocketError();
    void onNetworkReadError();
    void onNetworkWriteError();
    void onNetworkTlsHandshakeFailure();
    void onNetworkBytesTransmitted(uint64_t bytes);
    void onNetworkBytesReceived(uint64_t bytes);
    void updateNetworkSocketRtt(double rtt_ms);
    void updateNetworkBandwidth(double bps);
    void onNetworkConnectionFailure();

    // --- DatabaseMonitor forwarding ---
    void onDatabaseSuccessfulWrite();
    void onDatabaseFailedWrite();
    void recordDatabaseInsertLatency(double ms);
    void recordDatabaseQueryLatency(double ms);
    void setDatabaseActiveConnections(uint64_t count);
    void onDatabaseConnectionFailure();
    void onDatabaseTransaction();
    void setDatabaseWritesPerSec(double rate);
    void setDatabaseReadsPerSec(double rate);
    void setDatabaseQueueWaiting(uint64_t count);

    // --- ThroughputTracker specialized forwarding ---
    void onTick();
    void onTrade();
    void onOrderbookUpdate();
    void onSubscription();
    void onBroadcast();
    void onDatabaseWrite();
    void onDatabaseRead();

    // --- SystemMonitor ---
    void updateCpuUsage();

    // Get a snapshot of all metrics
    FeedMetricsSnapshot getSnapshot();

    // Access individual monitors for direct use
    ExchangeMonitor& exchangeMonitor();
    QueueMonitor& queueMonitor();
    FeedHealthMonitor& feedHealthMonitor();
    SessionMonitor& sessionMonitor();
    NetworkMonitor& networkMonitor();
    DatabaseMonitor& databaseMonitor();
    LatencyTracker& latencyTracker();
    ThroughputTracker& throughputTracker();
    SystemMonitor& systemMonitor();

    // Reset all counters
    void reset();

private:
    LatencyTracker latency_tracker_;
    ThroughputTracker throughput_tracker_;
    SystemMonitor system_monitor_;
    ExchangeMonitor exchange_monitor_;
    QueueMonitor queue_monitor_;
    FeedHealthMonitor feed_health_monitor_;
    SessionMonitor session_monitor_;
    NetworkMonitor network_monitor_;
    DatabaseMonitor database_monitor_;

    // Legacy atomic counters (redirected to new monitors for backward compat)
    std::atomic<uint64_t> parse_errors_{0};
    std::atomic<uint64_t> reconnect_count_{0};
};

#endif // METRICS_COLLECTOR_H
