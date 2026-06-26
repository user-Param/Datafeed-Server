#include "snapshot_converter.h"
#include "metrics_collector.h"
#include "../sadapter.h"
#include <nlohmann/json.hpp>
#include <string>

using json = nlohmann::json;

namespace snapshot_converter {

static std::string health_status_to_string(FeedHealthMonitor::HealthStatus status) {
    switch (status) {
        case FeedHealthMonitor::HealthStatus::HEALTHY:  return "healthy";
        case FeedHealthMonitor::HealthStatus::DEGRADED: return "degraded";
        case FeedHealthMonitor::HealthStatus::CRITICAL: return "critical";
    }
    return "unknown";
}

datafeed::FeedMetricsSnapshot to_db_snapshot(
    const ::FeedMetricsSnapshot& in,
    const std::string& instance_id,
    uint64_t measured_at)
{
    datafeed::FeedMetricsSnapshot out;
    out.id = 0;
    out.instance_id = instance_id;
    out.measured_at = measured_at;

    auto set_latency = [&](LatencyTracker::LatencyCategory cat,
                           std::optional<double>& p50,
                           std::optional<double>& p95,
                           std::optional<double>& p99)
    {
        auto it = in.latency_stats.find(cat);
        if (it != in.latency_stats.end()) {
            p50 = it->second.p50;
            p95 = it->second.p95;
            p99 = it->second.p99;
        }
    };

    set_latency(LatencyTracker::LatencyCategory::GENERAL,
                out.p50_latency_ms, out.p95_latency_ms, out.p99_latency_ms);

    auto lit = in.latency_stats.find(LatencyTracker::LatencyCategory::GENERAL);
    if (lit != in.latency_stats.end()) {
        out.avg_latency_ms = lit->second.average;
    }

    set_latency(LatencyTracker::LatencyCategory::EXCHANGE,
                out.exchange_p50_ms, out.exchange_p95_ms, out.exchange_p99_ms);
    set_latency(LatencyTracker::LatencyCategory::PARSING,
                out.parsing_p50_ms, out.parsing_p95_ms, out.parsing_p99_ms);
    set_latency(LatencyTracker::LatencyCategory::NORMALIZATION,
                out.normalization_p50_ms, out.normalization_p95_ms, out.normalization_p99_ms);
    set_latency(LatencyTracker::LatencyCategory::PROCESSING,
                out.processing_p50_ms, out.processing_p95_ms, out.processing_p99_ms);
    set_latency(LatencyTracker::LatencyCategory::BROADCAST,
                out.broadcast_p50_ms, out.broadcast_p95_ms, out.broadcast_p99_ms);
    set_latency(LatencyTracker::LatencyCategory::SERIALIZATION,
                out.serialization_p50_ms, out.serialization_p95_ms, out.serialization_p99_ms);
    set_latency(LatencyTracker::LatencyCategory::SOCKET_SEND,
                out.socket_send_p50_ms, out.socket_send_p95_ms, out.socket_send_p99_ms);

    json latency_json = json::object();
    for (const auto& [cat, stats] : in.latency_stats) {
        std::string cat_name;
        switch (cat) {
            case LatencyTracker::LatencyCategory::EXCHANGE:      cat_name = "exchange"; break;
            case LatencyTracker::LatencyCategory::PARSING:       cat_name = "parsing"; break;
            case LatencyTracker::LatencyCategory::NORMALIZATION: cat_name = "normalization"; break;
            case LatencyTracker::LatencyCategory::PROCESSING:    cat_name = "processing"; break;
            case LatencyTracker::LatencyCategory::BROADCAST:     cat_name = "broadcast"; break;
            case LatencyTracker::LatencyCategory::SERIALIZATION: cat_name = "serialization"; break;
            case LatencyTracker::LatencyCategory::SOCKET_SEND:   cat_name = "socket_send"; break;
            case LatencyTracker::LatencyCategory::GENERAL:       cat_name = "general"; break;
        }
        json j;
        j["average"] = stats.average;
        j["min"] = stats.min;
        j["max"] = stats.max;
        j["p50"] = stats.p50;
        j["p95"] = stats.p95;
        j["p99"] = stats.p99;
        j["count"] = stats.count;
        latency_json[cat_name] = std::move(j);
    }
    out.latency_stats_jsonb = latency_json.dump();

    out.messages_per_sec = in.messages_received_per_sec + in.messages_sent_per_sec;
    out.packets_per_sec = in.packets_received_per_sec + in.packets_sent_per_sec;
    out.bytes_per_sec = in.bytes_received_per_sec + in.bytes_sent_per_sec;
    out.ticks_per_sec = in.ticks_per_sec;
    out.trades_per_sec = in.trades_per_sec;
    out.orderbook_updates_per_sec = in.orderbook_updates_per_sec;
    out.broadcasts_per_sec = in.broadcasts_per_sec;
    out.subscriptions_per_sec = in.subscriptions_per_sec;
    out.database_writes_per_sec = in.database_writes_per_sec;
    out.database_reads_per_sec = in.database_reads_per_sec;

    out.total_messages = static_cast<int64_t>(in.total_messages_received + in.total_messages_sent);
    out.total_packets = static_cast<int64_t>(in.total_packets_received + in.total_packets_sent);
    out.total_bytes = static_cast<int64_t>(in.total_bytes_received + in.total_bytes_sent);
    out.total_ticks = static_cast<int64_t>(in.total_ticks);
    out.total_trades = static_cast<int64_t>(in.total_trades);
    out.total_orderbook_updates = static_cast<int64_t>(in.total_orderbook_updates);
    out.total_broadcasts = static_cast<int64_t>(in.total_broadcasts);
    out.total_subscriptions = static_cast<int64_t>(in.total_subscriptions);
    out.total_database_writes = static_cast<int64_t>(in.total_database_writes);
    out.total_database_reads = static_cast<int64_t>(in.total_database_reads);

    out.msgs_sent = static_cast<int64_t>(in.total_messages_sent);
    out.msgs_received = static_cast<int64_t>(in.total_messages_received);
    out.bytes_sent = static_cast<int64_t>(in.total_bytes_sent);
    out.bytes_received = static_cast<int64_t>(in.total_bytes_received);

    out.cpu_usage = in.cpu_usage_percent;
    out.memory_usage = static_cast<int64_t>(in.memory_rss);
    out.thread_count = in.thread_count;
    out.uptime_seconds = static_cast<int64_t>(in.uptime_seconds);
    out.peak_rss = static_cast<int64_t>(in.peak_rss);
    out.virtual_memory = static_cast<int64_t>(in.virtual_memory);
    out.heap_usage = static_cast<int64_t>(in.heap_usage);
    out.memory_growth_rate = in.memory_growth_rate;

    out.incoming_queue_depth = static_cast<int64_t>(in.incoming_queue_depth);
    out.outgoing_queue_depth = static_cast<int64_t>(in.outgoing_queue_depth);
    out.serialization_queue_depth = static_cast<int64_t>(in.serialization_queue_depth);
    out.max_incoming_queue_depth = static_cast<int64_t>(in.max_incoming_queue_depth);
    out.max_outgoing_queue_depth = static_cast<int64_t>(in.max_outgoing_queue_depth);
    out.max_serialization_queue_depth = static_cast<int64_t>(in.max_serialization_queue_depth);
    out.queue_overflow_count = static_cast<int64_t>(in.queue_overflow_count);
    out.queue_wait_time_ms = in.queue_wait_time_ms;
    out.queue_processing_time_ms = in.queue_processing_time_ms;
    out.queue_backpressure = in.queue_backpressure;

    out.packet_drops = static_cast<int64_t>(in.packet_drops);
    out.duplicate_packets = static_cast<int64_t>(in.duplicate_packets);
    out.out_of_order_packets = static_cast<int64_t>(in.out_of_order_packets);
    out.sequence_gaps = static_cast<int64_t>(in.sequence_gaps);
    out.missing_ticks = static_cast<int64_t>(in.missing_ticks);
    out.invalid_messages = static_cast<int64_t>(in.invalid_messages);
    out.corrupted_packets = static_cast<int64_t>(in.corrupted_packets);
    out.parse_failures = static_cast<int64_t>(in.parse_failures);
    out.stale_feed = in.stale_feed;
    out.feed_health_score = static_cast<int64_t>(in.feed_health_score);
    out.feed_health_status = health_status_to_string(in.feed_health_status);

    out.active_clients = static_cast<int64_t>(in.active_clients);
    out.active_sessions = static_cast<int64_t>(in.active_sessions);
    out.active_subscriptions = static_cast<int64_t>(in.active_subscriptions);
    out.total_connections = static_cast<int64_t>(in.total_connections);
    out.total_disconnections = static_cast<int64_t>(in.total_disconnections);
    out.reconnect_count = static_cast<int64_t>(in.reconnect_count);
    out.authentication_failures = static_cast<int64_t>(in.authentication_failures);
    out.avg_session_duration_ms = in.average_session_duration_ms;
    out.longest_session_duration_ms = in.longest_session_duration_ms;

    out.tcp_reconnects = static_cast<int64_t>(in.tcp_reconnects);
    out.socket_errors = static_cast<int64_t>(in.socket_errors);
    out.read_errors = static_cast<int64_t>(in.read_errors);
    out.write_errors = static_cast<int64_t>(in.write_errors);
    out.tls_handshake_failures = static_cast<int64_t>(in.tls_handshake_failures);
    out.network_bytes_transmitted = static_cast<int64_t>(in.bytes_transmitted);
    out.network_bytes_received = static_cast<int64_t>(in.network_bytes_received);
    out.socket_rtt_ms = in.socket_rtt_ms;
    out.network_bandwidth_bps = in.network_bandwidth_bps;
    out.network_connection_failures = static_cast<int64_t>(in.connection_failures);

    out.db_successful_writes = static_cast<int64_t>(in.successful_writes);
    out.db_failed_writes = static_cast<int64_t>(in.failed_writes);
    out.db_insert_latency_ms = in.insert_latency_ms;
    out.db_query_latency_ms = in.query_latency_ms;
    out.db_active_connections = static_cast<int64_t>(in.active_db_connections);
    out.db_connection_failures = static_cast<int64_t>(in.db_connection_failures);
    out.db_transaction_count = static_cast<int64_t>(in.transaction_count);
    out.db_writes_per_sec = in.writes_per_sec;
    out.db_reads_per_sec = in.reads_per_sec;
    out.db_queue_waiting = static_cast<int64_t>(in.db_queue_waiting);

    return out;
}

std::vector<datafeed::ExchangeMetricsEntry> to_exchange_entries(
    const ::FeedMetricsSnapshot& in,
    const std::string& instance_id,
    uint64_t measured_at)
{
    std::vector<datafeed::ExchangeMetricsEntry> entries;
    for (const auto& [name, stats] : in.exchange_stats) {
        datafeed::ExchangeMetricsEntry e;
        e.id = 0;
        e.instance_id = instance_id;
        e.exchange_name = name;
        e.snapshot_time = measured_at;
        e.connected = stats.connected;
        e.uptime_seconds = stats.uptime_seconds;
        e.reconnect_count = static_cast<int64_t>(stats.reconnect_count);
        e.heartbeat_failures = static_cast<int64_t>(stats.heartbeat_failures);
        e.websocket_disconnects = static_cast<int64_t>(stats.websocket_disconnects);
        e.messages_received = static_cast<int64_t>(stats.messages_received);
        e.messages_dropped = static_cast<int64_t>(stats.messages_dropped);
        e.parse_errors = static_cast<int64_t>(stats.parse_errors);
        e.feed_lag_ms = stats.feed_lag_ms;
        e.exchange_latency_ms = stats.exchange_latency_ms;
        e.stale = stats.stale;
        entries.push_back(std::move(e));
    }
    return entries;
}

datafeed::QueueEntry to_queue_entry(
    const ::FeedMetricsSnapshot& in,
    const std::string& instance_id,
    uint64_t measured_at)
{
    datafeed::QueueEntry e;
    e.id = 0;
    e.instance_id = instance_id;
    e.measured_at = measured_at;
    e.incoming_depth = static_cast<int64_t>(in.incoming_queue_depth);
    e.outgoing_depth = static_cast<int64_t>(in.outgoing_queue_depth);
    e.serialization_depth = static_cast<int64_t>(in.serialization_queue_depth);
    e.max_incoming_depth = static_cast<int64_t>(in.max_incoming_queue_depth);
    e.max_outgoing_depth = static_cast<int64_t>(in.max_outgoing_queue_depth);
    e.max_serialization_depth = static_cast<int64_t>(in.max_serialization_queue_depth);
    e.overflow_count = static_cast<int64_t>(in.queue_overflow_count);
    e.backpressure = in.queue_backpressure;
    e.wait_time_ms = in.queue_wait_time_ms;
    e.processing_time_ms = in.queue_processing_time_ms;
    return e;
}

datafeed::SystemMetricsEntry to_system_entry(
    const ::FeedMetricsSnapshot& in,
    const std::string& instance_id,
    uint64_t measured_at)
{
    datafeed::SystemMetricsEntry e;
    e.id = 0;
    e.instance_id = instance_id;
    e.measured_at = measured_at;
    e.cpu_usage_percent = in.cpu_usage_percent;
    e.memory_rss = static_cast<int64_t>(in.memory_rss);
    e.peak_rss = static_cast<int64_t>(in.peak_rss);
    e.virtual_memory = static_cast<int64_t>(in.virtual_memory);
    e.heap_usage = static_cast<int64_t>(in.heap_usage);
    e.memory_growth_rate = in.memory_growth_rate;
    e.thread_count = in.thread_count;
    e.uptime_seconds = in.uptime_seconds;
    return e;
}

datafeed::NetworkMetricsEntry to_network_entry(
    const ::FeedMetricsSnapshot& in,
    const std::string& instance_id,
    uint64_t measured_at)
{
    datafeed::NetworkMetricsEntry e;
    e.id = 0;
    e.instance_id = instance_id;
    e.measured_at = measured_at;
    e.tcp_reconnects = static_cast<int64_t>(in.tcp_reconnects);
    e.socket_errors = static_cast<int64_t>(in.socket_errors);
    e.read_errors = static_cast<int64_t>(in.read_errors);
    e.write_errors = static_cast<int64_t>(in.write_errors);
    e.tls_handshake_failures = static_cast<int64_t>(in.tls_handshake_failures);
    e.bytes_transmitted = static_cast<int64_t>(in.bytes_transmitted);
    e.bytes_received = static_cast<int64_t>(in.network_bytes_received);
    e.socket_rtt_ms = in.socket_rtt_ms;
    e.bandwidth_bps = in.network_bandwidth_bps;
    e.connection_failures = static_cast<int64_t>(in.connection_failures);
    return e;
}

datafeed::DatabaseMetricsEntry to_database_entry(
    const ::FeedMetricsSnapshot& in,
    const std::string& instance_id,
    uint64_t measured_at)
{
    datafeed::DatabaseMetricsEntry e;
    e.id = 0;
    e.instance_id = instance_id;
    e.measured_at = measured_at;
    e.successful_writes = static_cast<int64_t>(in.successful_writes);
    e.failed_writes = static_cast<int64_t>(in.failed_writes);
    e.insert_latency_ms = in.insert_latency_ms;
    e.query_latency_ms = in.query_latency_ms;
    e.active_connections = static_cast<int64_t>(in.active_db_connections);
    e.connection_failures = static_cast<int64_t>(in.db_connection_failures);
    e.transaction_count = static_cast<int64_t>(in.transaction_count);
    e.writes_per_sec = in.writes_per_sec;
    e.reads_per_sec = in.reads_per_sec;
    e.queue_waiting = static_cast<int64_t>(in.db_queue_waiting);
    return e;
}

} // namespace snapshot_converter
