#include "MonitorService.hpp"
#include "../../metrics/exchange_monitor.h"
#include "../../metrics/queue_monitor.h"
#include "../../metrics/feed_health_monitor.h"
#include "../../metrics/session_monitor.h"
#include "../../metrics/network_monitor.h"
#include "../../metrics/database_monitor.h"
#include "../../metrics/snapshot_converter.h"
#include <chrono>

namespace api {
namespace services {

MonitorService::MonitorService(
    std::shared_ptr<datafeed::SAdapter> sadapter,
    StateManager& state_manager,
    MetricsCollector& collector)
    : sadapter_(sadapter)
    , state_manager_(state_manager)
    , collector_(collector)
{
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    start_time_ = static_cast<uint64_t>(now);
}

nlohmann::json MonitorService::latencyCategoryToJson(const std::string& name,
    const std::unordered_map<LatencyTracker::LatencyCategory,
    ::FeedMetricsSnapshot::LatencyCategoryStats>& stats,
    LatencyTracker::LatencyCategory cat)
{
    nlohmann::json j;
    auto it = stats.find(cat);
    if (it != stats.end()) {
        j["average"] = it->second.average;
        j["minimum"] = it->second.min;
        j["maximum"] = it->second.max;
        j["p50"] = it->second.p50;
        j["p95"] = it->second.p95;
        j["p99"] = it->second.p99;
        j["sample_count"] = it->second.count;
    } else {
        j = nullptr;
    }
    return j;
}

nlohmann::json MonitorService::snapshotToJson(const ::FeedMetricsSnapshot& s)
{
    nlohmann::json j;

    // System
    nlohmann::json sys;
    sys["cpu_usage"] = s.cpu_usage_percent;
    sys["memory_rss"] = s.memory_rss;
    sys["peak_rss"] = s.peak_rss;
    sys["virtual_memory"] = s.virtual_memory;
    sys["heap_usage"] = s.heap_usage;
    sys["memory_growth"] = s.memory_growth_rate;
    sys["thread_count"] = s.thread_count;
    sys["uptime_seconds"] = s.uptime_seconds;
    j["system"] = sys;

    // Performance / Latency per category
    nlohmann::json perf;
    auto& lat_stats = s.latency_stats;
    perf["exchange"] = latencyCategoryToJson("exchange", lat_stats, LatencyTracker::LatencyCategory::EXCHANGE);
    perf["parsing"] = latencyCategoryToJson("parsing", lat_stats, LatencyTracker::LatencyCategory::PARSING);
    perf["normalization"] = latencyCategoryToJson("normalization", lat_stats, LatencyTracker::LatencyCategory::NORMALIZATION);
    perf["processing"] = latencyCategoryToJson("processing", lat_stats, LatencyTracker::LatencyCategory::PROCESSING);
    perf["serialization"] = latencyCategoryToJson("serialization", lat_stats, LatencyTracker::LatencyCategory::SERIALIZATION);
    perf["broadcast"] = latencyCategoryToJson("broadcast", lat_stats, LatencyTracker::LatencyCategory::BROADCAST);
    perf["socket_send"] = latencyCategoryToJson("socket_send", lat_stats, LatencyTracker::LatencyCategory::SOCKET_SEND);
    j["performance"] = perf;

    // Throughput
    nlohmann::json tp;
    tp["messages_per_sec"] = s.messages_received_per_sec;
    tp["packets_per_sec"] = s.packets_received_per_sec;
    tp["bytes_per_sec"] = s.bytes_received_per_sec;
    tp["trades_per_sec"] = s.trades_per_sec;
    tp["ticks_per_sec"] = s.ticks_per_sec;
    tp["orderbook_updates_per_sec"] = s.orderbook_updates_per_sec;
    tp["subscriptions_per_sec"] = s.subscriptions_per_sec;
    tp["broadcasts_per_sec"] = s.broadcasts_per_sec;
    tp["database_reads_per_sec"] = s.database_reads_per_sec;
    tp["database_writes_per_sec"] = s.database_writes_per_sec;
    nlohmann::json cum;
    cum["total_messages"] = s.total_messages_received + s.total_messages_sent;
    cum["total_packets"] = s.total_packets_received + s.total_packets_sent;
    cum["total_bytes"] = s.total_bytes_received + s.total_bytes_sent;
    cum["total_ticks"] = s.total_ticks;
    cum["total_trades"] = s.total_trades;
    cum["total_orderbook_updates"] = s.total_orderbook_updates;
    cum["total_broadcasts"] = s.total_broadcasts;
    cum["total_subscriptions"] = s.total_subscriptions;
    tp["cumulative"] = cum;
    j["throughput"] = tp;

    // Feed health
    nlohmann::json fh;
    fh["health_score"] = s.feed_health_score;
    fh["status"] = static_cast<int>(s.feed_health_status);
    fh["packet_drops"] = s.packet_drops;
    fh["duplicate_packets"] = s.duplicate_packets;
    fh["out_of_order_packets"] = s.out_of_order_packets;
    fh["sequence_gaps"] = s.sequence_gaps;
    fh["missing_ticks"] = s.missing_ticks;
    fh["invalid_messages"] = s.invalid_messages;
    fh["corrupted_packets"] = s.corrupted_packets;
    fh["parse_failures"] = s.parse_failures;
    fh["stale_feed"] = s.stale_feed;
    j["feed"] = fh;

    // Exchange
    nlohmann::json ex;
    for (const auto& [name, stats] : s.exchange_stats) {
        nlohmann::json e;
        e["connected"] = stats.connected;
        e["uptime_seconds"] = stats.uptime_seconds;
        e["reconnect_count"] = stats.reconnect_count;
        e["feed_lag_ms"] = stats.feed_lag_ms;
        e["exchange_latency_ms"] = stats.exchange_latency_ms;
        e["messages_received"] = stats.messages_received;
        e["messages_dropped"] = stats.messages_dropped;
        e["parse_errors"] = stats.parse_errors;
        e["websocket_disconnects"] = stats.websocket_disconnects;
        e["heartbeat_failures"] = stats.heartbeat_failures;
        e["stale"] = stats.stale;
        ex[name] = e;
    }
    j["exchanges"] = ex;

    // Queues
    nlohmann::json q;
    q["incoming_depth"] = s.incoming_queue_depth;
    q["outgoing_depth"] = s.outgoing_queue_depth;
    q["serialization_depth"] = s.serialization_queue_depth;
    q["max_incoming_depth"] = s.max_incoming_queue_depth;
    q["max_outgoing_depth"] = s.max_outgoing_queue_depth;
    q["max_serialization_depth"] = s.max_serialization_queue_depth;
    q["overflow_count"] = s.queue_overflow_count;
    q["backpressure"] = s.queue_backpressure;
    q["wait_time_ms"] = s.queue_wait_time_ms;
    q["processing_time_ms"] = s.queue_processing_time_ms;
    j["queues"] = q;

    // Network
    nlohmann::json nw;
    nw["tcp_reconnects"] = s.tcp_reconnects;
    nw["socket_errors"] = s.socket_errors;
    nw["read_errors"] = s.read_errors;
    nw["write_errors"] = s.write_errors;
    nw["tls_handshake_failures"] = s.tls_handshake_failures;
    nw["bytes_transmitted"] = s.bytes_transmitted;
    nw["bytes_received"] = s.network_bytes_received;
    nw["socket_rtt_ms"] = s.socket_rtt_ms;
    nw["bandwidth_bps"] = s.network_bandwidth_bps;
    nw["connection_failures"] = s.connection_failures;
    j["network"] = nw;

    // Database
    nlohmann::json db;
    db["active_connections"] = s.active_db_connections;
    db["insert_latency_ms"] = s.insert_latency_ms;
    db["query_latency_ms"] = s.query_latency_ms;
    db["transaction_count"] = s.transaction_count;
    db["successful_writes"] = s.successful_writes;
    db["failed_writes"] = s.failed_writes;
    db["reads_per_sec"] = s.reads_per_sec;
    db["writes_per_sec"] = s.writes_per_sec;
    db["queue_waiting"] = s.db_queue_waiting;
    db["connection_failures"] = s.db_connection_failures;
    j["database"] = db;

    // Sessions
    nlohmann::json ses;
    ses["active_clients"] = s.active_clients;
    ses["active_sessions"] = s.active_sessions;
    ses["active_subscriptions"] = s.active_subscriptions;
    ses["authentication_failures"] = s.authentication_failures;
    ses["reconnect_count"] = s.reconnect_count;
    ses["avg_session_duration_ms"] = s.average_session_duration_ms;
    ses["longest_session_duration_ms"] = s.longest_session_duration_ms;
    ses["total_connections"] = s.total_connections;
    ses["total_disconnections"] = s.total_disconnections;
    j["sessions"] = ses;

    return j;
}

nlohmann::json MonitorService::getDashboard()
{
    auto s = state_manager_.getLiveSnapshot();
    auto j = snapshotToJson(s);

    j["service_uptime"] = s.uptime_seconds;
    j["version"] = "1.0.0";
    j["db_connected"] = sadapter_ && sadapter_->is_connected();
    j["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Alerts summary
    nlohmann::json alerts = nlohmann::json::array();
    if (sadapter_ && sadapter_->is_connected()) {
        auto alert_list = sadapter_->get_alerts_by_condition(
            "acknowledged = false ORDER BY created_at DESC LIMIT 20");
        for (const auto& a : alert_list) {
            nlohmann::json aj;
            aj["alert_id"] = a.alert_id;
            aj["severity"] = a.severity;
            aj["source"] = a.source;
            aj["metric_name"] = a.metric_name;
            aj["message"] = a.message.value_or("");
            aj["created_at"] = a.created_at;
            alerts.push_back(aj);
        }
    }
    j["recent_alerts"] = alerts;

    // Health score
    double health = 100.0;
    if (s.stale_feed) health -= 30.0;
    if (!s.exchange_stats.empty()) {
        for (const auto& [_, es] : s.exchange_stats) {
            if (!es.connected) health -= 20.0;
            if (es.stale) health -= 15.0;
        }
    }
    if (s.queue_overflow_count > 0) health -= 5.0;
    if (s.failed_writes > 0) health -= 5.0;
    health = std::max(0.0, health);
    j["health_score"] = health;

    return j;
}

nlohmann::json MonitorService::getLiveMetrics()
{
    auto s = state_manager_.getLiveSnapshot();
    return snapshotToJson(s);
}

nlohmann::json MonitorService::getHistoryMetrics(const std::string& from, const std::string& to, const std::string& interval)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;

    std::string where;
    if (!from.empty() && !to.empty()) {
        where = "measured_at >= " + from + " AND measured_at <= " + to;
    } else if (!from.empty()) {
        where = "measured_at >= " + from;
    } else if (!to.empty()) {
        where = "measured_at <= " + to;
    }
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto snapshots = sadapter_->get_feed_metrics_snapshots_by_condition(where);
    for (const auto& snap : snapshots) {
        nlohmann::json j;
        j["measured_at"] = snap.measured_at;
        j["cpu_usage"] = snap.cpu_usage;
        j["memory_usage"] = snap.memory_usage;
        j["thread_count"] = snap.thread_count;
        j["uptime_seconds"] = snap.uptime_seconds;
        j["peak_rss"] = snap.peak_rss;
        j["virtual_memory"] = snap.virtual_memory;
        j["heap_usage"] = snap.heap_usage;
        j["memory_growth_rate"] = snap.memory_growth_rate;
        j["p50_latency_ms"] = snap.p50_latency_ms;
        j["p95_latency_ms"] = snap.p95_latency_ms;
        j["p99_latency_ms"] = snap.p99_latency_ms;
        j["avg_latency_ms"] = snap.avg_latency_ms;
        j["messages_per_sec"] = snap.messages_per_sec;
        j["packets_per_sec"] = snap.packets_per_sec;
        j["bytes_per_sec"] = snap.bytes_per_sec;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLivePerformance()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json j;
    auto& lat_stats = s.latency_stats;
    j["exchange"] = latencyCategoryToJson("exchange", lat_stats, LatencyTracker::LatencyCategory::EXCHANGE);
    j["parsing"] = latencyCategoryToJson("parsing", lat_stats, LatencyTracker::LatencyCategory::PARSING);
    j["normalization"] = latencyCategoryToJson("normalization", lat_stats, LatencyTracker::LatencyCategory::NORMALIZATION);
    j["processing"] = latencyCategoryToJson("processing", lat_stats, LatencyTracker::LatencyCategory::PROCESSING);
    j["serialization"] = latencyCategoryToJson("serialization", lat_stats, LatencyTracker::LatencyCategory::SERIALIZATION);
    j["broadcast"] = latencyCategoryToJson("broadcast", lat_stats, LatencyTracker::LatencyCategory::BROADCAST);
    j["socket_send"] = latencyCategoryToJson("socket_send", lat_stats, LatencyTracker::LatencyCategory::SOCKET_SEND);
    return j;
}

nlohmann::json MonitorService::getHistoryPerformance(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto snapshots = sadapter_->get_feed_metrics_snapshots_by_condition(where);
    for (const auto& snap : snapshots) {
        nlohmann::json j;
        j["measured_at"] = snap.measured_at;
        j["p50_latency_ms"] = snap.p50_latency_ms;
        j["p95_latency_ms"] = snap.p95_latency_ms;
        j["p99_latency_ms"] = snap.p99_latency_ms;
        j["avg_latency_ms"] = snap.avg_latency_ms;
        j["exchange_p50_ms"] = snap.exchange_p50_ms;
        j["exchange_p95_ms"] = snap.exchange_p95_ms;
        j["exchange_p99_ms"] = snap.exchange_p99_ms;
        j["parsing_p50_ms"] = snap.parsing_p50_ms;
        j["parsing_p95_ms"] = snap.parsing_p95_ms;
        j["parsing_p99_ms"] = snap.parsing_p99_ms;
        j["normalization_p50_ms"] = snap.normalization_p50_ms;
        j["normalization_p95_ms"] = snap.normalization_p95_ms;
        j["normalization_p99_ms"] = snap.normalization_p99_ms;
        j["processing_p50_ms"] = snap.processing_p50_ms;
        j["processing_p95_ms"] = snap.processing_p95_ms;
        j["processing_p99_ms"] = snap.processing_p99_ms;
        j["serialization_p50_ms"] = snap.serialization_p50_ms;
        j["serialization_p95_ms"] = snap.serialization_p95_ms;
        j["serialization_p99_ms"] = snap.serialization_p99_ms;
        j["socket_send_p50_ms"] = snap.socket_send_p50_ms;
        j["socket_send_p95_ms"] = snap.socket_send_p95_ms;
        j["socket_send_p99_ms"] = snap.socket_send_p99_ms;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLiveThroughput()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json tp;
    tp["messages_per_sec"] = s.messages_received_per_sec;
    tp["packets_per_sec"] = s.packets_received_per_sec;
    tp["bytes_per_sec"] = s.bytes_received_per_sec;
    tp["trades_per_sec"] = s.trades_per_sec;
    tp["ticks_per_sec"] = s.ticks_per_sec;
    tp["orderbook_updates_per_sec"] = s.orderbook_updates_per_sec;
    tp["subscriptions_per_sec"] = s.subscriptions_per_sec;
    tp["broadcasts_per_sec"] = s.broadcasts_per_sec;
    tp["database_reads_per_sec"] = s.database_reads_per_sec;
    tp["database_writes_per_sec"] = s.database_writes_per_sec;
    nlohmann::json cum;
    cum["total_messages"] = s.total_messages_received + s.total_messages_sent;
    cum["total_packets"] = s.total_packets_received + s.total_packets_sent;
    cum["total_bytes"] = s.total_bytes_received + s.total_bytes_sent;
    cum["total_ticks"] = s.total_ticks;
    cum["total_trades"] = s.total_trades;
    cum["total_orderbook_updates"] = s.total_orderbook_updates;
    cum["total_broadcasts"] = s.total_broadcasts;
    cum["total_subscriptions"] = s.total_subscriptions;
    tp["cumulative"] = cum;
    return tp;
}

nlohmann::json MonitorService::getHistoryThroughput(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto snapshots = sadapter_->get_feed_metrics_snapshots_by_condition(where);
    for (const auto& snap : snapshots) {
        nlohmann::json j;
        j["measured_at"] = snap.measured_at;
        j["messages_per_sec"] = snap.messages_per_sec;
        j["packets_per_sec"] = snap.packets_per_sec;
        j["bytes_per_sec"] = snap.bytes_per_sec;
        j["ticks_per_sec"] = snap.ticks_per_sec;
        j["trades_per_sec"] = snap.trades_per_sec;
        j["orderbook_updates_per_sec"] = snap.orderbook_updates_per_sec;
        j["subscriptions_per_sec"] = snap.subscriptions_per_sec;
        j["broadcasts_per_sec"] = snap.broadcasts_per_sec;
        j["database_writes_per_sec"] = snap.database_writes_per_sec;
        j["database_reads_per_sec"] = snap.database_reads_per_sec;
        j["total_messages"] = snap.total_messages;
        j["total_packets"] = snap.total_packets;
        j["total_bytes"] = snap.total_bytes;
        j["total_ticks"] = snap.total_ticks;
        j["total_trades"] = snap.total_trades;
        j["total_orderbook_updates"] = snap.total_orderbook_updates;
        j["total_broadcasts"] = snap.total_broadcasts;
        j["total_subscriptions"] = snap.total_subscriptions;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLiveFeed()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json fh;
    fh["health_score"] = s.feed_health_score;
    fh["status"] = static_cast<int>(s.feed_health_status);
    fh["packet_drops"] = s.packet_drops;
    fh["duplicate_packets"] = s.duplicate_packets;
    fh["out_of_order_packets"] = s.out_of_order_packets;
    fh["sequence_gaps"] = s.sequence_gaps;
    fh["missing_ticks"] = s.missing_ticks;
    fh["invalid_messages"] = s.invalid_messages;
    fh["corrupted_packets"] = s.corrupted_packets;
    fh["parse_failures"] = s.parse_failures;
    fh["stale_feed"] = s.stale_feed;
    return fh;
}

nlohmann::json MonitorService::getHistoryFeed(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto snapshots = sadapter_->get_feed_metrics_snapshots_by_condition(where);
    for (const auto& snap : snapshots) {
        nlohmann::json j;
        j["measured_at"] = snap.measured_at;
        j["packet_drops"] = snap.packet_drops;
        j["duplicate_packets"] = snap.duplicate_packets;
        j["out_of_order_packets"] = snap.out_of_order_packets;
        j["sequence_gaps"] = snap.sequence_gaps;
        j["missing_ticks"] = snap.missing_ticks;
        j["invalid_messages"] = snap.invalid_messages;
        j["corrupted_packets"] = snap.corrupted_packets;
        j["parse_failures"] = snap.parse_failures;
        j["stale_feed"] = snap.stale_feed;
        j["feed_health_score"] = snap.feed_health_score;
        j["feed_health_status"] = snap.feed_health_status;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getExchanges()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& [name, _] : s.exchange_stats) {
        arr.push_back(name);
    }
    return arr;
}

nlohmann::json MonitorService::getExchange(const std::string& exchange)
{
    auto s = state_manager_.getLiveSnapshot();
    auto it = s.exchange_stats.find(exchange);
    if (it == s.exchange_stats.end()) {
        return nullptr;
    }
    const auto& stats = it->second;
    nlohmann::json e;
    e["exchange"] = exchange;
    e["connected"] = stats.connected;
    e["uptime_seconds"] = stats.uptime_seconds;
    e["reconnect_count"] = stats.reconnect_count;
    e["feed_lag_ms"] = stats.feed_lag_ms;
    e["exchange_latency_ms"] = stats.exchange_latency_ms;
    e["messages_received"] = stats.messages_received;
    e["messages_dropped"] = stats.messages_dropped;
    e["parse_errors"] = stats.parse_errors;
    e["websocket_disconnects"] = stats.websocket_disconnects;
    e["heartbeat_failures"] = stats.heartbeat_failures;
    e["stale"] = stats.stale;
    e["health"] = stats.connected ? (stats.stale ? "degraded" : "healthy") : "offline";
    e["status"] = stats.stale ? "stale" : (stats.connected ? "connected" : "disconnected");
    return e;
}

nlohmann::json MonitorService::getExchangeHistory(const std::string& exchange, const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where = "exchange_name = '" + exchange + "'";
    if (!from.empty() && !to.empty()) where += " AND snapshot_time >= " + from + " AND snapshot_time <= " + to;
    else if (!from.empty()) where += " AND snapshot_time >= " + from;
    else if (!to.empty()) where += " AND snapshot_time <= " + to;
    where += " ORDER BY snapshot_time ASC";

    auto entries = sadapter_->get_exchange_metrics_by_condition(where);
    for (const auto& entry : entries) {
        nlohmann::json j;
        j["snapshot_time"] = entry.snapshot_time;
        j["connected"] = entry.connected;
        j["uptime_seconds"] = entry.uptime_seconds;
        j["reconnect_count"] = entry.reconnect_count;
        j["messages_received"] = entry.messages_received;
        j["messages_dropped"] = entry.messages_dropped;
        j["parse_errors"] = entry.parse_errors;
        j["feed_lag_ms"] = entry.feed_lag_ms;
        j["exchange_latency_ms"] = entry.exchange_latency_ms;
        j["stale"] = entry.stale;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLiveQueues()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json q;
    q["incoming_depth"] = s.incoming_queue_depth;
    q["outgoing_depth"] = s.outgoing_queue_depth;
    q["serialization_depth"] = s.serialization_queue_depth;
    q["max_incoming_depth"] = s.max_incoming_queue_depth;
    q["max_outgoing_depth"] = s.max_outgoing_queue_depth;
    q["max_serialization_depth"] = s.max_serialization_queue_depth;
    q["overflow_count"] = s.queue_overflow_count;
    q["backpressure"] = s.queue_backpressure;
    q["wait_time_ms"] = s.queue_wait_time_ms;
    q["processing_time_ms"] = s.queue_processing_time_ms;
    return q;
}

nlohmann::json MonitorService::getHistoryQueues(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto entries = sadapter_->get_queue_entries_by_condition(where);
    for (const auto& entry : entries) {
        nlohmann::json j;
        j["measured_at"] = entry.measured_at;
        j["incoming_depth"] = entry.incoming_depth;
        j["outgoing_depth"] = entry.outgoing_depth;
        j["serialization_depth"] = entry.serialization_depth;
        j["max_incoming_depth"] = entry.max_incoming_depth;
        j["max_outgoing_depth"] = entry.max_outgoing_depth;
        j["max_serialization_depth"] = entry.max_serialization_depth;
        j["overflow_count"] = entry.overflow_count;
        j["backpressure"] = entry.backpressure;
        j["wait_time_ms"] = entry.wait_time_ms;
        j["processing_time_ms"] = entry.processing_time_ms;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLiveNetwork()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json nw;
    nw["tcp_reconnects"] = s.tcp_reconnects;
    nw["socket_errors"] = s.socket_errors;
    nw["read_errors"] = s.read_errors;
    nw["write_errors"] = s.write_errors;
    nw["tls_handshake_failures"] = s.tls_handshake_failures;
    nw["bytes_transmitted"] = s.bytes_transmitted;
    nw["bytes_received"] = s.network_bytes_received;
    nw["socket_rtt_ms"] = s.socket_rtt_ms;
    nw["bandwidth_bps"] = s.network_bandwidth_bps;
    nw["connection_failures"] = s.connection_failures;
    return nw;
}

nlohmann::json MonitorService::getHistoryNetwork(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto entries = sadapter_->get_network_metrics_by_condition(where);
    for (const auto& entry : entries) {
        nlohmann::json j;
        j["measured_at"] = entry.measured_at;
        j["tcp_reconnects"] = entry.tcp_reconnects;
        j["socket_errors"] = entry.socket_errors;
        j["read_errors"] = entry.read_errors;
        j["write_errors"] = entry.write_errors;
        j["tls_handshake_failures"] = entry.tls_handshake_failures;
        j["bytes_transmitted"] = entry.bytes_transmitted;
        j["bytes_received"] = entry.bytes_received;
        j["socket_rtt_ms"] = entry.socket_rtt_ms;
        j["bandwidth_bps"] = entry.bandwidth_bps;
        j["connection_failures"] = entry.connection_failures;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLiveDatabase()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json db;
    db["active_connections"] = s.active_db_connections;
    db["insert_latency_ms"] = s.insert_latency_ms;
    db["query_latency_ms"] = s.query_latency_ms;
    db["transaction_count"] = s.transaction_count;
    db["successful_writes"] = s.successful_writes;
    db["failed_writes"] = s.failed_writes;
    db["reads_per_sec"] = s.reads_per_sec;
    db["writes_per_sec"] = s.writes_per_sec;
    db["queue_waiting"] = s.db_queue_waiting;
    db["connection_failures"] = s.db_connection_failures;
    return db;
}

nlohmann::json MonitorService::getHistoryDatabase(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto entries = sadapter_->get_database_metrics_by_condition(where);
    for (const auto& entry : entries) {
        nlohmann::json j;
        j["measured_at"] = entry.measured_at;
        j["active_connections"] = entry.active_connections;
        j["insert_latency_ms"] = entry.insert_latency_ms;
        j["query_latency_ms"] = entry.query_latency_ms;
        j["transaction_count"] = entry.transaction_count;
        j["successful_writes"] = entry.successful_writes;
        j["failed_writes"] = entry.failed_writes;
        j["reads_per_sec"] = entry.reads_per_sec;
        j["writes_per_sec"] = entry.writes_per_sec;
        j["queue_waiting"] = entry.queue_waiting;
        j["connection_failures"] = entry.connection_failures;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLiveSystem()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json sys;
    sys["cpu_usage"] = s.cpu_usage_percent;
    sys["memory_rss"] = s.memory_rss;
    sys["peak_rss"] = s.peak_rss;
    sys["virtual_memory"] = s.virtual_memory;
    sys["heap_usage"] = s.heap_usage;
    sys["memory_growth"] = s.memory_growth_rate;
    sys["thread_count"] = s.thread_count;
    sys["uptime_seconds"] = s.uptime_seconds;
    return sys;
}

nlohmann::json MonitorService::getHistorySystem(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto entries = sadapter_->get_system_metrics_by_condition(where);
    for (const auto& entry : entries) {
        nlohmann::json j;
        j["measured_at"] = entry.measured_at;
        j["cpu_usage_percent"] = entry.cpu_usage_percent;
        j["memory_rss"] = entry.memory_rss;
        j["peak_rss"] = entry.peak_rss;
        j["virtual_memory"] = entry.virtual_memory;
        j["heap_usage"] = entry.heap_usage;
        j["memory_growth_rate"] = entry.memory_growth_rate;
        j["thread_count"] = entry.thread_count;
        j["uptime_seconds"] = entry.uptime_seconds;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getLiveSessions()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json ses;
    ses["active_clients"] = s.active_clients;
    ses["active_sessions"] = s.active_sessions;
    ses["active_subscriptions"] = s.active_subscriptions;
    ses["authentication_failures"] = s.authentication_failures;
    ses["reconnect_count"] = s.reconnect_count;
    ses["avg_session_duration_ms"] = s.average_session_duration_ms;
    ses["longest_session_duration_ms"] = s.longest_session_duration_ms;
    ses["total_connections"] = s.total_connections;
    ses["total_disconnections"] = s.total_disconnections;
    return ses;
}

nlohmann::json MonitorService::getHistorySessions(const std::string& from, const std::string& to)
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;
    std::string where;
    if (!from.empty() && !to.empty()) where = "measured_at >= " + from + " AND measured_at <= " + to;
    else if (!from.empty()) where = "measured_at >= " + from;
    else if (!to.empty()) where = "measured_at <= " + to;
    if (!where.empty()) where += " ORDER BY measured_at ASC";

    auto snapshots = sadapter_->get_feed_metrics_snapshots_by_condition(where);
    for (const auto& snap : snapshots) {
        nlohmann::json j;
        j["measured_at"] = snap.measured_at;
        j["active_clients"] = snap.active_clients;
        j["active_sessions"] = snap.active_sessions;
        j["active_subscriptions"] = snap.active_subscriptions;
        j["authentication_failures"] = snap.authentication_failures;
        j["reconnect_count"] = snap.reconnect_count;
        j["avg_session_duration_ms"] = snap.avg_session_duration_ms;
        j["longest_session_duration_ms"] = snap.longest_session_duration_ms;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getAnalytics()
{
    auto s = state_manager_.getLiveSnapshot();
    nlohmann::json a;

    // Current latency values
    double avg_lat = 0.0, max_lat = 0.0;
    for (const auto& [cat, stats] : s.latency_stats) {
        avg_lat += stats.average;
        if (stats.max > max_lat) max_lat = stats.max;
    }
    if (!s.latency_stats.empty()) avg_lat /= s.latency_stats.size();
    a["average_latency"] = avg_lat;
    a["worst_latency"] = max_lat;

    a["peak_throughput"] = s.messages_received_per_sec;
    a["average_throughput"] = s.messages_received_per_sec;

    a["peak_cpu"] = s.cpu_usage_percent;
    a["average_cpu"] = s.cpu_usage_percent;
    a["peak_memory"] = static_cast<double>(s.peak_rss);
    a["average_memory"] = static_cast<double>(s.memory_rss);

    // Exchange analytics
    std::string most_active_exchange;
    uint64_t max_msgs = 0;
    double total_exchange_uptime = 0.0;
    for (const auto& [name, es] : s.exchange_stats) {
        total_exchange_uptime += es.uptime_seconds;
        if (es.messages_received > max_msgs) {
            max_msgs = es.messages_received;
            most_active_exchange = name;
        }
    }
    a["most_active_exchange"] = most_active_exchange;
    a["exchange_uptime_seconds"] = total_exchange_uptime;

    // Health score trend (current only)
    a["health_score"] = s.feed_health_score;

    // DB performance
    nlohmann::json dbp;
    dbp["insert_latency_ms"] = s.insert_latency_ms;
    dbp["query_latency_ms"] = s.query_latency_ms;
    dbp["writes_per_sec"] = s.writes_per_sec;
    dbp["reads_per_sec"] = s.reads_per_sec;
    a["database_performance"] = dbp;

    return a;
}

nlohmann::json MonitorService::getTimeline()
{
    nlohmann::json result = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return result;

    auto events = sadapter_->get_feed_events_by_condition(
        "ORDER BY occurred_at DESC LIMIT 100");
    for (const auto& ev : events) {
        nlohmann::json j;
        j["event_id"] = ev.event_id;
        j["actor_type"] = ev.actor_type;
        j["actor_id"] = ev.actor_id;
        j["action_type"] = ev.action_type;
        j["target_type"] = ev.target_type;
        j["target_id"] = ev.target_id;
        j["result"] = ev.result;
        j["error_code"] = ev.error_code;
        j["occurred_at"] = ev.occurred_at;
        result.push_back(j);
    }
    return result;
}

nlohmann::json MonitorService::getDependencies()
{
    nlohmann::json deps;
    deps["database"] = sadapter_ && sadapter_->is_connected() ? "healthy" : "unhealthy";

    auto s = state_manager_.getLiveSnapshot();
    bool any_exchange_connected = false;
    for (const auto& [_, es] : s.exchange_stats) {
        if (es.connected) { any_exchange_connected = true; break; }
    }
    deps["exchange_connections"] = any_exchange_connected ? "healthy" : "degraded";
    deps["websocket_layer"] = "healthy";
    deps["snapshot_scheduler"] = "healthy";
    deps["state_manager"] = "healthy";
    deps["persistence_layer"] = sadapter_ && sadapter_->is_connected() ? "healthy" : "unhealthy";
    deps["monitoring_system"] = "healthy";
    return deps;
}

nlohmann::json MonitorService::getTopology()
{
    nlohmann::json top;
    top["server"] = "datafeed";
    top["router"] = "regex-based";
    top["controllers"] = nlohmann::json::array({"FeedController", "DashboardController", "MonitorController",
        "AlertsController", "ConfigController", "SearchController"});
    top["services"] = nlohmann::json::array({"FeedService", "ClientService", "MonitorService",
        "AlertsService", "ConfigService", "SearchService"});
    top["state_manager"] = "active";
    top["metrics_collector"] = "active";
    top["database"] = (sadapter_ && sadapter_->is_connected()) ? "connected" : "disconnected";
    top["exchanges"] = nlohmann::json::array();
    auto s = state_manager_.getLiveSnapshot();
    for (const auto& [name, es] : s.exchange_stats) {
        nlohmann::json ex;
        ex["name"] = name;
        ex["status"] = es.connected ? "connected" : "disconnected";
        top["exchanges"].push_back(ex);
    }
    return top;
}

} // namespace services
} // namespace api
