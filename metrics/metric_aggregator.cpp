#include "metric_aggregator.h"
#include "metrics_collector.h"
#include <algorithm>
#include <cmath>

void MetricAggregator::addSnapshot(const FeedMetricsSnapshot& s) {
    ++count_;

    // ── Latency ──────────────────────────────────────────────
    for (const auto& [cat, stats] : s.latency_stats) {
        auto& r = latency_running_[cat];
        if (!r.initialized) {
            r.sum_avg = stats.average; r.sum_min = stats.min; r.sum_max = stats.max;
            r.sum_p50 = stats.p50; r.sum_p95 = stats.p95; r.sum_p99 = stats.p99;
            r.sum_count = stats.count;
            r.min_min = stats.min; r.max_max = stats.max;
            r.min_p50 = stats.p50; r.max_p50 = stats.p50;
            r.min_p95 = stats.p95; r.max_p95 = stats.p95;
            r.min_p99 = stats.p99; r.max_p99 = stats.p99;
            r.initialized = true;
        } else {
            r.sum_avg += stats.average; r.sum_min += stats.min; r.sum_max += stats.max;
            r.sum_p50 += stats.p50; r.sum_p95 += stats.p95; r.sum_p99 += stats.p99;
            r.sum_count += stats.count;
            r.min_min = std::min(r.min_min, stats.min);
            r.max_max = std::max(r.max_max, stats.max);
            r.min_p50 = std::min(r.min_p50, stats.p50);
            r.max_p50 = std::max(r.max_p50, stats.p50);
            r.min_p95 = std::min(r.min_p95, stats.p95);
            r.max_p95 = std::max(r.max_p95, stats.p95);
            r.min_p99 = std::min(r.min_p99, stats.p99);
            r.max_p99 = std::max(r.max_p99, stats.p99);
        }
    }

    // ── Throughput rates (per-second values — average) ──────
    auto accum_rate = [&](double val, double& sum, double& mn, double& mx, bool& init) {
        sum += val;
        if (!init) { mn = val; mx = val; init = true; }
        else { mn = std::min(mn, val); mx = std::max(mx, val); }
    };

    accum_rate(s.messages_received_per_sec, sum_messages_recv_, min_messages_recv_, max_messages_recv_, rates_messages_initialized_);
    accum_rate(s.messages_sent_per_sec, sum_messages_sent_, min_messages_sent_, max_messages_sent_, rates_messages_initialized_);
    accum_rate(s.bytes_received_per_sec, sum_bytes_recv_, min_bytes_recv_, max_bytes_recv_, rates_bytes_initialized_);
    accum_rate(s.bytes_sent_per_sec, sum_bytes_sent_, min_bytes_sent_, max_bytes_sent_, rates_bytes_initialized_);
    accum_rate(s.packets_received_per_sec, sum_packets_recv_, min_packets_recv_, max_packets_recv_, rates_packets_initialized_);
    accum_rate(s.packets_sent_per_sec, sum_packets_sent_, min_packets_sent_, max_packets_sent_, rates_packets_initialized_);
    accum_rate(s.ticks_per_sec, sum_ticks_, min_ticks_, max_ticks_, rates_ticks_initialized_);
    accum_rate(s.trades_per_sec, sum_trades_, min_trades_, max_trades_, rates_trades_initialized_);
    accum_rate(s.orderbook_updates_per_sec, sum_ob_updates_, min_ob_updates_, max_ob_updates_, rates_ob_initialized_);
    accum_rate(s.subscriptions_per_sec, sum_subs_, min_subs_, max_subs_, rates_subs_initialized_);
    accum_rate(s.broadcasts_per_sec, sum_broadcasts_, min_broadcasts_, max_broadcasts_, rates_broadcasts_initialized_);
    accum_rate(s.database_writes_per_sec, sum_db_writes_, min_db_writes_, max_db_writes_, rates_db_writes_initialized_);
    accum_rate(s.database_reads_per_sec, sum_db_reads_, min_db_reads_, max_db_reads_, rates_db_reads_initialized_);

    // ── Cumulative totals (latest value) ────────────────────
    last_total_messages_recv_ = s.total_messages_received;
    last_total_messages_sent_ = s.total_messages_sent;
    last_total_bytes_recv_ = s.total_bytes_received;
    last_total_bytes_sent_ = s.total_bytes_sent;
    last_total_packets_recv_ = s.total_packets_received;
    last_total_packets_sent_ = s.total_packets_sent;
    last_total_ticks_ = s.total_ticks;
    last_total_trades_ = s.total_trades;
    last_total_ob_updates_ = s.total_orderbook_updates;
    last_total_subs_ = s.total_subscriptions;
    last_total_broadcasts_ = s.total_broadcasts;
    last_total_db_writes_ = s.total_database_writes;
    last_total_db_reads_ = s.total_database_reads;

    // ── System metrics (gauges) ──────────────────────────────
    if (!system_initialized_) {
        sum_cpu_ = s.cpu_usage_percent;
        min_cpu_ = s.cpu_usage_percent; max_cpu_ = s.cpu_usage_percent;
        sum_mem_rss_ = s.memory_rss;
        min_mem_rss_ = s.memory_rss; max_mem_rss_ = s.memory_rss;
        sum_peak_rss_ = s.peak_rss;
        sum_vm_ = s.virtual_memory;
        sum_heap_ = s.heap_usage;
        sum_mem_growth_ = s.memory_growth_rate;
        sum_threads_ = s.thread_count;
        sum_uptime_ = s.uptime_seconds;
        system_initialized_ = true;
    } else {
        sum_cpu_ += s.cpu_usage_percent;
        min_cpu_ = std::min(min_cpu_, s.cpu_usage_percent);
        max_cpu_ = std::max(max_cpu_, s.cpu_usage_percent);
        sum_mem_rss_ += s.memory_rss;
        min_mem_rss_ = std::min(min_mem_rss_, s.memory_rss);
        max_mem_rss_ = std::max(max_mem_rss_, s.memory_rss);
        sum_peak_rss_ += s.peak_rss;
        sum_vm_ += s.virtual_memory;
        sum_heap_ += s.heap_usage;
        sum_mem_growth_ += s.memory_growth_rate;
        sum_threads_ += s.thread_count;
        sum_uptime_ += s.uptime_seconds;
    }

    // ── Queue metrics (gauges) ──────────────────────────────
    if (!queue_initialized_) {
        sum_in_q_ = s.incoming_queue_depth; sum_out_q_ = s.outgoing_queue_depth;
        sum_ser_q_ = s.serialization_queue_depth;
        sum_max_in_q_ = s.max_incoming_queue_depth;
        sum_max_out_q_ = s.max_outgoing_queue_depth;
        sum_max_ser_q_ = s.max_serialization_queue_depth;
        sum_overflow_ = s.queue_overflow_count;
        sum_wait_ms_ = s.queue_wait_time_ms;
        sum_proc_ms_ = s.queue_processing_time_ms;
        bp_true_count_ = s.queue_backpressure ? 1 : 0;
        queue_initialized_ = true;
    } else {
        sum_in_q_ += s.incoming_queue_depth; sum_out_q_ += s.outgoing_queue_depth;
        sum_ser_q_ += s.serialization_queue_depth;
        sum_max_in_q_ += s.max_incoming_queue_depth;
        sum_max_out_q_ += s.max_outgoing_queue_depth;
        sum_max_ser_q_ += s.max_serialization_queue_depth;
        sum_overflow_ += s.queue_overflow_count;
        sum_wait_ms_ += s.queue_wait_time_ms;
        sum_proc_ms_ += s.queue_processing_time_ms;
        if (s.queue_backpressure) ++bp_true_count_;
    }

    // ── Feed health (cumulative counters, keep latest) ──────
    last_packet_drops_ = s.packet_drops;
    last_dup_ = s.duplicate_packets;
    last_o3_ = s.out_of_order_packets;
    last_gaps_ = s.sequence_gaps;
    last_missing_ = s.missing_ticks;
    last_invalid_ = s.invalid_messages;
    last_corrupted_ = s.corrupted_packets;
    last_parse_fail_ = s.parse_failures;
    if (s.stale_feed) ++stale_true_count_;
    sum_health_score_ += s.feed_health_score;
    switch (s.feed_health_status) {
        case FeedHealthMonitor::HealthStatus::HEALTHY:  ++healthy_count_; break;
        case FeedHealthMonitor::HealthStatus::DEGRADED: ++degraded_count_; break;
        case FeedHealthMonitor::HealthStatus::CRITICAL: ++critical_count_; break;
    }

    // ── Session metrics (gauges + cumulative) ──────────────
    if (!session_initialized_) {
        sum_active_clients_ = s.active_clients;
        sum_active_sessions_ = s.active_sessions;
        sum_active_subs_ = s.active_subscriptions;
        sum_avg_dur_ = s.average_session_duration_ms;
        sum_longest_dur_ = s.longest_session_duration_ms;
        session_initialized_ = true;
    } else {
        sum_active_clients_ += s.active_clients;
        sum_active_sessions_ += s.active_sessions;
        sum_active_subs_ += s.active_subscriptions;
        sum_avg_dur_ += s.average_session_duration_ms;
        sum_longest_dur_ += s.longest_session_duration_ms;
    }
    last_total_conn_ = s.total_connections;
    last_total_disconn_ = s.total_disconnections;
    last_reconn_ = s.reconnect_count;
    last_auth_fail_ = s.authentication_failures;

    // ── Network metrics (cumulative + gauges) ───────────────
    last_tcp_reconn_ = s.tcp_reconnects;
    last_sock_err_ = s.socket_errors;
    last_read_err_ = s.read_errors;
    last_write_err_ = s.write_errors;
    last_tls_fail_ = s.tls_handshake_failures;
    last_bytes_tx_ = s.bytes_transmitted;
    last_bytes_rx_ = s.network_bytes_received;
    last_conn_fail_ = s.connection_failures;

    if (!network_initialized_) {
        sum_rtt_ = s.socket_rtt_ms;
        sum_bw_ = s.network_bandwidth_bps;
        network_initialized_ = true;
    } else {
        sum_rtt_ += s.socket_rtt_ms;
        sum_bw_ += s.network_bandwidth_bps;
    }

    // ── Exchange metrics (per-exchange) ─────────────────────
    for (const auto& [name, es] : s.exchange_stats) {
        auto& r = exchange_running_[name];
        r.sum_uptime += es.uptime_seconds;
        r.sum_reconn += es.reconnect_count;
        r.sum_hb_fail += es.heartbeat_failures;
        r.sum_ws_disc += es.websocket_disconnects;
        r.sum_msgs_recv += es.messages_received;
        r.sum_msgs_drop += es.messages_dropped;
        r.sum_parse_err += es.parse_errors;
        r.sum_feed_lag += es.feed_lag_ms;
        r.sum_ex_lat += es.exchange_latency_ms;
        if (es.connected) ++r.connected_count;
        if (es.stale) ++r.stale_count;
        r.last_connected = es.connected;
        r.last_stale = es.stale;
        ++r.sample_count;
    }

    // ── Database metrics (cumulative + gauges) ──────────────
    last_db_writes_ok_ = s.successful_writes;
    last_db_writes_fail_ = s.failed_writes;
    last_db_conn_fail_ = s.db_connection_failures;
    last_txn_count_ = s.transaction_count;

    if (!database_initialized_) {
        sum_ins_lat_ = s.insert_latency_ms;
        sum_qry_lat_ = s.query_latency_ms;
        sum_db_conn_ = s.active_db_connections;
        sum_db_wps_ = s.writes_per_sec;
        sum_db_rps_ = s.reads_per_sec;
        sum_db_q_wait_ = s.db_queue_waiting;
        database_initialized_ = true;
    } else {
        sum_ins_lat_ += s.insert_latency_ms;
        sum_qry_lat_ += s.query_latency_ms;
        sum_db_conn_ += s.active_db_connections;
        sum_db_wps_ += s.writes_per_sec;
        sum_db_rps_ += s.reads_per_sec;
        sum_db_q_wait_ += s.db_queue_waiting;
    }
}

FeedMetricsSnapshot MetricAggregator::produceSummary() {
    FeedMetricsSnapshot out{};
    if (count_ == 0) return out;
    double n = static_cast<double>(count_);

    // ── Latency ──────────────────────────────────────────────
    for (const auto& [cat, r] : latency_running_) {
        out.latency_stats[cat] = {
            r.sum_avg / n,
            r.min_min,
            r.max_max,
            r.sum_p50 / n,
            r.sum_p95 / n,
            r.sum_p99 / n,
            r.sum_count / count_
        };
    }

    // ── Throughput rates ─────────────────────────────────────
    out.messages_received_per_sec = sum_messages_recv_ / n;
    out.messages_sent_per_sec = sum_messages_sent_ / n;
    out.bytes_received_per_sec = sum_bytes_recv_ / n;
    out.bytes_sent_per_sec = sum_bytes_sent_ / n;
    out.packets_received_per_sec = sum_packets_recv_ / n;
    out.packets_sent_per_sec = sum_packets_sent_ / n;
    out.ticks_per_sec = sum_ticks_ / n;
    out.trades_per_sec = sum_trades_ / n;
    out.orderbook_updates_per_sec = sum_ob_updates_ / n;
    out.subscriptions_per_sec = sum_subs_ / n;
    out.broadcasts_per_sec = sum_broadcasts_ / n;
    out.database_writes_per_sec = sum_db_writes_ / n;
    out.database_reads_per_sec = sum_db_reads_ / n;

    // ── Cumulative totals (latest value) ─────────────────────
    out.total_messages_received = last_total_messages_recv_;
    out.total_messages_sent = last_total_messages_sent_;
    out.total_bytes_received = last_total_bytes_recv_;
    out.total_bytes_sent = last_total_bytes_sent_;
    out.total_packets_received = last_total_packets_recv_;
    out.total_packets_sent = last_total_packets_sent_;
    out.total_ticks = last_total_ticks_;
    out.total_trades = last_total_trades_;
    out.total_orderbook_updates = last_total_ob_updates_;
    out.total_subscriptions = last_total_subs_;
    out.total_broadcasts = last_total_broadcasts_;
    out.total_database_writes = last_total_db_writes_;
    out.total_database_reads = last_total_db_reads_;

    // ── System ───────────────────────────────────────────────
    out.cpu_usage_percent = sum_cpu_ / n;
    out.memory_rss = sum_mem_rss_ / count_;
    out.peak_rss = sum_peak_rss_ / count_;
    out.virtual_memory = sum_vm_ / count_;
    out.heap_usage = sum_heap_ / count_;
    out.memory_growth_rate = sum_mem_growth_ / n;
    out.thread_count = static_cast<uint32_t>(sum_threads_ / count_);
    out.uptime_seconds = sum_uptime_ / n;

    // ── Queue ────────────────────────────────────────────────
    out.incoming_queue_depth = sum_in_q_ / count_;
    out.outgoing_queue_depth = sum_out_q_ / count_;
    out.serialization_queue_depth = sum_ser_q_ / count_;
    out.max_incoming_queue_depth = sum_max_in_q_ / count_;
    out.max_outgoing_queue_depth = sum_max_out_q_ / count_;
    out.max_serialization_queue_depth = sum_max_ser_q_ / count_;
    out.queue_overflow_count = sum_overflow_ / count_;
    out.queue_backpressure = bp_true_count_ > (count_ / 2);
    out.queue_wait_time_ms = sum_wait_ms_ / n;
    out.queue_processing_time_ms = sum_proc_ms_ / n;

    // ── Feed health ──────────────────────────────────────────
    out.packet_drops = last_packet_drops_;
    out.duplicate_packets = last_dup_;
    out.out_of_order_packets = last_o3_;
    out.sequence_gaps = last_gaps_;
    out.missing_ticks = last_missing_;
    out.invalid_messages = last_invalid_;
    out.corrupted_packets = last_corrupted_;
    out.parse_failures = last_parse_fail_;
    out.stale_feed = stale_true_count_ > (count_ / 2);
    out.feed_health_score = static_cast<uint32_t>(sum_health_score_ / count_);
    if (healthy_count_ >= degraded_count_ && healthy_count_ >= critical_count_)
        out.feed_health_status = FeedHealthMonitor::HealthStatus::HEALTHY;
    else if (degraded_count_ >= critical_count_)
        out.feed_health_status = FeedHealthMonitor::HealthStatus::DEGRADED;
    else
        out.feed_health_status = FeedHealthMonitor::HealthStatus::CRITICAL;

    // ── Session ──────────────────────────────────────────────
    out.active_clients = sum_active_clients_ / count_;
    out.active_sessions = sum_active_sessions_ / count_;
    out.active_subscriptions = sum_active_subs_ / count_;
    out.total_connections = last_total_conn_;
    out.total_disconnections = last_total_disconn_;
    out.authentication_failures = last_auth_fail_;
    out.reconnect_count = last_reconn_;
    out.average_session_duration_ms = sum_avg_dur_ / n;
    out.longest_session_duration_ms = sum_longest_dur_ / n;

    // ── Network ──────────────────────────────────────────────
    out.tcp_reconnects = last_tcp_reconn_;
    out.socket_errors = last_sock_err_;
    out.read_errors = last_read_err_;
    out.write_errors = last_write_err_;
    out.tls_handshake_failures = last_tls_fail_;
    out.bytes_transmitted = last_bytes_tx_;
    out.network_bytes_received = last_bytes_rx_;
    out.socket_rtt_ms = sum_rtt_ / n;
    out.network_bandwidth_bps = sum_bw_ / n;
    out.connection_failures = last_conn_fail_;

    // ── Database ─────────────────────────────────────────────
    out.successful_writes = last_db_writes_ok_;
    out.failed_writes = last_db_writes_fail_;
    out.insert_latency_ms = sum_ins_lat_ / n;
    out.query_latency_ms = sum_qry_lat_ / n;
    out.active_db_connections = sum_db_conn_ / count_;
    out.db_connection_failures = last_db_conn_fail_;
    out.transaction_count = last_txn_count_;
    out.writes_per_sec = sum_db_wps_ / n;
    out.reads_per_sec = sum_db_rps_ / n;
    out.db_queue_waiting = sum_db_q_wait_ / count_;

    // ── Exchange ─────────────────────────────────────────────
    for (const auto& [name, r] : exchange_running_) {
        ExchangeMonitor::ExchangeStats es;
        es.connected = r.connected_count > (r.sample_count / 2);
        es.stale = r.stale_count > (r.sample_count / 2);
        if (r.sample_count > 0) {
            es.uptime_seconds = r.sum_uptime / r.sample_count;
            es.feed_lag_ms = r.sum_feed_lag / r.sample_count;
            es.exchange_latency_ms = r.sum_ex_lat / r.sample_count;
        }
        es.reconnect_count = r.sum_reconn / r.sample_count;
        es.heartbeat_failures = r.sum_hb_fail / r.sample_count;
        es.websocket_disconnects = r.sum_ws_disc / r.sample_count;
        es.messages_received = r.sum_msgs_recv / r.sample_count;
        es.messages_dropped = r.sum_msgs_drop / r.sample_count;
        es.parse_errors = r.sum_parse_err / r.sample_count;
        out.exchange_stats[name] = es;
    }

    return out;
}

void MetricAggregator::reset() {
    count_ = 0;
    latency_running_.clear();
    sum_messages_recv_ = sum_messages_sent_ = 0;
    sum_bytes_recv_ = sum_bytes_sent_ = 0;
    sum_packets_recv_ = sum_packets_sent_ = 0;
    sum_ticks_ = sum_trades_ = sum_ob_updates_ = 0;
    sum_subs_ = sum_broadcasts_ = 0;
    sum_db_writes_ = sum_db_reads_ = 0;
    rates_messages_initialized_ = false;
    rates_bytes_initialized_ = false;
    rates_packets_initialized_ = false;
    rates_ticks_initialized_ = false;
    rates_trades_initialized_ = false;
    rates_ob_initialized_ = false;
    rates_subs_initialized_ = false;
    rates_broadcasts_initialized_ = false;
    rates_db_writes_initialized_ = false;
    rates_db_reads_initialized_ = false;
    last_total_messages_recv_ = last_total_messages_sent_ = 0;
    last_total_bytes_recv_ = last_total_bytes_sent_ = 0;
    last_total_packets_recv_ = last_total_packets_sent_ = 0;
    last_total_ticks_ = last_total_trades_ = last_total_ob_updates_ = 0;
    last_total_subs_ = last_total_broadcasts_ = 0;
    last_total_db_writes_ = last_total_db_reads_ = 0;
    sum_cpu_ = 0; min_cpu_ = max_cpu_ = 0;
    sum_mem_rss_ = sum_peak_rss_ = sum_vm_ = sum_heap_ = 0;
    sum_mem_growth_ = sum_threads_ = sum_uptime_ = 0;
    system_initialized_ = false;
    sum_in_q_ = sum_out_q_ = sum_ser_q_ = 0;
    sum_max_in_q_ = sum_max_out_q_ = sum_max_ser_q_ = 0;
    sum_overflow_ = 0; sum_wait_ms_ = sum_proc_ms_ = 0;
    bp_true_count_ = 0; queue_initialized_ = false;
    last_packet_drops_ = last_dup_ = last_o3_ = 0;
    last_gaps_ = last_missing_ = last_invalid_ = 0;
    last_corrupted_ = last_parse_fail_ = 0;
    stale_true_count_ = 0; sum_health_score_ = 0;
    healthy_count_ = degraded_count_ = critical_count_ = 0;
    sum_active_clients_ = sum_active_sessions_ = sum_active_subs_ = 0;
    last_total_conn_ = last_total_disconn_ = 0;
    last_reconn_ = last_auth_fail_ = 0;
    sum_avg_dur_ = sum_longest_dur_ = 0;
    session_initialized_ = false;
    last_tcp_reconn_ = last_sock_err_ = 0;
    last_read_err_ = last_write_err_ = last_tls_fail_ = 0;
    last_bytes_tx_ = last_bytes_rx_ = 0;
    sum_rtt_ = sum_bw_ = 0; last_conn_fail_ = 0;
    network_initialized_ = false;
    last_db_writes_ok_ = last_db_writes_fail_ = 0;
    sum_ins_lat_ = sum_qry_lat_ = 0;
    sum_db_conn_ = 0; last_db_conn_fail_ = last_txn_count_ = 0;
    sum_db_wps_ = sum_db_rps_ = 0; sum_db_q_wait_ = 0;
    database_initialized_ = false;
    exchange_running_.clear();
}
