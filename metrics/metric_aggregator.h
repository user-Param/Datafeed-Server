#ifndef METRIC_AGGREGATOR_H
#define METRIC_AGGREGATOR_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "latency_tracker.h"
#include "feed_health_monitor.h"
#include "exchange_monitor.h"

struct FeedMetricsSnapshot;

class MetricAggregator {
public:
    MetricAggregator() = default;
    ~MetricAggregator() = default;

    MetricAggregator(const MetricAggregator&) = delete;
    MetricAggregator& operator=(const MetricAggregator&) = delete;

    void addSnapshot(const FeedMetricsSnapshot& snapshot);

    FeedMetricsSnapshot produceSummary();

    void reset();

    uint64_t sampleCount() const { return count_; }

private:
    uint64_t count_ = 0;

    // Running sum, min, max for each latency category
    struct LatencyRunning {
        double sum_avg = 0, sum_min = 0, sum_max = 0;
        double sum_p50 = 0, sum_p95 = 0, sum_p99 = 0;
        uint64_t sum_count = 0;
        double min_min = 0, max_max = 0;
        double min_p50 = 0, max_p50 = 0;
        double min_p95 = 0, max_p95 = 0;
        double min_p99 = 0, max_p99 = 0;
        bool initialized = false;
    };
    std::unordered_map<LatencyTracker::LatencyCategory, LatencyRunning> latency_running_;

    // Throughput rates (sum for averaging)
    double sum_messages_recv_ = 0, sum_messages_sent_ = 0;
    double sum_bytes_recv_ = 0, sum_bytes_sent_ = 0;
    double sum_packets_recv_ = 0, sum_packets_sent_ = 0;
    double sum_ticks_ = 0, sum_trades_ = 0, sum_ob_updates_ = 0;
    double sum_subs_ = 0, sum_broadcasts_ = 0;
    double sum_db_writes_ = 0, sum_db_reads_ = 0;

    // Independent min/max for every rate metric
    double min_messages_recv_ = 0, max_messages_recv_ = 0;
    double min_messages_sent_ = 0, max_messages_sent_ = 0;
    double min_bytes_recv_ = 0, max_bytes_recv_ = 0;
    double min_bytes_sent_ = 0, max_bytes_sent_ = 0;
    double min_packets_recv_ = 0, max_packets_recv_ = 0;
    double min_packets_sent_ = 0, max_packets_sent_ = 0;
    double min_ticks_ = 0, max_ticks_ = 0;
    double min_trades_ = 0, max_trades_ = 0;
    double min_ob_updates_ = 0, max_ob_updates_ = 0;
    double min_subs_ = 0, max_subs_ = 0;
    double min_broadcasts_ = 0, max_broadcasts_ = 0;
    double min_db_writes_ = 0, max_db_writes_ = 0;
    double min_db_reads_ = 0, max_db_reads_ = 0;

    // Independent initialization flags
    bool rates_messages_initialized_ = false;
    bool rates_bytes_initialized_ = false;
    bool rates_packets_initialized_ = false;
    bool rates_ticks_initialized_ = false;
    bool rates_trades_initialized_ = false;
    bool rates_ob_initialized_ = false;
    bool rates_subs_initialized_ = false;
    bool rates_broadcasts_initialized_ = false;
    bool rates_db_writes_initialized_ = false;
    bool rates_db_reads_initialized_ = false;

    // Cumulative totals (latest value)
    uint64_t last_total_messages_recv_ = 0;
    uint64_t last_total_messages_sent_ = 0;
    uint64_t last_total_bytes_recv_ = 0;
    uint64_t last_total_bytes_sent_ = 0;
    uint64_t last_total_packets_recv_ = 0;
    uint64_t last_total_packets_sent_ = 0;
    uint64_t last_total_ticks_ = 0;
    uint64_t last_total_trades_ = 0;
    uint64_t last_total_ob_updates_ = 0;
    uint64_t last_total_subs_ = 0;
    uint64_t last_total_broadcasts_ = 0;
    uint64_t last_total_db_writes_ = 0;
    uint64_t last_total_db_reads_ = 0;

    // System metrics (gauges)
    double sum_cpu_ = 0;
    double min_cpu_ = 0, max_cpu_ = 0;
    uint64_t sum_mem_rss_ = 0;
    uint64_t min_mem_rss_ = 0, max_mem_rss_ = 0;
    uint64_t sum_peak_rss_ = 0, sum_vm_ = 0, sum_heap_ = 0;
    double sum_mem_growth_ = 0;
    uint64_t sum_threads_ = 0;
    double sum_uptime_ = 0;
    bool system_initialized_ = false;

    // Queue metrics (gauges)
    uint64_t sum_in_q_ = 0, sum_out_q_ = 0, sum_ser_q_ = 0;
    uint64_t sum_max_in_q_ = 0, sum_max_out_q_ = 0, sum_max_ser_q_ = 0;
    uint64_t sum_overflow_ = 0;
    double sum_wait_ms_ = 0, sum_proc_ms_ = 0;
    uint64_t bp_true_count_ = 0;
    bool queue_initialized_ = false;

    // Feed health (cumulative counters)
    uint64_t last_packet_drops_ = 0, last_dup_ = 0, last_o3_ = 0;
    uint64_t last_gaps_ = 0, last_missing_ = 0, last_invalid_ = 0;
    uint64_t last_corrupted_ = 0, last_parse_fail_ = 0;
    int stale_true_count_ = 0;
    uint64_t sum_health_score_ = 0;
    uint64_t healthy_count_ = 0, degraded_count_ = 0, critical_count_ = 0;

    // Session metrics (gauges + cumulative)
    uint64_t sum_active_clients_ = 0, sum_active_sessions_ = 0, sum_active_subs_ = 0;
    uint64_t last_total_conn_ = 0, last_total_disconn_ = 0;
    uint64_t last_reconn_ = 0, last_auth_fail_ = 0;
    double sum_avg_dur_ = 0, sum_longest_dur_ = 0;
    bool session_initialized_ = false;

    // Network metrics (cumulative + gauges)
    uint64_t last_tcp_reconn_ = 0, last_sock_err_ = 0;
    uint64_t last_read_err_ = 0, last_write_err_ = 0;
    uint64_t last_tls_fail_ = 0;
    uint64_t last_bytes_tx_ = 0, last_bytes_rx_ = 0;
    double sum_rtt_ = 0, sum_bw_ = 0;
    uint64_t last_conn_fail_ = 0;
    bool network_initialized_ = false;

    // Database metrics (cumulative + gauges)
    uint64_t last_db_writes_ok_ = 0, last_db_writes_fail_ = 0;
    double sum_ins_lat_ = 0, sum_qry_lat_ = 0;
    uint64_t sum_db_conn_ = 0;
    uint64_t last_db_conn_fail_ = 0, last_txn_count_ = 0;
    double sum_db_wps_ = 0, sum_db_rps_ = 0;
    uint64_t sum_db_q_wait_ = 0;
    bool database_initialized_ = false;

    // Exchange metrics (per-exchange running stats)
    struct ExchangeRunning {
        double sum_uptime = 0;
        uint64_t sum_reconn = 0, sum_hb_fail = 0, sum_ws_disc = 0;
        uint64_t sum_msgs_recv = 0, sum_msgs_drop = 0, sum_parse_err = 0;
        double sum_feed_lag = 0, sum_ex_lat = 0;
        uint64_t connected_count = 0, stale_count = 0;
        uint64_t sample_count = 0;
        bool last_connected = false;
        bool last_stale = false;
    };
    std::unordered_map<std::string, ExchangeRunning> exchange_running_;
};

#endif
