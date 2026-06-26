#include "weekly_rollup.h"
#include "snapshot_converter.h"
#include "metrics_collector.h"
#include <iostream>
#include <cmath>

WeeklyRollup::WeeklyRollup(std::shared_ptr<datafeed::SAdapter> adapter,
                           std::string instance_id,
                           std::chrono::hours check_interval)
    : adapter_(std::move(adapter))
    , instance_id_(std::move(instance_id))
    , check_interval_(check_interval)
{
}

WeeklyRollup::~WeeklyRollup() {
    stop();
}

void WeeklyRollup::start() {
    if (running_.exchange(true, std::memory_order_relaxed)) return;
    worker_ = std::thread(&WeeklyRollup::run, this);
}

void WeeklyRollup::stop() {
    if (!running_.exchange(false, std::memory_order_relaxed)) return;
    if (worker_.joinable()) worker_.join();
}

bool WeeklyRollup::isNewWeek() {
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    struct tm tm;
    gmtime_r(&tt, &tm);

    // Compute start of current ISO week (Monday 00:00:00 UTC)
    int days_since_monday = (tm.tm_wday + 6) % 7; // Monday=0, Sunday=6
    tm.tm_mday -= days_since_monday;
    tm.tm_hour = 0; tm.tm_min = 0; tm.tm_sec = 0;
    time_t week_start_t = timegm(&tm);
    auto week_start = std::chrono::system_clock::from_time_t(week_start_t);

    if (!has_last_week_) {
        last_week_start_ = week_start;
        has_last_week_ = true;
        return false;
    }

    if (week_start > last_week_start_) {
        last_week_start_ = week_start;
        return true;
    }
    return false;
}

void WeeklyRollup::processWeek() {
    if (!adapter_ || !adapter_->is_connected()) return;

    auto week_end = last_week_start_;
    auto week_start = week_end - std::chrono::hours(24 * 7);
    uint64_t start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        week_start.time_since_epoch()).count();
    uint64_t end_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        week_end.time_since_epoch()).count();

    // Query 15-min summaries for this week
    std::string cond = "instance_id = '" + instance_id_ +
        "' AND measured_at >= to_timestamp(" + std::to_string(start_ms / 1000) +
        ") AND measured_at < to_timestamp(" + std::to_string(end_ms / 1000) + ")";
    auto snapshots = adapter_->get_feed_metrics_snapshots_by_condition(cond);

    if (snapshots.empty()) {
        std::cout << "[WeeklyRollup] No snapshots found for week ending "
                  << std::chrono::system_clock::to_time_t(week_end) << std::endl;
        return;
    }

    // Compute weekly average for every field
    auto& first = snapshots.front();
    datafeed::WeeklyMetricsSummary summary;
    summary.instance_id = instance_id_;
    summary.week_start = std::chrono::duration_cast<std::chrono::milliseconds>(
        week_start.time_since_epoch()).count();
    summary.sample_count = static_cast<int64_t>(snapshots.size());

    // Helper: compute average of optional<double> fields
    auto avg_double = [&](std::optional<double> datafeed::FeedMetricsSnapshot::*member) -> std::optional<double> {
        double sum = 0; int count = 0;
        for (const auto& s : snapshots) {
            auto v = s.*member;
            if (v) { sum += *v; ++count; }
        }
        if (count == 0) return std::nullopt;
        return sum / count;
    };

    // Helper: compute average of optional<int64_t> fields
    auto avg_int64 = [&](std::optional<int64_t> datafeed::FeedMetricsSnapshot::*member) -> std::optional<int64_t> {
        int64_t sum = 0; int count = 0;
        for (const auto& s : snapshots) {
            auto v = s.*member;
            if (v) { sum += *v; ++count; }
        }
        if (count == 0) return std::nullopt;
        return sum / count;
    };

    // Helper: get latest non-null value of optional<int64_t> fields
    auto latest_int64 = [&](std::optional<int64_t> datafeed::FeedMetricsSnapshot::*member) -> std::optional<int64_t> {
        std::optional<int64_t> val = std::nullopt;
        for (const auto& s : snapshots) {
            auto v = s.*member;
            if (v) val = v;
        }
        return val;
    };

    // Populate summary from averaged 15-min data
    summary.p50_latency_ms = avg_double(&datafeed::FeedMetricsSnapshot::p50_latency_ms);
    summary.p95_latency_ms = avg_double(&datafeed::FeedMetricsSnapshot::p95_latency_ms);
    summary.p99_latency_ms = avg_double(&datafeed::FeedMetricsSnapshot::p99_latency_ms);
    summary.avg_latency_ms = avg_double(&datafeed::FeedMetricsSnapshot::avg_latency_ms);
    summary.cpu_usage = avg_double(&datafeed::FeedMetricsSnapshot::cpu_usage);
    summary.memory_usage = avg_int64(&datafeed::FeedMetricsSnapshot::memory_usage);
    summary.thread_count = avg_int64(&datafeed::FeedMetricsSnapshot::thread_count);
    summary.uptime_seconds = avg_int64(&datafeed::FeedMetricsSnapshot::uptime_seconds);

    summary.exchange_p50_ms = avg_double(&datafeed::FeedMetricsSnapshot::exchange_p50_ms);
    summary.exchange_p95_ms = avg_double(&datafeed::FeedMetricsSnapshot::exchange_p95_ms);
    summary.exchange_p99_ms = avg_double(&datafeed::FeedMetricsSnapshot::exchange_p99_ms);
    summary.parsing_p50_ms = avg_double(&datafeed::FeedMetricsSnapshot::parsing_p50_ms);
    summary.parsing_p95_ms = avg_double(&datafeed::FeedMetricsSnapshot::parsing_p95_ms);
    summary.parsing_p99_ms = avg_double(&datafeed::FeedMetricsSnapshot::parsing_p99_ms);

    summary.messages_per_sec = avg_double(&datafeed::FeedMetricsSnapshot::messages_per_sec);
    summary.packets_per_sec = avg_double(&datafeed::FeedMetricsSnapshot::packets_per_sec);
    summary.ticks_per_sec = avg_double(&datafeed::FeedMetricsSnapshot::ticks_per_sec);
    summary.trades_per_sec = avg_double(&datafeed::FeedMetricsSnapshot::trades_per_sec);

    summary.total_messages = latest_int64(&datafeed::FeedMetricsSnapshot::total_messages);
    summary.total_packets = latest_int64(&datafeed::FeedMetricsSnapshot::total_packets);
    summary.total_bytes = latest_int64(&datafeed::FeedMetricsSnapshot::total_bytes);
    summary.total_ticks = latest_int64(&datafeed::FeedMetricsSnapshot::total_ticks);

    summary.incoming_queue_depth = avg_int64(&datafeed::FeedMetricsSnapshot::incoming_queue_depth);
    summary.outgoing_queue_depth = avg_int64(&datafeed::FeedMetricsSnapshot::outgoing_queue_depth);

    summary.packet_drops = latest_int64(&datafeed::FeedMetricsSnapshot::packet_drops);
    summary.duplicate_packets = latest_int64(&datafeed::FeedMetricsSnapshot::duplicate_packets);
    summary.out_of_order_packets = latest_int64(&datafeed::FeedMetricsSnapshot::out_of_order_packets);
    summary.parse_failures = latest_int64(&datafeed::FeedMetricsSnapshot::parse_failures);

    summary.active_clients = avg_int64(&datafeed::FeedMetricsSnapshot::active_clients);
    summary.active_sessions = avg_int64(&datafeed::FeedMetricsSnapshot::active_sessions);
    summary.active_subscriptions = avg_int64(&datafeed::FeedMetricsSnapshot::active_subscriptions);

    summary.tcp_reconnects = latest_int64(&datafeed::FeedMetricsSnapshot::tcp_reconnects);
    summary.socket_errors = latest_int64(&datafeed::FeedMetricsSnapshot::socket_errors);
    summary.socket_rtt_ms = avg_double(&datafeed::FeedMetricsSnapshot::socket_rtt_ms);
    summary.network_bandwidth_bps = avg_double(&datafeed::FeedMetricsSnapshot::network_bandwidth_bps);

    summary.db_successful_writes = latest_int64(&datafeed::FeedMetricsSnapshot::db_successful_writes);
    summary.db_failed_writes = latest_int64(&datafeed::FeedMetricsSnapshot::db_failed_writes);
    summary.db_insert_latency_ms = avg_double(&datafeed::FeedMetricsSnapshot::db_insert_latency_ms);
    summary.db_query_latency_ms = avg_double(&datafeed::FeedMetricsSnapshot::db_query_latency_ms);

    summary.peak_rss = avg_int64(&datafeed::FeedMetricsSnapshot::peak_rss);
    summary.virtual_memory = avg_int64(&datafeed::FeedMetricsSnapshot::virtual_memory);
    summary.heap_usage = avg_int64(&datafeed::FeedMetricsSnapshot::heap_usage);
    summary.memory_growth_rate = avg_double(&datafeed::FeedMetricsSnapshot::memory_growth_rate);

    // Persist weekly summary
    auto result = adapter_->create_weekly_metrics_summary(summary);
    if (result) {
        std::cout << "[WeeklyRollup] Created weekly summary (id=" << result->id
                  << ", samples=" << summary.sample_count << ")" << std::endl;

        // Delete 15-min summaries older than retention period
        auto retention_start = std::chrono::system_clock::now()
            - std::chrono::hours(24 * RetentionDays);
        uint64_t before_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            retention_start.time_since_epoch()).count();
        int64_t deleted = adapter_->delete_old_snapshots(before_ms);
        if (deleted > 0) {
            std::cout << "[WeeklyRollup] Retention: deleted " << deleted
                      << " old snapshots" << std::endl;
        }
    } else {
        std::cerr << "[WeeklyRollup] Failed to persist weekly summary" << std::endl;
    }
}

void WeeklyRollup::run() {
    while (running_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(check_interval_);
        if (!running_.load(std::memory_order_relaxed)) break;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (isNewWeek()) {
                processWeek();
            }
        }
    }
}
