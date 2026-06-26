#ifndef LATENCY_TRACKER_H
#define LATENCY_TRACKER_H

#include <chrono>
#include <cstdint>
#include <vector>
#include <mutex>
#include <algorithm>
#include <unordered_map>

class LatencyTracker {
public:
    using clock = std::chrono::steady_clock;
    using duration = clock::duration;
    using time_point = clock::time_point;

    enum class LatencyCategory {
        EXCHANGE,
        PARSING,
        NORMALIZATION,
        PROCESSING,
        BROADCAST,
        SERIALIZATION,
        SOCKET_SEND,
        GENERAL
    };

    LatencyTracker(size_t window_size = 1000);
    ~LatencyTracker() = default;

    // Start a latency measurement and return the start time.
    time_point startLatencyMeasurement();

    // End a latency measurement, given the start time, and record the latency.
    void endLatencyMeasurement(time_point start_time);

    // Start a latency measurement for a specific category.
    time_point startLatencyMeasurement(LatencyCategory category);

    // End a latency measurement for a specific category.
    void endLatencyMeasurement(time_point start_time, LatencyCategory category);

    // Get the current latency statistics for a specific category.
    struct LatencyStats {
        double average;   // in milliseconds
        double min;       // in milliseconds
        double max;       // in milliseconds
        double p50;       // in milliseconds
        double p95;       // in milliseconds
        double p99;       // in milliseconds
        uint64_t count;   // number of samples in the window
    };

    LatencyStats getLatencyStats(LatencyCategory category = LatencyCategory::GENERAL) const;

    // Get all latency statistics as a map.
    std::unordered_map<LatencyCategory, LatencyStats> getAllLatencyStats() const;

    // Clear the latency samples for a specific category or all categories.
    void clear(LatencyCategory category = LatencyCategory::GENERAL);
    void clearAll();

private:
    // Unlocked helper — caller must hold mutex_
    LatencyStats getLatencyStatsUnlocked(LatencyCategory category) const;

    struct LatencyWindow {
        std::vector<double> latencies;
        size_t head;
        uint64_t total_count;
        double sum_latency;

        LatencyWindow() = default;
        LatencyWindow(size_t window_size)
            : latencies(window_size, 0.0)
            , head(0)
            , total_count(0)
            , sum_latency(0.0)
        {}
    };

    mutable std::mutex mutex_;
    size_t window_size_;
    std::unordered_map<LatencyCategory, LatencyWindow> latency_windows_;
};

#endif // LATENCY_TRACKER_H