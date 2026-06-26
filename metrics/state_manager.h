#ifndef STATE_MANAGER_H
#define STATE_MANAGER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "ring_buffer.h"
#include "metric_aggregator.h"
#include "metrics_collector.h"

class MetricsCollector;

class StateManager {
public:
    using SummaryCallback = std::function<void(const FeedMetricsSnapshot&)>;

    explicit StateManager(MetricsCollector& collector);
    ~StateManager();

    StateManager(const StateManager&) = delete;
    StateManager& operator=(const StateManager&) = delete;

    void start();
    void stop();

    FeedMetricsSnapshot getLiveSnapshot() const;

    static constexpr size_t RingCapacity = 900;

    RingBuffer<FeedMetricsSnapshot, RingCapacity>& ringBuffer() { return ring_; }
    const RingBuffer<FeedMetricsSnapshot, RingCapacity>& ringBuffer() const { return ring_; }

    void setSummaryCallback(SummaryCallback callback);

private:
    void run();

    MetricsCollector& collector_;
    RingBuffer<FeedMetricsSnapshot, RingCapacity> ring_;
    MetricAggregator aggregator_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    mutable std::mutex snapshot_mutex_;
    FeedMetricsSnapshot latest_snapshot_;
    bool has_snapshot_ = false;

    SummaryCallback summary_callback_;
    std::mutex callback_mutex_;

    std::chrono::steady_clock::time_point interval_start_;
    static constexpr auto AggregationInterval = std::chrono::minutes(15);
};

#endif
