#include "state_manager.h"
#include "metrics_collector.h"
#include <iostream>

StateManager::StateManager(MetricsCollector& collector)
    : collector_(collector)
{
}

StateManager::~StateManager() {
    stop();
}

void StateManager::start() {
    if (running_.exchange(true, std::memory_order_relaxed)) return;
    interval_start_ = std::chrono::steady_clock::now();
    worker_ = std::thread(&StateManager::run, this);
}

void StateManager::stop() {
    if (!running_.exchange(false, std::memory_order_relaxed)) return;
    if (worker_.joinable()) worker_.join();
}

FeedMetricsSnapshot StateManager::getLiveSnapshot() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    return has_snapshot_ ? latest_snapshot_ : FeedMetricsSnapshot{};
}

void StateManager::setSummaryCallback(SummaryCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    summary_callback_ = std::move(callback);
}

void StateManager::run() {
    auto next_tick = std::chrono::steady_clock::now();

    while (running_.load(std::memory_order_relaxed)) {
        auto now = std::chrono::steady_clock::now();

        // Get snapshot from collector
        auto snapshot = collector_.getSnapshot();

        // Update live snapshot (thread-safe for REST/WS reads)
        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            latest_snapshot_ = snapshot;
            has_snapshot_ = true;
        }

        // Feed ring buffer (for recent history)
        ring_.push(snapshot);

        // Feed aggregator (for 15-min running stats)
        aggregator_.addSnapshot(snapshot);

        // Check if 15-min aggregation interval elapsed
        auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(
            now - interval_start_);
        if (elapsed >= AggregationInterval) {
            auto summary = aggregator_.produceSummary();

            // Fire callback to persist
            {
                std::lock_guard<std::mutex> lock(callback_mutex_);
                if (summary_callback_) {
                    summary_callback_(summary);
                }
            }

            // Reset only the aggregator (not ring buffer, not live snapshot)
            aggregator_.reset();
            interval_start_ = std::chrono::steady_clock::now();
        }

        // Sleep until next 1-second tick
        next_tick += std::chrono::seconds(1);
        auto sleep_until = std::max(next_tick, std::chrono::steady_clock::now());
        std::this_thread::sleep_until(sleep_until);
    }
}
