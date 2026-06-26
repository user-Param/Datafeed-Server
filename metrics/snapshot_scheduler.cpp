#include "snapshot_scheduler.h"
#include "metrics_collector.h"
#include <chrono>

SnapshotScheduler::SnapshotScheduler(MetricsCollector& collector,
                                     std::chrono::milliseconds interval)
    : collector_(collector)
    , interval_(interval)
{
}

SnapshotScheduler::~SnapshotScheduler() {
    stop();
}

void SnapshotScheduler::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
        return;
    }
    paused_.store(false, std::memory_order_relaxed);
    worker_ = std::thread(&SnapshotScheduler::run, this);
}

void SnapshotScheduler::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
        return;
    }
    if (worker_.joinable()) {
        worker_.join();
    }
}

void SnapshotScheduler::pause() {
    paused_.store(true, std::memory_order_relaxed);
}

void SnapshotScheduler::resume() {
    paused_.store(false, std::memory_order_relaxed);
}

bool SnapshotScheduler::isRunning() const {
    return running_.load(std::memory_order_relaxed);
}

bool SnapshotScheduler::isPaused() const {
    return paused_.load(std::memory_order_relaxed);
}

void SnapshotScheduler::setInterval(std::chrono::milliseconds interval) {
    interval_ = interval;
}

void SnapshotScheduler::registerCallback(SnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    callbacks_.push_back(std::move(callback));
}

void SnapshotScheduler::clearCallbacks() {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    callbacks_.clear();
}

void SnapshotScheduler::run() {
    while (running_.load(std::memory_order_relaxed)) {
        auto tick_start = std::chrono::steady_clock::now();

        if (!paused_.load(std::memory_order_relaxed)) {
            auto snapshot = collector_.getSnapshot();
            std::lock_guard<std::mutex> lock(callbacks_mutex_);
            for (const auto& cb : callbacks_) {
                if (cb) {
                    cb(snapshot);
                }
            }
        }

        auto elapsed = std::chrono::steady_clock::now() - tick_start;
        auto sleep_duration = interval_ - std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
        if (sleep_duration > std::chrono::milliseconds::zero()) {
            std::this_thread::sleep_for(sleep_duration);
        }
    }
}
