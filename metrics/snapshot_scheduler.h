#ifndef SNAPSHOT_SCHEDULER_H
#define SNAPSHOT_SCHEDULER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

struct FeedMetricsSnapshot;
class MetricsCollector;

class SnapshotScheduler {
public:
    using SnapshotCallback = std::function<void(const FeedMetricsSnapshot&)>;

    explicit SnapshotScheduler(MetricsCollector& collector,
                               std::chrono::milliseconds interval = std::chrono::milliseconds(1000));
    ~SnapshotScheduler();

    SnapshotScheduler(const SnapshotScheduler&) = delete;
    SnapshotScheduler& operator=(const SnapshotScheduler&) = delete;

    void start();
    void stop();
    void pause();
    void resume();

    bool isRunning() const;
    bool isPaused() const;

    void setInterval(std::chrono::milliseconds interval);

    void registerCallback(SnapshotCallback callback);
    void clearCallbacks();

private:
    void run();

    MetricsCollector& collector_;
    std::chrono::milliseconds interval_;
    std::atomic<bool> running_{false};
    std::atomic<bool> paused_{false};
    std::thread worker_;

    mutable std::mutex callbacks_mutex_;
    std::vector<SnapshotCallback> callbacks_;
};

#endif
