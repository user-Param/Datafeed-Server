#ifndef WEEKLY_ROLLUP_H
#define WEEKLY_ROLLUP_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include "../sadapter.h"

class WeeklyRollup {
public:
    explicit WeeklyRollup(std::shared_ptr<datafeed::SAdapter> adapter,
                          std::string instance_id = "live-1",
                          std::chrono::hours check_interval = std::chrono::hours(1));
    ~WeeklyRollup();

    WeeklyRollup(const WeeklyRollup&) = delete;
    WeeklyRollup& operator=(const WeeklyRollup&) = delete;

    void start();
    void stop();

private:
    void run();
    void processWeek();
    bool isNewWeek();

    std::shared_ptr<datafeed::SAdapter> adapter_;
    std::string instance_id_;
    std::chrono::hours check_interval_;
    std::atomic<bool> running_{false};
    std::thread worker_;

    mutable std::mutex mutex_;
    std::chrono::system_clock::time_point last_week_start_;
    bool has_last_week_ = false;

    static constexpr int RetentionDays = 90;
};

#endif
