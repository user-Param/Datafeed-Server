#ifndef FEED_HEALTH_MONITOR_H
#define FEED_HEALTH_MONITOR_H

#include <atomic>
#include <chrono>
#include <cstdint>

class FeedHealthMonitor {
public:
    enum class HealthStatus {
        HEALTHY,
        DEGRADED,
        CRITICAL
    };

    FeedHealthMonitor() = default;
    ~FeedHealthMonitor() = default;

    struct FeedHealthStats {
        uint64_t packet_drops = 0;
        uint64_t duplicate_packets = 0;
        uint64_t out_of_order_packets = 0;
        uint64_t sequence_gaps = 0;
        uint64_t missing_ticks = 0;
        uint64_t invalid_messages = 0;
        uint64_t corrupted_packets = 0;
        uint64_t parse_failures = 0;
        bool stale_feed = false;
        uint32_t health_score = 100;
        HealthStatus status = HealthStatus::HEALTHY;
    };

    FeedHealthStats getStats() const;

    void onPacketDrop();
    void onDuplicatePacket();
    void onOutOfOrderPacket();
    void onSequenceGap();
    void onMissingTick();
    void onInvalidMessage();
    void onCorruptedPacket();
    void onParseFailure();

    void markStale(bool stale);
    uint32_t calculateHealthScore() const;
    HealthStatus getHealthStatus() const;

    void reset();

private:
    std::atomic<uint64_t> packet_drops_{0};
    std::atomic<uint64_t> duplicate_packets_{0};
    std::atomic<uint64_t> out_of_order_packets_{0};
    std::atomic<uint64_t> sequence_gaps_{0};
    std::atomic<uint64_t> missing_ticks_{0};
    std::atomic<uint64_t> invalid_messages_{0};
    std::atomic<uint64_t> corrupted_packets_{0};
    std::atomic<uint64_t> parse_failures_{0};
    std::atomic<bool> stale_feed_{false};
};

#endif
