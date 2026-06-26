#include "feed_health_monitor.h"
#include <algorithm>
#include <cstdint>

FeedHealthMonitor::FeedHealthStats FeedHealthMonitor::getStats() const {
    FeedHealthStats stats{};
    stats.packet_drops = packet_drops_.load(std::memory_order_relaxed);
    stats.duplicate_packets = duplicate_packets_.load(std::memory_order_relaxed);
    stats.out_of_order_packets = out_of_order_packets_.load(std::memory_order_relaxed);
    stats.sequence_gaps = sequence_gaps_.load(std::memory_order_relaxed);
    stats.missing_ticks = missing_ticks_.load(std::memory_order_relaxed);
    stats.invalid_messages = invalid_messages_.load(std::memory_order_relaxed);
    stats.corrupted_packets = corrupted_packets_.load(std::memory_order_relaxed);
    stats.parse_failures = parse_failures_.load(std::memory_order_relaxed);
    stats.stale_feed = stale_feed_.load(std::memory_order_relaxed);
    stats.health_score = calculateHealthScore();
    stats.status = getHealthStatus();
    return stats;
}

void FeedHealthMonitor::onPacketDrop() {
    packet_drops_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::onDuplicatePacket() {
    duplicate_packets_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::onOutOfOrderPacket() {
    out_of_order_packets_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::onSequenceGap() {
    sequence_gaps_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::onMissingTick() {
    missing_ticks_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::onInvalidMessage() {
    invalid_messages_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::onCorruptedPacket() {
    corrupted_packets_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::onParseFailure() {
    parse_failures_.fetch_add(1, std::memory_order_relaxed);
}

void FeedHealthMonitor::markStale(bool stale) {
    stale_feed_.store(stale, std::memory_order_relaxed);
}

uint32_t FeedHealthMonitor::calculateHealthScore() const {
    uint64_t total_issues =
        packet_drops_.load(std::memory_order_relaxed) +
        duplicate_packets_.load(std::memory_order_relaxed) +
        out_of_order_packets_.load(std::memory_order_relaxed) +
        sequence_gaps_.load(std::memory_order_relaxed) +
        missing_ticks_.load(std::memory_order_relaxed) +
        invalid_messages_.load(std::memory_order_relaxed) +
        corrupted_packets_.load(std::memory_order_relaxed) +
        parse_failures_.load(std::memory_order_relaxed);

    if (stale_feed_.load(std::memory_order_relaxed)) {
        return 0;
    }

    uint32_t score = 100;
    if (total_issues > 1000) {
        score = 0;
    } else if (total_issues > 100) {
        score = 25;
    } else if (total_issues > 50) {
        score = 50;
    } else if (total_issues > 10) {
        score = 75;
    }

    return score;
}

FeedHealthMonitor::HealthStatus FeedHealthMonitor::getHealthStatus() const {
    if (stale_feed_.load(std::memory_order_relaxed)) {
        return HealthStatus::CRITICAL;
    }

    uint32_t score = calculateHealthScore();
    if (score >= 75) return HealthStatus::HEALTHY;
    if (score >= 25) return HealthStatus::DEGRADED;
    return HealthStatus::CRITICAL;
}

void FeedHealthMonitor::reset() {
    packet_drops_.store(0, std::memory_order_relaxed);
    duplicate_packets_.store(0, std::memory_order_relaxed);
    out_of_order_packets_.store(0, std::memory_order_relaxed);
    sequence_gaps_.store(0, std::memory_order_relaxed);
    missing_ticks_.store(0, std::memory_order_relaxed);
    invalid_messages_.store(0, std::memory_order_relaxed);
    corrupted_packets_.store(0, std::memory_order_relaxed);
    parse_failures_.store(0, std::memory_order_relaxed);
    stale_feed_.store(false, std::memory_order_relaxed);
}
