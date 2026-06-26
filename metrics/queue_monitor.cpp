#include "queue_monitor.h"
#include <algorithm>

QueueMonitor::QueueStats QueueMonitor::getStats() const {
    return {
        incoming_depth_.load(std::memory_order_relaxed),
        outgoing_depth_.load(std::memory_order_relaxed),
        serialization_depth_.load(std::memory_order_relaxed),
        max_incoming_depth_.load(std::memory_order_relaxed),
        max_outgoing_depth_.load(std::memory_order_relaxed),
        max_serialization_depth_.load(std::memory_order_relaxed),
        overflow_count_.load(std::memory_order_relaxed),
        backpressure_.load(std::memory_order_relaxed),
        queue_wait_time_ms_.load(std::memory_order_relaxed),
        queue_processing_time_ms_.load(std::memory_order_relaxed)
    };
}

void QueueMonitor::incrementIncoming() {
    uint64_t prev = incoming_depth_.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t max = max_incoming_depth_.load(std::memory_order_relaxed);
    while (prev > max) {
        max_incoming_depth_.compare_exchange_weak(max, prev, std::memory_order_relaxed);
    }
}

void QueueMonitor::decrementIncoming() {
    uint64_t val = incoming_depth_.load(std::memory_order_relaxed);
    if (val > 0) {
        incoming_depth_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void QueueMonitor::setIncomingDepth(uint64_t depth) {
    incoming_depth_.store(depth, std::memory_order_relaxed);
    uint64_t max = max_incoming_depth_.load(std::memory_order_relaxed);
    while (depth > max) {
        max_incoming_depth_.compare_exchange_weak(max, depth, std::memory_order_relaxed);
    }
}

void QueueMonitor::incrementOutgoing() {
    uint64_t prev = outgoing_depth_.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t max = max_outgoing_depth_.load(std::memory_order_relaxed);
    while (prev > max) {
        max_outgoing_depth_.compare_exchange_weak(max, prev, std::memory_order_relaxed);
    }
}

void QueueMonitor::decrementOutgoing() {
    uint64_t val = outgoing_depth_.load(std::memory_order_relaxed);
    if (val > 0) {
        outgoing_depth_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void QueueMonitor::setOutgoingDepth(uint64_t depth) {
    outgoing_depth_.store(depth, std::memory_order_relaxed);
    uint64_t max = max_outgoing_depth_.load(std::memory_order_relaxed);
    while (depth > max) {
        max_outgoing_depth_.compare_exchange_weak(max, depth, std::memory_order_relaxed);
    }
}

void QueueMonitor::incrementSerialization() {
    uint64_t prev = serialization_depth_.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t max = max_serialization_depth_.load(std::memory_order_relaxed);
    while (prev > max) {
        max_serialization_depth_.compare_exchange_weak(max, prev, std::memory_order_relaxed);
    }
}

void QueueMonitor::decrementSerialization() {
    uint64_t val = serialization_depth_.load(std::memory_order_relaxed);
    if (val > 0) {
        serialization_depth_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void QueueMonitor::setSerializationDepth(uint64_t depth) {
    serialization_depth_.store(depth, std::memory_order_relaxed);
    uint64_t max = max_serialization_depth_.load(std::memory_order_relaxed);
    while (depth > max) {
        max_serialization_depth_.compare_exchange_weak(max, depth, std::memory_order_relaxed);
    }
}

void QueueMonitor::onOverflow() {
    overflow_count_.fetch_add(1, std::memory_order_relaxed);
}

void QueueMonitor::setBackpressure(bool active) {
    backpressure_.store(active, std::memory_order_relaxed);
}

void QueueMonitor::recordWaitTime(double ms) {
    queue_wait_time_ms_.store(ms, std::memory_order_relaxed);
}

void QueueMonitor::recordProcessingTime(double ms) {
    queue_processing_time_ms_.store(ms, std::memory_order_relaxed);
}

void QueueMonitor::reset() {
    incoming_depth_.store(0, std::memory_order_relaxed);
    outgoing_depth_.store(0, std::memory_order_relaxed);
    serialization_depth_.store(0, std::memory_order_relaxed);
    max_incoming_depth_.store(0, std::memory_order_relaxed);
    max_outgoing_depth_.store(0, std::memory_order_relaxed);
    max_serialization_depth_.store(0, std::memory_order_relaxed);
    overflow_count_.store(0, std::memory_order_relaxed);
    backpressure_.store(false, std::memory_order_relaxed);
    queue_wait_time_ms_.store(0.0, std::memory_order_relaxed);
    queue_processing_time_ms_.store(0.0, std::memory_order_relaxed);
}
