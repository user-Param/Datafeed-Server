#include "database_monitor.h"

DatabaseMonitor::DatabaseStats DatabaseMonitor::getStats() const {
    return {
        successful_writes_.load(std::memory_order_relaxed),
        failed_writes_.load(std::memory_order_relaxed),
        insert_latency_ms_.load(std::memory_order_relaxed),
        query_latency_ms_.load(std::memory_order_relaxed),
        active_connections_.load(std::memory_order_relaxed),
        connection_failures_.load(std::memory_order_relaxed),
        transaction_count_.load(std::memory_order_relaxed),
        writes_per_sec_.load(std::memory_order_relaxed),
        reads_per_sec_.load(std::memory_order_relaxed),
        queue_waiting_.load(std::memory_order_relaxed)
    };
}

void DatabaseMonitor::onSuccessfulWrite() {
    successful_writes_.fetch_add(1, std::memory_order_relaxed);
}

void DatabaseMonitor::onFailedWrite() {
    failed_writes_.fetch_add(1, std::memory_order_relaxed);
}

void DatabaseMonitor::recordInsertLatency(double ms) {
    insert_latency_ms_.store(ms, std::memory_order_relaxed);
}

void DatabaseMonitor::recordQueryLatency(double ms) {
    query_latency_ms_.store(ms, std::memory_order_relaxed);
}

void DatabaseMonitor::setActiveConnections(uint64_t count) {
    active_connections_.store(count, std::memory_order_relaxed);
}

void DatabaseMonitor::onConnectionFailure() {
    connection_failures_.fetch_add(1, std::memory_order_relaxed);
}

void DatabaseMonitor::onTransaction() {
    transaction_count_.fetch_add(1, std::memory_order_relaxed);
}

void DatabaseMonitor::setWritesPerSec(double rate) {
    writes_per_sec_.store(rate, std::memory_order_relaxed);
}

void DatabaseMonitor::setReadsPerSec(double rate) {
    reads_per_sec_.store(rate, std::memory_order_relaxed);
}

void DatabaseMonitor::setQueueWaiting(uint64_t count) {
    queue_waiting_.store(count, std::memory_order_relaxed);
}

void DatabaseMonitor::reset() {
    successful_writes_.store(0, std::memory_order_relaxed);
    failed_writes_.store(0, std::memory_order_relaxed);
    insert_latency_ms_.store(0.0, std::memory_order_relaxed);
    query_latency_ms_.store(0.0, std::memory_order_relaxed);
    active_connections_.store(0, std::memory_order_relaxed);
    connection_failures_.store(0, std::memory_order_relaxed);
    transaction_count_.store(0, std::memory_order_relaxed);
    writes_per_sec_.store(0.0, std::memory_order_relaxed);
    reads_per_sec_.store(0.0, std::memory_order_relaxed);
    queue_waiting_.store(0, std::memory_order_relaxed);
}
