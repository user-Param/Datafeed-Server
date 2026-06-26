#include "exchange_monitor.h"
#include <algorithm>

void ExchangeMonitor::onConnected(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.connected.store(true, std::memory_order_relaxed);
    data.connected_since = std::chrono::steady_clock::now();
    data.last_heartbeat = std::chrono::steady_clock::now();
    data.last_message = std::chrono::steady_clock::now();
}

void ExchangeMonitor::onDisconnected(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = exchanges_.find(exchange);
    if (it != exchanges_.end()) {
        it->second.connected.store(false, std::memory_order_relaxed);
        it->second.disconnected_since = std::chrono::steady_clock::now();
    }
}

void ExchangeMonitor::onHeartbeat(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.last_heartbeat = std::chrono::steady_clock::now();
}

void ExchangeMonitor::onHeartbeatFailure(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.heartbeat_failures.fetch_add(1, std::memory_order_relaxed);
}

void ExchangeMonitor::onMessageReceived(const std::string& exchange) {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.messages_received.fetch_add(1, std::memory_order_relaxed);
    data.last_message = now;
}

void ExchangeMonitor::onMessageDropped(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.messages_dropped.fetch_add(1, std::memory_order_relaxed);
}

void ExchangeMonitor::onParseError(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.parse_errors.fetch_add(1, std::memory_order_relaxed);
}

void ExchangeMonitor::onWebSocketDisconnect(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.websocket_disconnects.fetch_add(1, std::memory_order_relaxed);
}

void ExchangeMonitor::onReconnect(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.reconnect_count.fetch_add(1, std::memory_order_relaxed);
}

void ExchangeMonitor::updateFeedLag(const std::string& exchange, double lag_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.feed_lag_ms.store(lag_ms, std::memory_order_relaxed);
}

void ExchangeMonitor::updateExchangeLatency(const std::string& exchange, double latency_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = exchanges_[exchange];
    data.exchange_latency_ms.store(latency_ms, std::memory_order_relaxed);
}

ExchangeMonitor::ExchangeStats ExchangeMonitor::getStats(const std::string& exchange) const {
    ExchangeStats stats{};
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = exchanges_.find(exchange);
    if (it == exchanges_.end()) return stats;

    const auto& data = it->second;
    auto now = std::chrono::steady_clock::now();
    stats.connected = data.connected.load(std::memory_order_relaxed);
    stats.reconnect_count = data.reconnect_count.load(std::memory_order_relaxed);
    stats.messages_received = data.messages_received.load(std::memory_order_relaxed);
    stats.messages_dropped = data.messages_dropped.load(std::memory_order_relaxed);
    stats.parse_errors = data.parse_errors.load(std::memory_order_relaxed);
    stats.websocket_disconnects = data.websocket_disconnects.load(std::memory_order_relaxed);
    stats.heartbeat_failures = data.heartbeat_failures.load(std::memory_order_relaxed);
    stats.feed_lag_ms = data.feed_lag_ms.load(std::memory_order_relaxed);
    stats.exchange_latency_ms = data.exchange_latency_ms.load(std::memory_order_relaxed);
    stats.last_heartbeat = data.last_heartbeat;
    stats.last_message = data.last_message;

    if (data.connected.load(std::memory_order_relaxed)) {
        stats.uptime_seconds = std::chrono::duration<double>(now - data.connected_since).count();
        auto since_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(now - data.last_heartbeat).count();
        stats.stale = since_heartbeat > 5000;
    }
    stats.connected_since = data.connected_since;

    return stats;
}

std::unordered_map<std::string, ExchangeMonitor::ExchangeStats> ExchangeMonitor::getAllStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, ExchangeStats> result;
    for (const auto& pair : exchanges_) {
        result[pair.first] = getStats(pair.first);
    }
    return result;
}

void ExchangeMonitor::checkStale(const std::string& exchange, std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = exchanges_.find(exchange);
    if (it != exchanges_.end()) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.last_heartbeat);
        it->second.connected.store(elapsed < timeout, std::memory_order_relaxed);
    }
}

void ExchangeMonitor::checkAllStale(std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    for (auto& pair : exchanges_) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - pair.second.last_heartbeat);
        pair.second.connected.store(elapsed < timeout, std::memory_order_relaxed);
    }
}

void ExchangeMonitor::reset(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = exchanges_.find(exchange);
    if (it != exchanges_.end()) {
        exchanges_.erase(it);
    }
}

void ExchangeMonitor::resetAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    exchanges_.clear();
}
