#include "session_monitor.h"
#include <algorithm>

SessionMonitor::SessionStats SessionMonitor::getStats() const {
    SessionStats stats{};
    stats.active_clients = active_clients_.load(std::memory_order_relaxed);
    stats.active_sessions = active_sessions_.load(std::memory_order_relaxed);
    stats.active_subscriptions = active_subscriptions_.load(std::memory_order_relaxed);
    stats.total_connections = total_connections_.load(std::memory_order_relaxed);
    stats.total_disconnections = total_disconnections_.load(std::memory_order_relaxed);
    stats.authentication_failures = authentication_failures_.load(std::memory_order_relaxed);
    stats.reconnect_count = reconnect_count_.load(std::memory_order_relaxed);
    stats.average_session_duration_ms = average_session_duration_ms_.load(std::memory_order_relaxed);
    stats.longest_session_duration_ms = longest_session_duration_ms_.load(std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(clients_mutex_);
    uint64_t total_client_connections = 0;
    for (const auto& pair : connections_per_client_) {
        total_client_connections += pair.second;
    }
    uint64_t num_clients = connections_per_client_.size();
    stats.connections_per_client = num_clients > 0 ? total_client_connections / num_clients : 0;

    return stats;
}

std::unordered_map<std::string, uint64_t> SessionMonitor::getConnectionsPerClient() const {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    return connections_per_client_;
}

void SessionMonitor::onClientConnected(const std::string& client_id) {
    active_clients_.fetch_add(1, std::memory_order_relaxed);
    total_connections_.fetch_add(1, std::memory_order_relaxed);

    if (!client_id.empty()) {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        connections_per_client_[client_id]++;
    }
}

void SessionMonitor::onClientDisconnected(const std::string& client_id) {
    uint64_t current = active_clients_.load(std::memory_order_relaxed);
    if (current > 0) {
        active_clients_.fetch_sub(1, std::memory_order_relaxed);
    }
    total_disconnections_.fetch_add(1, std::memory_order_relaxed);
}

void SessionMonitor::onSessionCreated() {
    active_sessions_.fetch_add(1, std::memory_order_relaxed);
}

void SessionMonitor::onSessionRemoved() {
    uint64_t current = active_sessions_.load(std::memory_order_relaxed);
    if (current > 0) {
        active_sessions_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void SessionMonitor::onSubscriptionCreated() {
    active_subscriptions_.fetch_add(1, std::memory_order_relaxed);
}

void SessionMonitor::onSubscriptionRemoved() {
    uint64_t current = active_subscriptions_.load(std::memory_order_relaxed);
    if (current > 0) {
        active_subscriptions_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void SessionMonitor::onAuthenticationFailure(const std::string& client_id) {
    authentication_failures_.fetch_add(1, std::memory_order_relaxed);
}

void SessionMonitor::onReconnect(const std::string& client_id) {
    reconnect_count_.fetch_add(1, std::memory_order_relaxed);
}

void SessionMonitor::reset() {
    active_clients_.store(0, std::memory_order_relaxed);
    active_sessions_.store(0, std::memory_order_relaxed);
    active_subscriptions_.store(0, std::memory_order_relaxed);
    total_connections_.store(0, std::memory_order_relaxed);
    total_disconnections_.store(0, std::memory_order_relaxed);
    authentication_failures_.store(0, std::memory_order_relaxed);
    reconnect_count_.store(0, std::memory_order_relaxed);
    average_session_duration_ms_.store(0.0, std::memory_order_relaxed);
    longest_session_duration_ms_.store(0.0, std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(clients_mutex_);
    connections_per_client_.clear();
}
