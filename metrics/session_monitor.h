#ifndef SESSION_MONITOR_H
#define SESSION_MONITOR_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

class SessionMonitor {
public:
    SessionMonitor() = default;
    ~SessionMonitor() = default;

    struct SessionStats {
        uint64_t active_clients = 0;
        uint64_t active_sessions = 0;
        uint64_t active_subscriptions = 0;
        uint64_t total_connections = 0;
        uint64_t total_disconnections = 0;
        uint64_t authentication_failures = 0;
        uint64_t reconnect_count = 0;
        double average_session_duration_ms = 0.0;
        double longest_session_duration_ms = 0.0;
        uint64_t connections_per_client = 0;
    };

    SessionStats getStats() const;
    std::unordered_map<std::string, uint64_t> getConnectionsPerClient() const;

    void onClientConnected(const std::string& client_id = "");
    void onClientDisconnected(const std::string& client_id = "");
    void onSessionCreated();
    void onSessionRemoved();
    void onSubscriptionCreated();
    void onSubscriptionRemoved();
    void onAuthenticationFailure(const std::string& client_id = "");
    void onReconnect(const std::string& client_id = "");

    void reset();

private:
    std::atomic<uint64_t> active_clients_{0};
    std::atomic<uint64_t> active_sessions_{0};
    std::atomic<uint64_t> active_subscriptions_{0};
    std::atomic<uint64_t> total_connections_{0};
    std::atomic<uint64_t> total_disconnections_{0};
    std::atomic<uint64_t> authentication_failures_{0};
    std::atomic<uint64_t> reconnect_count_{0};
    std::atomic<double> average_session_duration_ms_{0.0};
    std::atomic<double> longest_session_duration_ms_{0.0};

    mutable std::mutex clients_mutex_;
    std::unordered_map<std::string, uint64_t> connections_per_client_;
};

#endif
