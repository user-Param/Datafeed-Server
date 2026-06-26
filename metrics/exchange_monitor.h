#ifndef EXCHANGE_MONITOR_H
#define EXCHANGE_MONITOR_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

class ExchangeMonitor {
public:
    struct ExchangeStats {
        bool connected = false;
        double uptime_seconds = 0.0;
        uint64_t reconnect_count = 0;
        std::chrono::steady_clock::time_point last_heartbeat;
        std::chrono::steady_clock::time_point last_message;
        double feed_lag_ms = 0.0;
        double exchange_latency_ms = 0.0;
        uint64_t messages_received = 0;
        uint64_t messages_dropped = 0;
        uint64_t parse_errors = 0;
        uint64_t websocket_disconnects = 0;
        uint64_t heartbeat_failures = 0;
        bool stale = false;
        std::chrono::steady_clock::time_point connected_since;
    };

    ExchangeMonitor() = default;
    ~ExchangeMonitor() = default;

    void onConnected(const std::string& exchange);
    void onDisconnected(const std::string& exchange);
    void onHeartbeat(const std::string& exchange);
    void onHeartbeatFailure(const std::string& exchange);
    void onMessageReceived(const std::string& exchange);
    void onMessageDropped(const std::string& exchange);
    void onParseError(const std::string& exchange);
    void onWebSocketDisconnect(const std::string& exchange);
    void onReconnect(const std::string& exchange);
    void updateFeedLag(const std::string& exchange, double lag_ms);
    void updateExchangeLatency(const std::string& exchange, double latency_ms);

    ExchangeStats getStats(const std::string& exchange) const;
    std::unordered_map<std::string, ExchangeStats> getAllStats() const;

    void checkStale(const std::string& exchange, std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    void checkAllStale(std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    void reset(const std::string& exchange);
    void resetAll();

private:
    struct ExchangeData {
        std::atomic<bool> connected{false};
        std::atomic<uint64_t> reconnect_count{0};
        std::atomic<uint64_t> messages_received{0};
        std::atomic<uint64_t> messages_dropped{0};
        std::atomic<uint64_t> parse_errors{0};
        std::atomic<uint64_t> websocket_disconnects{0};
        std::atomic<uint64_t> heartbeat_failures{0};
        std::atomic<double> feed_lag_ms{0.0};
        std::atomic<double> exchange_latency_ms{0.0};
        std::chrono::steady_clock::time_point last_heartbeat;
        std::chrono::steady_clock::time_point last_message;
        std::chrono::steady_clock::time_point connected_since;
        std::chrono::steady_clock::time_point disconnected_since;
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, ExchangeData> exchanges_;
};

#endif
