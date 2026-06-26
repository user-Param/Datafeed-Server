#ifndef DATABASE_MONITOR_H
#define DATABASE_MONITOR_H

#include <atomic>
#include <chrono>
#include <cstdint>

class DatabaseMonitor {
public:
    DatabaseMonitor() = default;
    ~DatabaseMonitor() = default;

    struct DatabaseStats {
        uint64_t successful_writes = 0;
        uint64_t failed_writes = 0;
        double insert_latency_ms = 0.0;
        double query_latency_ms = 0.0;
        uint64_t active_connections = 0;
        uint64_t connection_failures = 0;
        uint64_t transaction_count = 0;
        double writes_per_sec = 0.0;
        double reads_per_sec = 0.0;
        uint64_t queue_waiting = 0;
    };

    DatabaseStats getStats() const;

    void onSuccessfulWrite();
    void onFailedWrite();
    void recordInsertLatency(double ms);
    void recordQueryLatency(double ms);
    void setActiveConnections(uint64_t count);
    void onConnectionFailure();
    void onTransaction();
    void setWritesPerSec(double rate);
    void setReadsPerSec(double rate);
    void setQueueWaiting(uint64_t count);

    void reset();

private:
    std::atomic<uint64_t> successful_writes_{0};
    std::atomic<uint64_t> failed_writes_{0};
    std::atomic<double> insert_latency_ms_{0.0};
    std::atomic<double> query_latency_ms_{0.0};
    std::atomic<uint64_t> active_connections_{0};
    std::atomic<uint64_t> connection_failures_{0};
    std::atomic<uint64_t> transaction_count_{0};
    std::atomic<double> writes_per_sec_{0.0};
    std::atomic<double> reads_per_sec_{0.0};
    std::atomic<uint64_t> queue_waiting_{0};
};

#endif
