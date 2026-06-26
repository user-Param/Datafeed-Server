#ifndef QUEUE_MONITOR_H
#define QUEUE_MONITOR_H

#include <atomic>
#include <chrono>
#include <cstdint>

class QueueMonitor {
public:
    QueueMonitor() = default;
    ~QueueMonitor() = default;

    struct QueueStats {
        uint64_t incoming_depth = 0;
        uint64_t outgoing_depth = 0;
        uint64_t serialization_depth = 0;
        uint64_t max_incoming_depth = 0;
        uint64_t max_outgoing_depth = 0;
        uint64_t max_serialization_depth = 0;
        uint64_t overflow_count = 0;
        bool backpressure = false;
        double queue_wait_time_ms = 0.0;
        double queue_processing_time_ms = 0.0;
    };

    QueueStats getStats() const;

    void incrementIncoming();
    void decrementIncoming();
    void setIncomingDepth(uint64_t depth);

    void incrementOutgoing();
    void decrementOutgoing();
    void setOutgoingDepth(uint64_t depth);

    void incrementSerialization();
    void decrementSerialization();
    void setSerializationDepth(uint64_t depth);

    void onOverflow();
    void setBackpressure(bool active);
    void recordWaitTime(double ms);
    void recordProcessingTime(double ms);

    void reset();

private:
    std::atomic<uint64_t> incoming_depth_{0};
    std::atomic<uint64_t> outgoing_depth_{0};
    std::atomic<uint64_t> serialization_depth_{0};
    std::atomic<uint64_t> max_incoming_depth_{0};
    std::atomic<uint64_t> max_outgoing_depth_{0};
    std::atomic<uint64_t> max_serialization_depth_{0};
    std::atomic<uint64_t> overflow_count_{0};
    std::atomic<bool> backpressure_{false};
    std::atomic<double> queue_wait_time_ms_{0.0};
    std::atomic<double> queue_processing_time_ms_{0.0};
};

#endif
