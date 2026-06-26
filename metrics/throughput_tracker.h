#ifndef THROUGHPUT_TRACKER_H
#define THROUGHPUT_TRACKER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

class ThroughputTracker {
public:
    ThroughputTracker();
    ~ThroughputTracker() = default;

    // Increment counters
    void onMessageReceived(size_t bytes = 0, size_t packets = 0);
    void onMessageSent(size_t bytes = 0, size_t packets = 0);

    // Specialized counters for market data
    void onTick();
    void onTrade();
    void onOrderbookUpdate();
    void onSubscription();
    void onBroadcast();
    void onDatabaseWrite();
    void onDatabaseRead();

    // Get current statistics including rates since last call
    struct ThroughputStats {
        uint64_t messages_received;
        uint64_t messages_sent;
        uint64_t bytes_received;
        uint64_t bytes_sent;
        uint64_t packets_received;
        uint64_t packets_sent;
        double messages_received_per_sec;
        double messages_sent_per_sec;
        double bytes_received_per_sec;
        double bytes_sent_per_sec;
        double packets_received_per_sec;
        double packets_sent_per_sec;
        
        // Specialized rates
        uint64_t ticks;
        uint64_t trades;
        uint64_t orderbook_updates;
        uint64_t subscriptions;
        uint64_t broadcasts;
        uint64_t database_writes;
        uint64_t database_reads;
        double ticks_per_sec;
        double trades_per_sec;
        double orderbook_updates_per_sec;
        double subscriptions_per_sec;
        double broadcasts_per_sec;
        double database_writes_per_sec;
        double database_reads_per_sec;
    };

    ThroughputStats getAndUpdateStats();

    // Reset all counters
    void reset();

private:
    std::atomic<uint64_t> messages_received_;
    std::atomic<uint64_t> messages_sent_;
    std::atomic<uint64_t> bytes_received_;
    std::atomic<uint64_t> bytes_sent_;
    std::atomic<uint64_t> packets_received_;
    std::atomic<uint64_t> packets_sent_;

    // Specialized counters
    std::atomic<uint64_t> ticks_;
    std::atomic<uint64_t> trades_;
    std::atomic<uint64_t> orderbook_updates_;
    std::atomic<uint64_t> subscriptions_;
    std::atomic<uint64_t> broadcasts_;
    std::atomic<uint64_t> database_writes_;
    std::atomic<uint64_t> database_reads_;

    std::mutex mutex_;
    std::chrono::steady_clock::time_point last_update_time_;
    uint64_t last_messages_received_;
    uint64_t last_messages_sent_;
    uint64_t last_bytes_received_;
    uint64_t last_bytes_sent_;
    uint64_t last_packets_received_;
    uint64_t last_packets_sent_;
    uint64_t last_ticks_;
    uint64_t last_trades_;
    uint64_t last_orderbook_updates_;
    uint64_t last_subscriptions_;
    uint64_t last_broadcasts_;
    uint64_t last_database_writes_;
    uint64_t last_database_reads_;
};

#endif // THROUGHPUT_TRACKER_H