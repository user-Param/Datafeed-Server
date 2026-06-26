#include "throughput_tracker.h"
#include <chrono>

ThroughputTracker::ThroughputTracker()
    : messages_received_(0)
    , messages_sent_(0)
    , bytes_received_(0)
    , bytes_sent_(0)
    , packets_received_(0)
    , packets_sent_(0)
    , ticks_(0)
    , trades_(0)
    , orderbook_updates_(0)
    , subscriptions_(0)
    , broadcasts_(0)
    , database_writes_(0)
    , database_reads_(0)
{
    auto now = std::chrono::steady_clock::now();
    last_update_time_ = now;
    last_messages_received_ = 0;
    last_messages_sent_ = 0;
    last_bytes_received_ = 0;
    last_bytes_sent_ = 0;
    last_packets_received_ = 0;
    last_packets_sent_ = 0;
    last_ticks_ = 0;
    last_trades_ = 0;
    last_orderbook_updates_ = 0;
    last_subscriptions_ = 0;
    last_broadcasts_ = 0;
    last_database_writes_ = 0;
    last_database_reads_ = 0;
}

void ThroughputTracker::onMessageReceived(size_t bytes, size_t packets) {
    messages_received_.fetch_add(1, std::memory_order_relaxed);
    if (bytes > 0) {
        bytes_received_.fetch_add(bytes, std::memory_order_relaxed);
    }
    if (packets > 0) {
        packets_received_.fetch_add(packets, std::memory_order_relaxed);
    }
}

void ThroughputTracker::onMessageSent(size_t bytes, size_t packets) {
    messages_sent_.fetch_add(1, std::memory_order_relaxed);
    if (bytes > 0) {
        bytes_sent_.fetch_add(bytes, std::memory_order_relaxed);
    }
    if (packets > 0) {
        packets_sent_.fetch_add(packets, std::memory_order_relaxed);
    }
}

void ThroughputTracker::onTick() {
    ticks_.fetch_add(1, std::memory_order_relaxed);
}

void ThroughputTracker::onTrade() {
    trades_.fetch_add(1, std::memory_order_relaxed);
}

void ThroughputTracker::onOrderbookUpdate() {
    orderbook_updates_.fetch_add(1, std::memory_order_relaxed);
}

void ThroughputTracker::onSubscription() {
    subscriptions_.fetch_add(1, std::memory_order_relaxed);
}

void ThroughputTracker::onBroadcast() {
    broadcasts_.fetch_add(1, std::memory_order_relaxed);
}

void ThroughputTracker::onDatabaseWrite() {
    database_writes_.fetch_add(1, std::memory_order_relaxed);
}

void ThroughputTracker::onDatabaseRead() {
    database_reads_.fetch_add(1, std::memory_order_relaxed);
}

ThroughputTracker::ThroughputStats ThroughputTracker::getAndUpdateStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(now - last_update_time_).count();

    // Avoid division by zero
    if (elapsed_sec < 1e-9) {
        elapsed_sec = 1e-9;
    }

    uint64_t current_mr = messages_received_.load(std::memory_order_relaxed);
    uint64_t current_ms = messages_sent_.load(std::memory_order_relaxed);
    uint64_t current_br = bytes_received_.load(std::memory_order_relaxed);
    uint64_t current_bs = bytes_sent_.load(std::memory_order_relaxed);
    uint64_t current_pr = packets_received_.load(std::memory_order_relaxed);
    uint64_t current_ps = packets_sent_.load(std::memory_order_relaxed);
    uint64_t current_ticks = ticks_.load(std::memory_order_relaxed);
    uint64_t current_trades = trades_.load(std::memory_order_relaxed);
    uint64_t current_orderbook_updates = orderbook_updates_.load(std::memory_order_relaxed);
    uint64_t current_subscriptions = subscriptions_.load(std::memory_order_relaxed);
    uint64_t current_broadcasts = broadcasts_.load(std::memory_order_relaxed);
    uint64_t current_database_writes = database_writes_.load(std::memory_order_relaxed);
    uint64_t current_database_reads = database_reads_.load(std::memory_order_relaxed);

    double mr_per_sec = 0.0, ms_per_sec = 0.0, br_per_sec = 0.0, bs_per_sec = 0.0, pr_per_sec = 0.0, ps_per_sec = 0.0;
    double ticks_per_sec = 0.0, trades_per_sec = 0.0, orderbook_updates_per_sec = 0.0;
    double subscriptions_per_sec = 0.0, broadcasts_per_sec = 0.0, database_writes_per_sec = 0.0, database_reads_per_sec = 0.0;
    
    if (elapsed_sec > 0.0) {
        mr_per_sec = static_cast<double>(current_mr - last_messages_received_) / elapsed_sec;
        ms_per_sec = static_cast<double>(current_ms - last_messages_sent_) / elapsed_sec;
        br_per_sec = static_cast<double>(current_br - last_bytes_received_) / elapsed_sec;
        bs_per_sec = static_cast<double>(current_bs - last_bytes_sent_) / elapsed_sec;
        pr_per_sec = static_cast<double>(current_pr - last_packets_received_) / elapsed_sec;
        ps_per_sec = static_cast<double>(current_ps - last_packets_sent_) / elapsed_sec;
        ticks_per_sec = static_cast<double>(current_ticks - last_ticks_) / elapsed_sec;
        trades_per_sec = static_cast<double>(current_trades - last_trades_) / elapsed_sec;
        orderbook_updates_per_sec = static_cast<double>(current_orderbook_updates - last_orderbook_updates_) / elapsed_sec;
        subscriptions_per_sec = static_cast<double>(current_subscriptions - last_subscriptions_) / elapsed_sec;
        broadcasts_per_sec = static_cast<double>(current_broadcasts - last_broadcasts_) / elapsed_sec;
        database_writes_per_sec = static_cast<double>(current_database_writes - last_database_writes_) / elapsed_sec;
        database_reads_per_sec = static_cast<double>(current_database_reads - last_database_reads_) / elapsed_sec;
    }

    // Update last values
    last_messages_received_ = current_mr;
    last_messages_sent_ = current_ms;
    last_bytes_received_ = current_br;
    last_bytes_sent_ = current_bs;
    last_packets_received_ = current_pr;
    last_packets_sent_ = current_ps;
    last_ticks_ = current_ticks;
    last_trades_ = current_trades;
    last_orderbook_updates_ = current_orderbook_updates;
    last_subscriptions_ = current_subscriptions;
    last_broadcasts_ = current_broadcasts;
    last_database_writes_ = current_database_writes;
    last_database_reads_ = current_database_reads;
    last_update_time_ = now;

    return {
        current_mr,
        current_ms,
        current_br,
        current_bs,
        current_pr,
        current_ps,
        mr_per_sec,
        ms_per_sec,
        br_per_sec,
        bs_per_sec,
        pr_per_sec,
        ps_per_sec,
        current_ticks,
        current_trades,
        current_orderbook_updates,
        current_subscriptions,
        current_broadcasts,
        current_database_writes,
        current_database_reads,
        ticks_per_sec,
        trades_per_sec,
        orderbook_updates_per_sec,
        subscriptions_per_sec,
        broadcasts_per_sec,
        database_writes_per_sec,
        database_reads_per_sec
    };
}

void ThroughputTracker::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    messages_received_.store(0, std::memory_order_relaxed);
    messages_sent_.store(0, std::memory_order_relaxed);
    bytes_received_.store(0, std::memory_order_relaxed);
    bytes_sent_.store(0, std::memory_order_relaxed);
    packets_received_.store(0, std::memory_order_relaxed);
    packets_sent_.store(0, std::memory_order_relaxed);
    ticks_.store(0, std::memory_order_relaxed);
    trades_.store(0, std::memory_order_relaxed);
    orderbook_updates_.store(0, std::memory_order_relaxed);
    subscriptions_.store(0, std::memory_order_relaxed);
    broadcasts_.store(0, std::memory_order_relaxed);
    database_writes_.store(0, std::memory_order_relaxed);
    database_reads_.store(0, std::memory_order_relaxed);

    auto now = std::chrono::steady_clock::now();
    last_update_time_ = now;
    last_messages_received_ = 0;
    last_messages_sent_ = 0;
    last_bytes_received_ = 0;
    last_bytes_sent_ = 0;
    last_packets_received_ = 0;
    last_packets_sent_ = 0;
    last_ticks_ = 0;
    last_trades_ = 0;
    last_orderbook_updates_ = 0;
    last_subscriptions_ = 0;
    last_broadcasts_ = 0;
    last_database_writes_ = 0;
    last_database_reads_ = 0;
}