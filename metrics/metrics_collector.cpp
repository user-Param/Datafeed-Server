#include "metrics_collector.h"

MetricsCollector::MetricsCollector()
    : latency_tracker_(1000)
    , throughput_tracker_()
    , system_monitor_()
    , exchange_monitor_()
    , queue_monitor_()
    , feed_health_monitor_()
    , session_monitor_()
    , network_monitor_()
    , database_monitor_()
{
}

// ---- Legacy throughput callbacks ----

void MetricsCollector::onMessageReceived(size_t bytes) {
    throughput_tracker_.onMessageReceived(bytes, 1);
}

void MetricsCollector::onMessageSent(size_t bytes) {
    throughput_tracker_.onMessageSent(bytes, 1);
}

// ---- Legacy latency measurement ----

std::chrono::steady_clock::time_point MetricsCollector::startLatencyMeasurement() {
    return latency_tracker_.startLatencyMeasurement(LatencyTracker::LatencyCategory::GENERAL);
}

void MetricsCollector::endLatencyMeasurement(std::chrono::steady_clock::time_point start_time) {
    latency_tracker_.endLatencyMeasurement(start_time, LatencyTracker::LatencyCategory::GENERAL);
}

std::chrono::steady_clock::time_point MetricsCollector::startLatencyMeasurement(LatencyTracker::LatencyCategory category) {
    return latency_tracker_.startLatencyMeasurement(category);
}

void MetricsCollector::endLatencyMeasurement(std::chrono::steady_clock::time_point start_time, LatencyTracker::LatencyCategory category) {
    latency_tracker_.endLatencyMeasurement(start_time, category);
}

// ---- Legacy session/client callbacks (map to session_monitor) ----

void MetricsCollector::onClientConnected() {
    session_monitor_.onClientConnected();
}

void MetricsCollector::onClientDisconnected() {
    session_monitor_.onClientDisconnected();
}

void MetricsCollector::onSessionCreated() {
    session_monitor_.onSessionCreated();
}

void MetricsCollector::onSessionRemoved() {
    session_monitor_.onSessionRemoved();
}

void MetricsCollector::onSubscriptionCreated() {
    session_monitor_.onSubscriptionCreated();
}

void MetricsCollector::onSubscriptionRemoved() {
    session_monitor_.onSubscriptionRemoved();
}

void MetricsCollector::onPacketDropped() {
    feed_health_monitor_.onPacketDrop();
}

void MetricsCollector::onDuplicatePacket() {
    feed_health_monitor_.onDuplicatePacket();
}

void MetricsCollector::onOutOfOrderPacket() {
    feed_health_monitor_.onOutOfOrderPacket();
}

void MetricsCollector::onParseError() {
    parse_errors_.fetch_add(1, std::memory_order_relaxed);
    feed_health_monitor_.onParseFailure();
}

void MetricsCollector::onReconnect() {
    reconnect_count_.fetch_add(1, std::memory_order_relaxed);
}

// ---- ExchangeMonitor forwarding ----

void MetricsCollector::onExchangeConnected(const std::string& exchange) {
    exchange_monitor_.onConnected(exchange);
}

void MetricsCollector::onExchangeDisconnected(const std::string& exchange) {
    exchange_monitor_.onDisconnected(exchange);
}

void MetricsCollector::onExchangeHeartbeat(const std::string& exchange) {
    exchange_monitor_.onHeartbeat(exchange);
}

void MetricsCollector::onExchangeHeartbeatFailure(const std::string& exchange) {
    exchange_monitor_.onHeartbeatFailure(exchange);
}

void MetricsCollector::onExchangeMessageReceived(const std::string& exchange) {
    exchange_monitor_.onMessageReceived(exchange);
}

void MetricsCollector::onExchangeMessageDropped(const std::string& exchange) {
    exchange_monitor_.onMessageDropped(exchange);
}

void MetricsCollector::onExchangeParseError(const std::string& exchange) {
    exchange_monitor_.onParseError(exchange);
}

void MetricsCollector::onExchangeWebSocketDisconnect(const std::string& exchange) {
    exchange_monitor_.onWebSocketDisconnect(exchange);
}

void MetricsCollector::onExchangeReconnect(const std::string& exchange) {
    exchange_monitor_.onReconnect(exchange);
}

void MetricsCollector::updateExchangeFeedLag(const std::string& exchange, double lag_ms) {
    exchange_monitor_.updateFeedLag(exchange, lag_ms);
}

void MetricsCollector::updateExchangeLatency(const std::string& exchange, double latency_ms) {
    exchange_monitor_.updateExchangeLatency(exchange, latency_ms);
}

// ---- QueueMonitor forwarding ----

void MetricsCollector::incrementIncomingQueue() {
    queue_monitor_.incrementIncoming();
}

void MetricsCollector::decrementIncomingQueue() {
    queue_monitor_.decrementIncoming();
}

void MetricsCollector::setIncomingQueueDepth(uint64_t depth) {
    queue_monitor_.setIncomingDepth(depth);
}

void MetricsCollector::incrementOutgoingQueue() {
    queue_monitor_.incrementOutgoing();
}

void MetricsCollector::decrementOutgoingQueue() {
    queue_monitor_.decrementOutgoing();
}

void MetricsCollector::setOutgoingQueueDepth(uint64_t depth) {
    queue_monitor_.setOutgoingDepth(depth);
}

void MetricsCollector::incrementSerializationQueue() {
    queue_monitor_.incrementSerialization();
}

void MetricsCollector::decrementSerializationQueue() {
    queue_monitor_.decrementSerialization();
}

void MetricsCollector::setSerializationQueueDepth(uint64_t depth) {
    queue_monitor_.setSerializationDepth(depth);
}

void MetricsCollector::onQueueOverflow() {
    queue_monitor_.onOverflow();
}

void MetricsCollector::setQueueBackpressure(bool active) {
    queue_monitor_.setBackpressure(active);
}

void MetricsCollector::recordQueueWaitTime(double ms) {
    queue_monitor_.recordWaitTime(ms);
}

void MetricsCollector::recordQueueProcessingTime(double ms) {
    queue_monitor_.recordProcessingTime(ms);
}

// ---- FeedHealthMonitor forwarding ----

void MetricsCollector::onFeedPacketDrop() {
    feed_health_monitor_.onPacketDrop();
}

void MetricsCollector::onFeedDuplicatePacket() {
    feed_health_monitor_.onDuplicatePacket();
}

void MetricsCollector::onFeedOutOfOrderPacket() {
    feed_health_monitor_.onOutOfOrderPacket();
}

void MetricsCollector::onFeedSequenceGap() {
    feed_health_monitor_.onSequenceGap();
}

void MetricsCollector::onFeedMissingTick() {
    feed_health_monitor_.onMissingTick();
}

void MetricsCollector::onFeedInvalidMessage() {
    feed_health_monitor_.onInvalidMessage();
}

void MetricsCollector::onFeedCorruptedPacket() {
    feed_health_monitor_.onCorruptedPacket();
}

void MetricsCollector::onFeedParseFailure() {
    feed_health_monitor_.onParseFailure();
}

void MetricsCollector::markFeedStale(bool stale) {
    feed_health_monitor_.markStale(stale);
}

// ---- SessionMonitor forwarding ----

void MetricsCollector::onSessionClientConnected(const std::string& client_id) {
    session_monitor_.onClientConnected(client_id);
}

void MetricsCollector::onSessionClientDisconnected(const std::string& client_id) {
    session_monitor_.onClientDisconnected(client_id);
}

void MetricsCollector::onSessionAuthenticationFailure(const std::string& client_id) {
    session_monitor_.onAuthenticationFailure(client_id);
}

void MetricsCollector::onSessionReconnect(const std::string& client_id) {
    session_monitor_.onReconnect(client_id);
}

// ---- NetworkMonitor forwarding ----

void MetricsCollector::onNetworkTcpReconnect() {
    network_monitor_.onTcpReconnect();
}

void MetricsCollector::onNetworkSocketError() {
    network_monitor_.onSocketError();
}

void MetricsCollector::onNetworkReadError() {
    network_monitor_.onReadError();
}

void MetricsCollector::onNetworkWriteError() {
    network_monitor_.onWriteError();
}

void MetricsCollector::onNetworkTlsHandshakeFailure() {
    network_monitor_.onTlsHandshakeFailure();
}

void MetricsCollector::onNetworkBytesTransmitted(uint64_t bytes) {
    network_monitor_.onBytesTransmitted(bytes);
}

void MetricsCollector::onNetworkBytesReceived(uint64_t bytes) {
    network_monitor_.onBytesReceived(bytes);
}

void MetricsCollector::updateNetworkSocketRtt(double rtt_ms) {
    network_monitor_.updateSocketRtt(rtt_ms);
}

void MetricsCollector::updateNetworkBandwidth(double bps) {
    network_monitor_.updateNetworkBandwidth(bps);
}

void MetricsCollector::onNetworkConnectionFailure() {
    network_monitor_.onConnectionFailure();
}

// ---- DatabaseMonitor forwarding ----

void MetricsCollector::onDatabaseSuccessfulWrite() {
    database_monitor_.onSuccessfulWrite();
}

void MetricsCollector::onDatabaseFailedWrite() {
    database_monitor_.onFailedWrite();
}

void MetricsCollector::recordDatabaseInsertLatency(double ms) {
    database_monitor_.recordInsertLatency(ms);
}

void MetricsCollector::recordDatabaseQueryLatency(double ms) {
    database_monitor_.recordQueryLatency(ms);
}

void MetricsCollector::setDatabaseActiveConnections(uint64_t count) {
    database_monitor_.setActiveConnections(count);
}

void MetricsCollector::onDatabaseConnectionFailure() {
    database_monitor_.onConnectionFailure();
}

void MetricsCollector::onDatabaseTransaction() {
    database_monitor_.onTransaction();
}

void MetricsCollector::setDatabaseWritesPerSec(double rate) {
    database_monitor_.setWritesPerSec(rate);
}

void MetricsCollector::setDatabaseReadsPerSec(double rate) {
    database_monitor_.setReadsPerSec(rate);
}

void MetricsCollector::setDatabaseQueueWaiting(uint64_t count) {
    database_monitor_.setQueueWaiting(count);
}

// ---- ThroughputTracker specialized ----

void MetricsCollector::onTick() {
    throughput_tracker_.onTick();
}

void MetricsCollector::onTrade() {
    throughput_tracker_.onTrade();
}

void MetricsCollector::onOrderbookUpdate() {
    throughput_tracker_.onOrderbookUpdate();
}

void MetricsCollector::onSubscription() {
    throughput_tracker_.onSubscription();
}

void MetricsCollector::onBroadcast() {
    throughput_tracker_.onBroadcast();
}

void MetricsCollector::onDatabaseWrite() {
    throughput_tracker_.onDatabaseWrite();
}

void MetricsCollector::onDatabaseRead() {
    throughput_tracker_.onDatabaseRead();
}

// ---- SystemMonitor ----

void MetricsCollector::updateCpuUsage() {
    system_monitor_.updateCpuUsage();
}

// ---- Accessors ----

ExchangeMonitor& MetricsCollector::exchangeMonitor() { return exchange_monitor_; }
QueueMonitor& MetricsCollector::queueMonitor() { return queue_monitor_; }
FeedHealthMonitor& MetricsCollector::feedHealthMonitor() { return feed_health_monitor_; }
SessionMonitor& MetricsCollector::sessionMonitor() { return session_monitor_; }
NetworkMonitor& MetricsCollector::networkMonitor() { return network_monitor_; }
DatabaseMonitor& MetricsCollector::databaseMonitor() { return database_monitor_; }
LatencyTracker& MetricsCollector::latencyTracker() { return latency_tracker_; }
ThroughputTracker& MetricsCollector::throughputTracker() { return throughput_tracker_; }
SystemMonitor& MetricsCollector::systemMonitor() { return system_monitor_; }

// ---- Snapshot ----

FeedMetricsSnapshot MetricsCollector::getSnapshot() {
    FeedMetricsSnapshot snapshot{};

    // Latency statistics (all categories)
    auto all_latency_stats = latency_tracker_.getAllLatencyStats();
    for (const auto& pair : all_latency_stats) {
        snapshot.latency_stats[pair.first] = {
            pair.second.average,
            pair.second.min,
            pair.second.max,
            pair.second.p50,
            pair.second.p95,
            pair.second.p99,
            pair.second.count
        };
    }

    // Throughput statistics
    auto throughput_stats = throughput_tracker_.getAndUpdateStats();
    snapshot.messages_received_per_sec = throughput_stats.messages_received_per_sec;
    snapshot.messages_sent_per_sec = throughput_stats.messages_sent_per_sec;
    snapshot.bytes_received_per_sec = throughput_stats.bytes_received_per_sec;
    snapshot.bytes_sent_per_sec = throughput_stats.bytes_sent_per_sec;
    snapshot.packets_received_per_sec = throughput_stats.packets_received_per_sec;
    snapshot.packets_sent_per_sec = throughput_stats.packets_sent_per_sec;
    snapshot.total_messages_received = throughput_stats.messages_received;
    snapshot.total_messages_sent = throughput_stats.messages_sent;
    snapshot.total_bytes_received = throughput_stats.bytes_received;
    snapshot.total_bytes_sent = throughput_stats.bytes_sent;
    snapshot.total_packets_received = throughput_stats.packets_received;
    snapshot.total_packets_sent = throughput_stats.packets_sent;
    snapshot.ticks_per_sec = throughput_stats.ticks_per_sec;
    snapshot.trades_per_sec = throughput_stats.trades_per_sec;
    snapshot.orderbook_updates_per_sec = throughput_stats.orderbook_updates_per_sec;
    snapshot.subscriptions_per_sec = throughput_stats.subscriptions_per_sec;
    snapshot.broadcasts_per_sec = throughput_stats.broadcasts_per_sec;
    snapshot.database_writes_per_sec = throughput_stats.database_writes_per_sec;
    snapshot.database_reads_per_sec = throughput_stats.database_reads_per_sec;
    snapshot.total_ticks = throughput_stats.ticks;
    snapshot.total_trades = throughput_stats.trades;
    snapshot.total_orderbook_updates = throughput_stats.orderbook_updates;
    snapshot.total_subscriptions = throughput_stats.subscriptions;
    snapshot.total_broadcasts = throughput_stats.broadcasts;
    snapshot.total_database_writes = throughput_stats.database_writes;
    snapshot.total_database_reads = throughput_stats.database_reads;

    // System metrics
    auto system_metrics = system_monitor_.getSystemMetrics();
    snapshot.cpu_usage_percent = system_metrics.cpu_usage_percent;
    snapshot.memory_rss = system_metrics.memory_rss;
    snapshot.peak_rss = system_metrics.peak_rss;
    snapshot.virtual_memory = system_metrics.virtual_memory;
    snapshot.heap_usage = system_metrics.heap_usage;
    snapshot.memory_growth_rate = system_metrics.memory_growth_rate;
    snapshot.thread_count = system_metrics.thread_count;
    snapshot.uptime_seconds = system_metrics.uptime_seconds;

    // Exchange metrics
    snapshot.exchange_stats = exchange_monitor_.getAllStats();

    // Queue metrics
    auto queue_stats = queue_monitor_.getStats();
    snapshot.incoming_queue_depth = queue_stats.incoming_depth;
    snapshot.outgoing_queue_depth = queue_stats.outgoing_depth;
    snapshot.serialization_queue_depth = queue_stats.serialization_depth;
    snapshot.max_incoming_queue_depth = queue_stats.max_incoming_depth;
    snapshot.max_outgoing_queue_depth = queue_stats.max_outgoing_depth;
    snapshot.max_serialization_queue_depth = queue_stats.max_serialization_depth;
    snapshot.queue_overflow_count = queue_stats.overflow_count;
    snapshot.queue_backpressure = queue_stats.backpressure;
    snapshot.queue_wait_time_ms = queue_stats.queue_wait_time_ms;
    snapshot.queue_processing_time_ms = queue_stats.queue_processing_time_ms;

    // Feed health metrics
    auto health_stats = feed_health_monitor_.getStats();
    snapshot.packet_drops = health_stats.packet_drops;
    snapshot.duplicate_packets = health_stats.duplicate_packets;
    snapshot.out_of_order_packets = health_stats.out_of_order_packets;
    snapshot.sequence_gaps = health_stats.sequence_gaps;
    snapshot.missing_ticks = health_stats.missing_ticks;
    snapshot.invalid_messages = health_stats.invalid_messages;
    snapshot.corrupted_packets = health_stats.corrupted_packets;
    snapshot.parse_failures = health_stats.parse_failures;
    snapshot.stale_feed = health_stats.stale_feed;
    snapshot.feed_health_score = health_stats.health_score;
    snapshot.feed_health_status = health_stats.status;

    // Session metrics
    auto session_stats = session_monitor_.getStats();
    snapshot.active_clients = session_stats.active_clients;
    snapshot.active_sessions = session_stats.active_sessions;
    snapshot.active_subscriptions = session_stats.active_subscriptions;
    snapshot.total_connections = session_stats.total_connections;
    snapshot.total_disconnections = session_stats.total_disconnections;
    snapshot.authentication_failures = session_stats.authentication_failures;
    snapshot.reconnect_count = session_stats.reconnect_count;
    snapshot.average_session_duration_ms = session_stats.average_session_duration_ms;
    snapshot.longest_session_duration_ms = session_stats.longest_session_duration_ms;

    // Network metrics
    auto network_stats = network_monitor_.getStats();
    snapshot.tcp_reconnects = network_stats.tcp_reconnects;
    snapshot.socket_errors = network_stats.socket_errors;
    snapshot.read_errors = network_stats.read_errors;
    snapshot.write_errors = network_stats.write_errors;
    snapshot.tls_handshake_failures = network_stats.tls_handshake_failures;
    snapshot.bytes_transmitted = network_stats.bytes_transmitted;
    snapshot.network_bytes_received = network_stats.bytes_received;
    snapshot.socket_rtt_ms = network_stats.socket_rtt_ms;
    snapshot.network_bandwidth_bps = network_stats.network_bandwidth_bps;
    snapshot.connection_failures = network_stats.connection_failures;

    // Database metrics
    auto db_stats = database_monitor_.getStats();
    snapshot.successful_writes = db_stats.successful_writes;
    snapshot.failed_writes = db_stats.failed_writes;
    snapshot.insert_latency_ms = db_stats.insert_latency_ms;
    snapshot.query_latency_ms = db_stats.query_latency_ms;
    snapshot.active_db_connections = db_stats.active_connections;
    snapshot.db_connection_failures = db_stats.connection_failures;
    snapshot.transaction_count = db_stats.transaction_count;
    snapshot.writes_per_sec = db_stats.writes_per_sec;
    snapshot.reads_per_sec = db_stats.reads_per_sec;
    snapshot.db_queue_waiting = db_stats.queue_waiting;

    return snapshot;
}

// ---- Reset ----

void MetricsCollector::reset() {
    latency_tracker_.clearAll();
    throughput_tracker_.reset();
    system_monitor_ = SystemMonitor();
    exchange_monitor_.resetAll();
    queue_monitor_.reset();
    feed_health_monitor_.reset();
    session_monitor_.reset();
    network_monitor_.reset();
    database_monitor_.reset();
    parse_errors_.store(0, std::memory_order_relaxed);
    reconnect_count_.store(0, std::memory_order_relaxed);
}
