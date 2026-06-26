#include "network_monitor.h"

NetworkMonitor::NetworkStats NetworkMonitor::getStats() const {
    return {
        tcp_reconnects_.load(std::memory_order_relaxed),
        socket_errors_.load(std::memory_order_relaxed),
        read_errors_.load(std::memory_order_relaxed),
        write_errors_.load(std::memory_order_relaxed),
        tls_handshake_failures_.load(std::memory_order_relaxed),
        bytes_transmitted_.load(std::memory_order_relaxed),
        bytes_received_.load(std::memory_order_relaxed),
        socket_rtt_ms_.load(std::memory_order_relaxed),
        network_bandwidth_bps_.load(std::memory_order_relaxed),
        connection_failures_.load(std::memory_order_relaxed)
    };
}

void NetworkMonitor::onTcpReconnect() {
    tcp_reconnects_.fetch_add(1, std::memory_order_relaxed);
}

void NetworkMonitor::onSocketError() {
    socket_errors_.fetch_add(1, std::memory_order_relaxed);
}

void NetworkMonitor::onReadError() {
    read_errors_.fetch_add(1, std::memory_order_relaxed);
}

void NetworkMonitor::onWriteError() {
    write_errors_.fetch_add(1, std::memory_order_relaxed);
}

void NetworkMonitor::onTlsHandshakeFailure() {
    tls_handshake_failures_.fetch_add(1, std::memory_order_relaxed);
}

void NetworkMonitor::onBytesTransmitted(uint64_t bytes) {
    bytes_transmitted_.fetch_add(bytes, std::memory_order_relaxed);
}

void NetworkMonitor::onBytesReceived(uint64_t bytes) {
    bytes_received_.fetch_add(bytes, std::memory_order_relaxed);
}

void NetworkMonitor::updateSocketRtt(double rtt_ms) {
    socket_rtt_ms_.store(rtt_ms, std::memory_order_relaxed);
}

void NetworkMonitor::updateNetworkBandwidth(double bps) {
    network_bandwidth_bps_.store(bps, std::memory_order_relaxed);
}

void NetworkMonitor::onConnectionFailure() {
    connection_failures_.fetch_add(1, std::memory_order_relaxed);
}

void NetworkMonitor::reset() {
    tcp_reconnects_.store(0, std::memory_order_relaxed);
    socket_errors_.store(0, std::memory_order_relaxed);
    read_errors_.store(0, std::memory_order_relaxed);
    write_errors_.store(0, std::memory_order_relaxed);
    tls_handshake_failures_.store(0, std::memory_order_relaxed);
    bytes_transmitted_.store(0, std::memory_order_relaxed);
    bytes_received_.store(0, std::memory_order_relaxed);
    socket_rtt_ms_.store(0.0, std::memory_order_relaxed);
    network_bandwidth_bps_.store(0.0, std::memory_order_relaxed);
    connection_failures_.store(0, std::memory_order_relaxed);
}
