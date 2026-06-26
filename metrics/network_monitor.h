#ifndef NETWORK_MONITOR_H
#define NETWORK_MONITOR_H

#include <atomic>
#include <chrono>
#include <cstdint>

class NetworkMonitor {
public:
    NetworkMonitor() = default;
    ~NetworkMonitor() = default;

    struct NetworkStats {
        uint64_t tcp_reconnects = 0;
        uint64_t socket_errors = 0;
        uint64_t read_errors = 0;
        uint64_t write_errors = 0;
        uint64_t tls_handshake_failures = 0;
        uint64_t bytes_transmitted = 0;
        uint64_t bytes_received = 0;
        double socket_rtt_ms = 0.0;
        double network_bandwidth_bps = 0.0;
        uint64_t connection_failures = 0;
    };

    NetworkStats getStats() const;

    void onTcpReconnect();
    void onSocketError();
    void onReadError();
    void onWriteError();
    void onTlsHandshakeFailure();
    void onBytesTransmitted(uint64_t bytes);
    void onBytesReceived(uint64_t bytes);
    void updateSocketRtt(double rtt_ms);
    void updateNetworkBandwidth(double bps);
    void onConnectionFailure();

    void reset();

private:
    std::atomic<uint64_t> tcp_reconnects_{0};
    std::atomic<uint64_t> socket_errors_{0};
    std::atomic<uint64_t> read_errors_{0};
    std::atomic<uint64_t> write_errors_{0};
    std::atomic<uint64_t> tls_handshake_failures_{0};
    std::atomic<uint64_t> bytes_transmitted_{0};
    std::atomic<uint64_t> bytes_received_{0};
    std::atomic<double> socket_rtt_ms_{0.0};
    std::atomic<double> network_bandwidth_bps_{0.0};
    std::atomic<uint64_t> connection_failures_{0};
};

#endif
