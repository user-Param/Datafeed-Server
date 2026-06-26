#ifndef SYSTEM_MONITOR_H
#define SYSTEM_MONITOR_H

#include <chrono>
#include <cstdint>

class SystemMonitor {
public:
    SystemMonitor();
    ~SystemMonitor() = default;

    struct SystemMetrics {
        double cpu_usage_percent;  // CPU usage as percentage
        uint64_t memory_rss;       // Resident Set Size in bytes
        uint64_t peak_rss;         // Peak RSS in bytes
        uint64_t virtual_memory;   // Virtual memory in bytes
        uint64_t heap_usage;       // Heap usage in bytes (where available)
        double memory_growth_rate; // Memory growth rate in bytes/sec
        uint32_t thread_count;     // Number of threads
        double uptime_seconds;     // Process uptime in seconds
    };

    // Get current system metrics
    SystemMetrics getSystemMetrics();

    // Update CPU usage measurement (call periodically for accurate CPU measurement)
    void updateCpuUsage();

private:
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point last_cpu_update_;
    uint64_t last_rss_;
    double cpu_usage_percent_;
    
#if defined(__APPLE__) || defined(__linux__)
    // CPU usage tracking fields
    uint64_t last_process_time_;
    uint64_t last_system_time_;
#endif
};

#endif // SYSTEM_MONITOR_H
