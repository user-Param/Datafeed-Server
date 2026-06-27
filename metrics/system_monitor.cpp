#include "system_monitor.h"
#include <chrono>
#include <thread>
#include <fstream>
#include <sstream>
#include <string>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <mach/task_info.h>
#include <mach/thread_act.h>
#include <malloc/malloc.h>
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <unistd.h>
#include <sys/times.h>
#include <sys/resource.h>
#include <malloc.h>
#elif defined(_WIN32)
#include <windows.h>
#include <psapi.h>
#include <pdh.h>
#endif

SystemMonitor::SystemMonitor()
    : start_time_(std::chrono::steady_clock::now())
    , last_cpu_update_(std::chrono::steady_clock::now())
    , last_rss_(0)
    , cpu_usage_percent_(0.0)
#if defined(__APPLE__) || defined(__linux__)
    , last_process_time_(0)
    , last_system_time_(0)
#endif
{
}

void SystemMonitor::updateCpuUsage() {
    auto now = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(now - last_cpu_update_).count();
    
    if (elapsed_sec < 0.1) {
        return; // Too frequent, skip
    }

#if defined(__APPLE__)
    // macOS CPU usage measurement
    struct mach_task_basic_info info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count) == KERN_SUCCESS) {
        uint64_t current_process_time = info.user_time.seconds * 1000000ULL + info.user_time.microseconds +
                                        info.system_time.seconds * 1000000ULL + info.system_time.microseconds;
        
        if (last_process_time_ > 0) {
            uint64_t process_time_diff = current_process_time - last_process_time_;
            double cpu_percent = (process_time_diff / elapsed_sec) / 10000.0; // Convert to percentage
            cpu_usage_percent_ = cpu_percent;
        }
        
        last_process_time_ = current_process_time;
    }

#elif defined(__linux__)
    // Linux CPU usage measurement
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    uint64_t current_process_time = (usage.ru_utime.tv_sec * 1000000ULL + usage.ru_utime.tv_usec) +
                                    (usage.ru_stime.tv_sec * 1000000ULL + usage.ru_stime.tv_usec);
    
    // Get system CPU time from /proc/stat
    std::ifstream stat("/proc/stat");
    std::string line;
    uint64_t total_system_time = 0;
    if (std::getline(stat, line) && line.find("cpu ") == 0) {
        std::istringstream iss(line);
        std::string cpu_label;
        uint64_t user, nice, system, idle;
        iss >> cpu_label >> user >> nice >> system >> idle;
        total_system_time = user + nice + system + idle;
    }
    
    if (last_process_time_ > 0 && last_system_time_ > 0) {
        uint64_t process_time_diff = current_process_time - last_process_time_;
        uint64_t system_time_diff = total_system_time - last_system_time_;
        if (system_time_diff > 0) {
            cpu_usage_percent_ = (static_cast<double>(process_time_diff) / system_time_diff) * 100.0;
        }
    }
    
    last_process_time_ = current_process_time;
    last_system_time_ = total_system_time;

#elif defined(_WIN32)
    // Windows CPU usage measurement
    FILETIME creation_time, exit_time, kernel_time, user_time;
    if (GetProcessTimes(GetCurrentProcess(), &creation_time, &exit_time, &kernel_time, &user_time)) {
        uint64_t current_process_time = (kernel_time.dwLowDateTime | (static_cast<uint64_t>(kernel_time.dwHighDateTime) << 32)) +
                                        (user_time.dwLowDateTime | (static_cast<uint64_t>(user_time.dwHighDateTime) << 32));
        
        if (last_process_time_ > 0) {
            uint64_t process_time_diff = current_process_time - last_process_time_;
            double cpu_percent = (process_time_diff / elapsed_sec) / 100000.0; // Convert to percentage
            cpu_usage_percent_ = cpu_percent;
        }
        
        last_process_time_ = current_process_time;
    }
#endif

    last_cpu_update_ = now;
}

SystemMonitor::SystemMetrics SystemMonitor::getSystemMetrics() {
    SystemMetrics metrics{};

    // Uptime (always available)
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(now - start_time_);
    metrics.uptime_seconds = duration.count();

    // Update CPU usage
    updateCpuUsage();
    metrics.cpu_usage_percent = cpu_usage_percent_;

#if defined(__APPLE__)
    // macOS-specific implementation
    struct mach_task_basic_info info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count) == KERN_SUCCESS) {
        metrics.memory_rss = info.resident_size;
        metrics.virtual_memory = info.virtual_size;
        
        // Update peak RSS
        if (info.resident_size > last_rss_) {
            last_rss_ = info.resident_size;
        }
        metrics.peak_rss = last_rss_;
        
        // Memory growth rate
        double elapsed_sec = std::chrono::duration<double>(now - last_cpu_update_).count();
        if (elapsed_sec > 0.0 && last_rss_ > 0) {
            metrics.memory_growth_rate = static_cast<double>(info.resident_size - last_rss_) / elapsed_sec;
        } else {
            metrics.memory_growth_rate = 0.0;
        }
        
        // Heap usage - use malloc statistics on macOS
        malloc_statistics_t malloc_stats;
        malloc_zone_statistics(nullptr, &malloc_stats);
        metrics.heap_usage = malloc_stats.size_in_use;
    } else {
        metrics.memory_rss = 0;
        metrics.peak_rss = 0;
        metrics.virtual_memory = 0;
        metrics.heap_usage = 0;
        metrics.memory_growth_rate = 0.0;
    }

    // Get thread count
    thread_act_array_t thread_list;
    mach_msg_type_number_t thread_count;
    if (task_threads(mach_task_self(), &thread_list, &thread_count) == KERN_SUCCESS) {
        metrics.thread_count = static_cast<uint32_t>(thread_count);
        vm_deallocate(mach_task_self(), (vm_address_t)thread_list, thread_count * sizeof(thread_t));
    } else {
        metrics.thread_count = 0;
    }

#elif defined(__linux__)
    // Linux-specific implementation
    std::ifstream statm("/proc/self/statm");
    if (statm.is_open()) {
        unsigned long size, resident, share, text, lib, data, dt;
        statm >> size >> resident >> share >> text >> lib >> data >> dt;
        uint64_t current_rss = resident * sysconf(_SC_PAGESIZE);
        metrics.memory_rss = current_rss;
        metrics.virtual_memory = size * sysconf(_SC_PAGESIZE);
        
        // Update peak RSS
        if (current_rss > last_rss_) {
            last_rss_ = current_rss;
        }
        metrics.peak_rss = last_rss_;
        
        // Memory growth rate
        double elapsed_sec = std::chrono::duration<double>(now - last_cpu_update_).count();
        if (elapsed_sec > 0.0 && last_rss_ > 0) {
            metrics.memory_growth_rate = static_cast<double>(current_rss - last_rss_) / elapsed_sec;
        } else {
            metrics.memory_growth_rate = 0.0;
        }
        
        // Heap usage — prefer mallinfo2 (glibc ≥ 2.33), fall back to mallinfo, else 0
        #if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
        struct mallinfo2 mi = mallinfo2();
        metrics.heap_usage = mi.uordblks;
        #elif defined(__GLIBC__)
        struct mallinfo mi = mallinfo();
        metrics.heap_usage = mi.uordblks;
        #else
        metrics.heap_usage = 0;
        #endif
    } else {
        metrics.memory_rss = 0;
        metrics.peak_rss = 0;
        metrics.virtual_memory = 0;
        metrics.heap_usage = 0;
        metrics.memory_growth_rate = 0.0;
    }

    // Get thread count
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.find("Threads:") == 0) {
            std::istringstream iss(line);
            std::string key;
            uint32_t value;
            iss >> key >> value;
            metrics.thread_count = value;
            break;
        }
    }
    if (metrics.thread_count == 0) {
        metrics.thread_count = 0;
    }

#elif defined(_WIN32)
    // Windows-specific implementation
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
        metrics.memory_rss = pmc.WorkingSetSize;
        metrics.peak_rss = pmc.PeakWorkingSetSize;
        
        // Memory growth rate
        double elapsed_sec = std::chrono::duration<double>(now - last_cpu_update_).count();
        if (elapsed_sec > 0.0 && last_rss_ > 0) {
            metrics.memory_growth_rate = static_cast<double>(pmc.WorkingSetSize - last_rss_) / elapsed_sec;
        } else {
            metrics.memory_growth_rate = 0.0;
        }
        last_rss_ = pmc.WorkingSetSize;
        
        // Virtual memory and heap usage not easily available on Windows
        metrics.virtual_memory = 0;
        metrics.heap_usage = 0;
    } else {
        metrics.memory_rss = 0;
        metrics.peak_rss = 0;
        metrics.virtual_memory = 0;
        metrics.heap_usage = 0;
        metrics.memory_growth_rate = 0.0;
    }

    // Get thread count
    HANDLE hThreadSnap = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
    if (hThreadSnap != INVALID_HANDLE_VALUE) {
        THREADENTRY32 te32;
        te32.dwSize = sizeof(THREADENTRY32);
        DWORD currentProcessId = GetCurrentProcessId();
        metrics.thread_count = 0;
        if (Thread32First(hThreadSnap, &te32)) {
            do {
                if (te32.th32OwnerProcessID == currentProcessId) {
                    metrics.thread_count++;
                }
            } while (Thread32Next(hThreadSnap, &te32));
        }
        CloseHandle(hThreadSnap);
    } else {
        metrics.thread_count = 0;
    }

#else
    // Generic fallback - placeholders
    metrics.memory_rss = 0;
    metrics.peak_rss = 0;
    metrics.virtual_memory = 0;
    metrics.heap_usage = 0;
    metrics.memory_growth_rate = 0.0;
    metrics.thread_count = 0;
#endif

    return metrics;
}