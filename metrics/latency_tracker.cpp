#include "latency_tracker.h"
#include <algorithm>
#include <cmath>

LatencyTracker::LatencyTracker(size_t window_size)
    : window_size_(window_size)
{
    // Initialize latency windows for all categories
    latency_windows_[LatencyCategory::EXCHANGE] = LatencyWindow(window_size);
    latency_windows_[LatencyCategory::PARSING] = LatencyWindow(window_size);
    latency_windows_[LatencyCategory::NORMALIZATION] = LatencyWindow(window_size);
    latency_windows_[LatencyCategory::PROCESSING] = LatencyWindow(window_size);
    latency_windows_[LatencyCategory::BROADCAST] = LatencyWindow(window_size);
    latency_windows_[LatencyCategory::SERIALIZATION] = LatencyWindow(window_size);
    latency_windows_[LatencyCategory::SOCKET_SEND] = LatencyWindow(window_size);
    latency_windows_[LatencyCategory::GENERAL] = LatencyWindow(window_size);
}

LatencyTracker::time_point LatencyTracker::startLatencyMeasurement() {
    return clock::now();
}

LatencyTracker::time_point LatencyTracker::startLatencyMeasurement(LatencyCategory category) {
    return clock::now();
}

void LatencyTracker::endLatencyMeasurement(time_point start_time) {
    endLatencyMeasurement(start_time, LatencyCategory::GENERAL);
}

void LatencyTracker::endLatencyMeasurement(time_point start_time, LatencyCategory category) {
    auto end_time = clock::now();
    duration diff = end_time - start_time;
    double latency_ms = std::chrono::duration<double, std::milli>(diff).count();

    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = latency_windows_.find(category);
    if (it == latency_windows_.end()) {
        return; // Category not found
    }

    LatencyWindow& window = it->second;
    size_t index = window.head;
    double old_val = window.latencies[index];
    window.latencies[index] = latency_ms;
    window.head = (window.head + 1) % window_size_;
    window.total_count++;

    // Update sum of latencies in the window
    if (window.total_count > window_size_) {
        window.sum_latency += latency_ms - old_val;
    } else {
        window.sum_latency += latency_ms;
    }
}

LatencyTracker::LatencyStats LatencyTracker::getLatencyStatsUnlocked(LatencyCategory category) const {
    auto it = latency_windows_.find(category);
    if (it == latency_windows_.end()) {
        return {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0};
    }

    const LatencyWindow& window = it->second;
    size_t valid_count = std::min(static_cast<size_t>(window.total_count), window_size_);
    if (valid_count == 0) {
        return {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0};
    }

    std::vector<double> samples;
    samples.reserve(valid_count);
    size_t start_index = (window.head + window_size_ - valid_count) % window_size_;
    for (size_t i = 0; i < valid_count; ++i) {
        size_t idx = (start_index + i) % window_size_;
        samples.push_back(window.latencies[idx]);
    }

    double min_val = samples[0];
    double max_val = samples[0];
    for (double val : samples) {
        if (val < min_val) min_val = val;
        if (val > max_val) max_val = val;
    }
    double average = window.sum_latency / static_cast<double>(valid_count);

    std::sort(samples.begin(), samples.end());
    auto percentile = [&](double p) {
        if (samples.empty()) return 0.0;
        double index = p * (static_cast<double>(samples.size()) - 1.0);
        size_t lower = static_cast<size_t>(std::floor(index));
        size_t upper = static_cast<size_t>(std::ceil(index));
        if (lower == upper) {
            return samples[lower];
        }
        double fraction = index - static_cast<double>(lower);
        return samples[lower] + fraction * (samples[upper] - samples[lower]);
    };

    return {average, min_val, max_val, percentile(0.5), percentile(0.95), percentile(0.99),
            static_cast<uint64_t>(valid_count)};
}

LatencyTracker::LatencyStats LatencyTracker::getLatencyStats(LatencyCategory category) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return getLatencyStatsUnlocked(category);
}

std::unordered_map<LatencyTracker::LatencyCategory, LatencyTracker::LatencyStats> 
LatencyTracker::getAllLatencyStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::unordered_map<LatencyCategory, LatencyStats> all_stats;
    for (const auto& pair : latency_windows_) {
        all_stats[pair.first] = getLatencyStatsUnlocked(pair.first);
    }
    return all_stats;
}

void LatencyTracker::clear(LatencyCategory category) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = latency_windows_.find(category);
    if (it != latency_windows_.end()) {
        LatencyWindow& window = it->second;
        window.head = 0;
        window.total_count = 0;
        window.sum_latency = 0.0;
        std::fill(window.latencies.begin(), window.latencies.end(), 0.0);
    }
}

void LatencyTracker::clearAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto& pair : latency_windows_) {
        LatencyWindow& window = pair.second;
        window.head = 0;
        window.total_count = 0;
        window.sum_latency = 0.0;
        std::fill(window.latencies.begin(), window.latencies.end(), 0.0);
    }
}