#ifndef SNAPSHOT_CONVERTER_H
#define SNAPSHOT_CONVERTER_H

#include <string>
#include <vector>
#include <cstdint>

struct FeedMetricsSnapshot;

namespace datafeed {
struct FeedMetricsSnapshot;
struct ExchangeMetricsEntry;
struct QueueEntry;
struct SystemMetricsEntry;
struct NetworkMetricsEntry;
struct DatabaseMetricsEntry;
}

namespace snapshot_converter {

datafeed::FeedMetricsSnapshot to_db_snapshot(
    const ::FeedMetricsSnapshot& snapshot,
    const std::string& instance_id,
    uint64_t measured_at);

std::vector<datafeed::ExchangeMetricsEntry> to_exchange_entries(
    const ::FeedMetricsSnapshot& snapshot,
    const std::string& instance_id,
    uint64_t measured_at);

datafeed::QueueEntry to_queue_entry(
    const ::FeedMetricsSnapshot& snapshot,
    const std::string& instance_id,
    uint64_t measured_at);

datafeed::SystemMetricsEntry to_system_entry(
    const ::FeedMetricsSnapshot& snapshot,
    const std::string& instance_id,
    uint64_t measured_at);

datafeed::NetworkMetricsEntry to_network_entry(
    const ::FeedMetricsSnapshot& snapshot,
    const std::string& instance_id,
    uint64_t measured_at);

datafeed::DatabaseMetricsEntry to_database_entry(
    const ::FeedMetricsSnapshot& snapshot,
    const std::string& instance_id,
    uint64_t measured_at);

} // namespace snapshot_converter

#endif
