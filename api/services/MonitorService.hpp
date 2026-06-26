#ifndef API_SERVICES_MONITOR_SERVICE_HPP
#define API_SERVICES_MONITOR_SERVICE_HPP

#include <memory>
#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>
#include "../../sadapter.h"
#include "../../metrics/state_manager.h"
#include "../../metrics/metrics_collector.h"

namespace api {
namespace services {

class MonitorService {
public:
    MonitorService(
        std::shared_ptr<datafeed::SAdapter> sadapter,
        StateManager& state_manager,
        MetricsCollector& collector);

    nlohmann::json getDashboard();
    nlohmann::json getLiveMetrics();
    nlohmann::json getHistoryMetrics(const std::string& from, const std::string& to, const std::string& interval);
    nlohmann::json getLivePerformance();
    nlohmann::json getHistoryPerformance(const std::string& from, const std::string& to);
    nlohmann::json getLiveThroughput();
    nlohmann::json getHistoryThroughput(const std::string& from, const std::string& to);
    nlohmann::json getLiveFeed();
    nlohmann::json getHistoryFeed(const std::string& from, const std::string& to);
    nlohmann::json getExchanges();
    nlohmann::json getExchange(const std::string& exchange);
    nlohmann::json getExchangeHistory(const std::string& exchange, const std::string& from, const std::string& to);
    nlohmann::json getLiveQueues();
    nlohmann::json getHistoryQueues(const std::string& from, const std::string& to);
    nlohmann::json getLiveNetwork();
    nlohmann::json getHistoryNetwork(const std::string& from, const std::string& to);
    nlohmann::json getLiveDatabase();
    nlohmann::json getHistoryDatabase(const std::string& from, const std::string& to);
    nlohmann::json getLiveSystem();
    nlohmann::json getHistorySystem(const std::string& from, const std::string& to);
    nlohmann::json getLiveSessions();
    nlohmann::json getHistorySessions(const std::string& from, const std::string& to);
    nlohmann::json getAnalytics();
    nlohmann::json getTimeline();
    nlohmann::json getDependencies();
    nlohmann::json getTopology();

private:
    nlohmann::json snapshotToJson(const ::FeedMetricsSnapshot& s);
    nlohmann::json latencyCategoryToJson(const std::string& name,
        const std::unordered_map<LatencyTracker::LatencyCategory,
        ::FeedMetricsSnapshot::LatencyCategoryStats>& stats,
        LatencyTracker::LatencyCategory cat);

    std::shared_ptr<datafeed::SAdapter> sadapter_;
    StateManager& state_manager_;
    MetricsCollector& collector_;
    uint64_t start_time_;
};

} // namespace services
} // namespace api

#endif
