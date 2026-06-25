#include "FeedService.hpp"
#include <iostream>
#include <optional>
#include <chrono>
#include <fstream>

namespace api {
namespace services {

FeedService::FeedService(std::shared_ptr<datafeed::SAdapter> adapter)
    : adapter_(std::move(adapter))
{
}

std::optional<dto::FeedStatusResponse> FeedService::getFeedStatus() {
    if (!adapter_ || !adapter_->is_connected()) {
        // #region agent log
        {
            std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
            dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"D\",\"location\":\"FeedService.cpp:getFeedStatus\","
                << "\"message\":\"adapter not connected\",\"data\":{},\"timestamp\":"
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch()).count()
                << "}\n";
        }
        // #endregion
        return std::nullopt;
    }

    try {
        auto instances = adapter_->get_feed_instances_by_condition("feed_status = 'connected'");
        if (instances.empty()) {
            instances = adapter_->get_feed_instances_by_condition("");
        }

        // #region agent log
        {
            std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
            dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"D\",\"location\":\"FeedService.cpp:getFeedStatus\","
                << "\"message\":\"feed instances queried\",\"data\":{\"count\":" << instances.size()
                << "},\"timestamp\":"
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch()).count()
                << "}\n";
        }
        // #endregion

        if (!instances.empty()) {
            auto& instance = instances.front();
            return dto::FeedStatusResponse{
                .instance_id = instance.instance_id,
                .exchange = instance.exchange,
                .status = instance.feed_status,
                .uptime = 0
            };
        }

        return std::nullopt;
    } catch (const std::exception& e) {
        std::cerr << "Error getting feed status: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<datafeed::FeedInstance> FeedService::getFeedInstanceById(const std::string& instance_id) {
    if (!adapter_ || !adapter_->is_connected() || instance_id.empty()) {
        return std::nullopt;
    }
    return adapter_->get_feed_instance_by_id(instance_id);
}

std::vector<datafeed::FeedInstance> FeedService::getFeedInstances() {
    if (!adapter_ || !adapter_->is_connected()) {
        return {};
    }
    return adapter_->get_feed_instances_by_condition("");
}

} // namespace services
} // namespace api
