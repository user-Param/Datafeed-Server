#include "FeedService.hpp"
#include <iostream>
#include <optional>
#include <chrono>

namespace api {
namespace services {

FeedService::FeedService(std::shared_ptr<datafeed::SAdapter> adapter)
    : adapter_(std::move(adapter))
{
}

std::optional<dto::FeedStatusResponse> FeedService::getFeedStatus() {
    if (!adapter_ || !adapter_->is_connected()) {
        return std::nullopt;
    }

    try {
        // Try to get a running instance first
        auto instances = adapter_->get_feed_instances_by_condition("feed_status = 'running'");
        if (!instances.empty()) {
            auto& instance = instances.front();
            return dto::FeedStatusResponse{
                .instance_id = instance.instance_id,
                .exchange = instance.exchange,
                .status = instance.feed_status,
                .uptime = 0 // placeholder; actual uptime not stored in FeedInstance
            };
        }

        // If no running instance, get the first instance
        instances = adapter_->get_feed_instances_by_condition("");
        if (!instances.empty()) {
            auto& instance = instances.front();
            return dto::FeedStatusResponse{
                .instance_id = instance.instance_id,
                .exchange = instance.exchange,
                .status = instance.feed_status,
                .uptime = 0 // placeholder
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