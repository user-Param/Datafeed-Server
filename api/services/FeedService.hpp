#ifndef API_SERVICES_FEED_SERVICE_HPP
#define API_SERVICES_FEED_SERVICE_HPP

#include <optional>
#include <memory>
#include <string>
#include <vector>
#include "../dto/FeedStatusResponse.hpp"
#include "../../sadapter.h"

namespace api {
namespace services {

class FeedService {
public:
    explicit FeedService(std::shared_ptr<datafeed::SAdapter> adapter);

    // Get the status of the feed instance (returns the first running instance or the first instance if none running)
    std::optional<dto::FeedStatusResponse> getFeedStatus();

    // Get feed instance by ID
    std::optional<datafeed::FeedInstance> getFeedInstanceById(const std::string& instance_id);

    // Get all feed instances
    std::vector<datafeed::FeedInstance> getFeedInstances();

private:
    std::shared_ptr<datafeed::SAdapter> adapter_;
};

} // namespace services
} // namespace api

#endif // API_SERVICES_FEED_SERVICE_HPP