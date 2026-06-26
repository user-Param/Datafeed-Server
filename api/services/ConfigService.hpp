#ifndef API_SERVICES_CONFIG_SERVICE_HPP
#define API_SERVICES_CONFIG_SERVICE_HPP

#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include "../../sadapter.h"

namespace api {
namespace services {

class ConfigService {
public:
    explicit ConfigService(std::shared_ptr<datafeed::SAdapter> sadapter);

    nlohmann::json getConfig();
    nlohmann::json updateConfig(const nlohmann::json& config);
    nlohmann::json getThresholds();
    nlohmann::json updateThresholds(const nlohmann::json& thresholds);

private:
    std::shared_ptr<datafeed::SAdapter> sadapter_;
};

} // namespace services
} // namespace api

#endif
