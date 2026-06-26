#ifndef API_SERVICES_ALERTS_SERVICE_HPP
#define API_SERVICES_ALERTS_SERVICE_HPP

#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include "../../sadapter.h"

namespace api {
namespace services {

class AlertsService {
public:
    explicit AlertsService(std::shared_ptr<datafeed::SAdapter> sadapter);

    nlohmann::json getAlerts(const std::string& status = "");
    nlohmann::json acknowledgeAlert(const std::string& alert_id);
    nlohmann::json getAlertHistory();
    nlohmann::json getAudit(const std::string& from, const std::string& to,
        const std::string& actor, const std::string& action,
        const std::string& exchange, const std::string& client);

private:
    std::shared_ptr<datafeed::SAdapter> sadapter_;
};

} // namespace services
} // namespace api

#endif
