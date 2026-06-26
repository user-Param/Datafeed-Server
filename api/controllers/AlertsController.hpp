#ifndef API_CONTROLLERS_ALERTS_CONTROLLER_HPP
#define API_CONTROLLERS_ALERTS_CONTROLLER_HPP

#include <memory>
#include <boost/beast/http.hpp>
#include "../services/AlertsService.hpp"

namespace api {
namespace controllers {

class AlertsController {
public:
    explicit AlertsController(std::shared_ptr<services::AlertsService> service);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetAlerts(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleAcknowledgeAlert(const boost::beast::http::request<boost::beast::http::string_body>& req,
                           const std::string& alert_id);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetAlertHistory(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetAudit(const boost::beast::http::request<boost::beast::http::string_body>& req);

private:
    std::string getQueryParam(const boost::beast::http::request<boost::beast::http::string_body>& req,
                              const std::string& key);
    boost::beast::http::response<boost::beast::http::string_body>
    makeJsonResponse(const boost::beast::http::request<boost::beast::http::string_body>& req,
                     const nlohmann::json& data);

    std::shared_ptr<services::AlertsService> service_;
};

} // namespace controllers
} // namespace api

#endif
