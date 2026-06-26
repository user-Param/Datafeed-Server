#ifndef API_CONTROLLERS_DASHBOARD_CONTROLLER_HPP
#define API_CONTROLLERS_DASHBOARD_CONTROLLER_HPP

#include <memory>
#include <boost/beast/http.hpp>
#include "../services/MonitorService.hpp"

namespace api {
namespace controllers {

class DashboardController {
public:
    explicit DashboardController(std::shared_ptr<services::MonitorService> service);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetDashboard(const boost::beast::http::request<boost::beast::http::string_body>& req);

private:
    std::shared_ptr<services::MonitorService> service_;
};

} // namespace controllers
} // namespace api

#endif
