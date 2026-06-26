#include "DashboardController.hpp"
#include <boost/beast/version.hpp>

namespace api {
namespace controllers {

DashboardController::DashboardController(std::shared_ptr<services::MonitorService> service)
    : service_(std::move(service)) {}

boost::beast::http::response<boost::beast::http::string_body>
DashboardController::handleGetDashboard(const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getDashboard();
    boost::beast::http::response<boost::beast::http::string_body> res{
        boost::beast::http::status::ok, req.version()};
    res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(boost::beast::http::field::content_type, "application/json");
    res.keep_alive(req.keep_alive());
    res.body() = data.dump();
    res.prepare_payload();
    return res;
}

} // namespace controllers
} // namespace api
