#include "AlertsController.hpp"
#include <nlohmann/json.hpp>
#include <boost/beast/version.hpp>

namespace api {
namespace controllers {

AlertsController::AlertsController(std::shared_ptr<services::AlertsService> service)
    : service_(std::move(service)) {}

std::string AlertsController::getQueryParam(
    const boost::beast::http::request<boost::beast::http::string_body>& req,
    const std::string& key)
{
    auto target = req.target();
    auto qpos = target.find('?');
    if (qpos == std::string::npos) return {};
    auto qsv = target.substr(qpos + 1);
    std::string qs(qsv.data(), qsv.size());
    std::string search = key + "=";
    auto kpos = qs.find(search);
    if (kpos == std::string::npos) return {};
    auto vstart = kpos + search.size();
    auto vend = qs.find('&', vstart);
    return qs.substr(vstart, vend == std::string::npos ? vend : vend - vstart);
}

boost::beast::http::response<boost::beast::http::string_body>
AlertsController::makeJsonResponse(
    const boost::beast::http::request<boost::beast::http::string_body>& req,
    const nlohmann::json& data)
{
    boost::beast::http::response<boost::beast::http::string_body> res{
        boost::beast::http::status::ok, req.version()};
    res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(boost::beast::http::field::content_type, "application/json");
    res.keep_alive(req.keep_alive());
    res.body() = data.dump();
    res.prepare_payload();
    return res;
}

boost::beast::http::response<boost::beast::http::string_body>
AlertsController::handleGetAlerts(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto status = getQueryParam(req, "status");
    auto data = service_->getAlerts(status);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
AlertsController::handleAcknowledgeAlert(
    const boost::beast::http::request<boost::beast::http::string_body>& req,
    const std::string& alert_id)
{
    auto data = service_->acknowledgeAlert(alert_id);
    if (data.is_null()) {
        boost::beast::http::response<boost::beast::http::string_body> res{
            boost::beast::http::status::not_found, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "application/json");
        res.keep_alive(req.keep_alive());
        res.body() = R"({"error":"alert not found"})";
        res.prepare_payload();
        return res;
    }
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
AlertsController::handleGetAlertHistory(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getAlertHistory();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
AlertsController::handleGetAudit(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto actor = getQueryParam(req, "actor");
    auto action = getQueryParam(req, "action");
    auto exchange = getQueryParam(req, "exchange");
    auto client = getQueryParam(req, "client");
    auto data = service_->getAudit(from, to, actor, action, exchange, client);
    return makeJsonResponse(req, data);
}

} // namespace controllers
} // namespace api
