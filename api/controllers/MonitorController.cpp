#include "MonitorController.hpp"
#include <nlohmann/json.hpp>
#include <boost/beast/version.hpp>

namespace api {
namespace controllers {

MonitorController::MonitorController(std::shared_ptr<services::MonitorService> service)
    : service_(std::move(service)) {}

std::string MonitorController::getQueryParam(
    const boost::beast::http::request<boost::beast::http::string_body>& req,
    const std::string& key)
{
    auto target = req.target();
    auto qpos = target.find('?');
    if (qpos == std::string::npos) return {};

    std::string qs = target.substr(qpos + 1);
    std::string search = key + "=";
    auto kpos = qs.find(search);
    if (kpos == std::string::npos) return {};

    auto vstart = kpos + search.size();
    auto vend = qs.find('&', vstart);
    return qs.substr(vstart, vend == std::string::npos ? vend : vend - vstart);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::makeJsonResponse(
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
MonitorController::handleGetLiveMetrics(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveMetrics();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistoryMetrics(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto interval = getQueryParam(req, "interval");
    auto data = service_->getHistoryMetrics(from, to, interval);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLivePerformance(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLivePerformance();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistoryPerformance(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistoryPerformance(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLiveThroughput(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveThroughput();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistoryThroughput(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistoryThroughput(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLiveFeed(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveFeed();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistoryFeed(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistoryFeed(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetExchanges(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getExchanges();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetExchange(
    const boost::beast::http::request<boost::beast::http::string_body>& req,
    const std::string& exchange)
{
    auto data = service_->getExchange(exchange);
    if (data.is_null()) {
        boost::beast::http::response<boost::beast::http::string_body> res{
            boost::beast::http::status::not_found, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "application/json");
        res.keep_alive(req.keep_alive());
        res.body() = R"({"error":"exchange not found"})";
        res.prepare_payload();
        return res;
    }
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetExchangeHistory(
    const boost::beast::http::request<boost::beast::http::string_body>& req,
    const std::string& exchange)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getExchangeHistory(exchange, from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLiveQueues(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveQueues();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistoryQueues(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistoryQueues(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLiveNetwork(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveNetwork();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistoryNetwork(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistoryNetwork(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLiveDatabase(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveDatabase();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistoryDatabase(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistoryDatabase(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLiveSystem(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveSystem();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistorySystem(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistorySystem(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetLiveSessions(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getLiveSessions();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetHistorySessions(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto from = getQueryParam(req, "from");
    auto to = getQueryParam(req, "to");
    auto data = service_->getHistorySessions(from, to);
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetAnalytics(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getAnalytics();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetTimeline(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getTimeline();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetDependencies(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getDependencies();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
MonitorController::handleGetTopology(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getTopology();
    return makeJsonResponse(req, data);
}

} // namespace controllers
} // namespace api
