#include "ConfigController.hpp"
#include <nlohmann/json.hpp>
#include <boost/beast/version.hpp>

namespace api {
namespace controllers {

ConfigController::ConfigController(std::shared_ptr<services::ConfigService> service)
    : service_(std::move(service)) {}

boost::beast::http::response<boost::beast::http::string_body>
ConfigController::makeJsonResponse(
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
ConfigController::handleGetConfig(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getConfig();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
ConfigController::handlePutConfig(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    try {
        auto body = nlohmann::json::parse(req.body());
        auto data = service_->updateConfig(body);
        return makeJsonResponse(req, data);
    } catch (const std::exception& e) {
        boost::beast::http::response<boost::beast::http::string_body> res{
            boost::beast::http::status::bad_request, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "application/json");
        res.keep_alive(req.keep_alive());
        res.body() = nlohmann::json({{"error", e.what()}}).dump();
        res.prepare_payload();
        return res;
    }
}

boost::beast::http::response<boost::beast::http::string_body>
ConfigController::handleGetThresholds(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto data = service_->getThresholds();
    return makeJsonResponse(req, data);
}

boost::beast::http::response<boost::beast::http::string_body>
ConfigController::handlePutThresholds(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    try {
        auto body = nlohmann::json::parse(req.body());
        auto data = service_->updateThresholds(body);
        return makeJsonResponse(req, data);
    } catch (const std::exception& e) {
        boost::beast::http::response<boost::beast::http::string_body> res{
            boost::beast::http::status::bad_request, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "application/json");
        res.keep_alive(req.keep_alive());
        res.body() = nlohmann::json({{"error", e.what()}}).dump();
        res.prepare_payload();
        return res;
    }
}

} // namespace controllers
} // namespace api
