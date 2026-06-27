#include "FeedController.hpp"
#include <boost/beast/version.hpp>
#include <boost/beast/http.hpp>
#include <iostream>
#include <nlohmann/json.hpp>

namespace api {
namespace controllers {

FeedController::FeedController(std::shared_ptr<api::services::FeedService> service)
    : service_(std::move(service))
{
}

boost::beast::http::response<boost::beast::http::string_body>
FeedController::handleGetStatus(const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    (void)req; // unused for now, but we could check method etc.
    auto status = service_->getFeedStatus();
    if (!status) {
        boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::internal_server_error, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "application/json");
        res.keep_alive(req.keep_alive());
        nlohmann::json j = {{"error", "Unable to get feed status"}};
        res.body() = j.dump();
        res.prepare_payload();
        return res;
    }

    boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::ok, req.version()};
    res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(boost::beast::http::field::content_type, "application/json");
    res.keep_alive(req.keep_alive());
    nlohmann::json j = {
        {"instance_id", status->instance_id},
        {"exchange", status->exchange},
        {"status", status->status},
        {"uptime", status->uptime}
    };
    if (status->status == "degraded") {
        j["database"] = "disconnected";
        j["feed_instances"] = 0;
        j["reason"] = "database unavailable";
    }
    res.body() = j.dump();
    res.prepare_payload();
    return res;
}

} // namespace controllers
} // namespace api