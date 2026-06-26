#include "SearchController.hpp"
#include <nlohmann/json.hpp>
#include <boost/beast/version.hpp>

namespace api {
namespace controllers {

SearchController::SearchController(std::shared_ptr<services::SearchService> service)
    : service_(std::move(service)) {}

std::string SearchController::getQueryParam(
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
SearchController::makeJsonResponse(
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
SearchController::handleSearch(
    const boost::beast::http::request<boost::beast::http::string_body>& req)
{
    auto q = getQueryParam(req, "q");
    auto data = service_->search(q);
    return makeJsonResponse(req, data);
}

} // namespace controllers
} // namespace api
