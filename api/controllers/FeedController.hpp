#ifndef API_CONTROLLERS_FEED_CONTROLLER_HPP
#define API_CONTROLLERS_FEED_CONTROLLER_HPP

#include <memory>
#include <string>
#include <boost/beast/http.hpp>
#include "../../services/FeedService.hpp"
#include "../../dto/FeedStatusResponse.hpp"
#include <nlohmann/json.hpp>

namespace api {
namespace controllers {

using api::services::FeedService;

class FeedController {
public:
    explicit FeedController(std::shared_ptr<FeedService> service);

    // Handle GET /api/v1/feed/status
    boost::beast::http::response<boost::beast::http::string_body>
    handleGetStatus(const boost::beast::http::request<boost::beast::http::string_body>& req);

private:
    std::shared_ptr<FeedService> service_;
};

} // namespace controllers
} // namespace api

#endif // API_CONTROLLERS_FEED_CONTROLLER_HPP