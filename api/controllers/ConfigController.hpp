#ifndef API_CONTROLLERS_CONFIG_CONTROLLER_HPP
#define API_CONTROLLERS_CONFIG_CONTROLLER_HPP

#include <memory>
#include <boost/beast/http.hpp>
#include "../services/ConfigService.hpp"

namespace api {
namespace controllers {

class ConfigController {
public:
    explicit ConfigController(std::shared_ptr<services::ConfigService> service);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetConfig(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handlePutConfig(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetThresholds(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handlePutThresholds(const boost::beast::http::request<boost::beast::http::string_body>& req);

private:
    boost::beast::http::response<boost::beast::http::string_body>
    makeJsonResponse(const boost::beast::http::request<boost::beast::http::string_body>& req,
                     const nlohmann::json& data);

    std::shared_ptr<services::ConfigService> service_;
};

} // namespace controllers
} // namespace api

#endif
