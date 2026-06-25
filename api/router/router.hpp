#ifndef API_ROUTER_ROUTER_HPP
#define API_ROUTER_ROUTER_HPP

#include <boost/beast/http.hpp>
#include <string>
#include <vector>
#include <functional>
#include <regex>
#include <optional>

namespace http = boost::beast::http;

namespace api {
namespace router {

struct Route {
    std::string method;
    std::string pattern;
    std::regex regex;
    std::function<http::response<http::string_body>(const http::request<http::string_body>&, const std::smatch&)> handler;
};

class Router {
public:
    void add_route(const std::string& method, const std::string& path,
                   std::function<http::response<http::string_body>(const http::request<http::string_body>&, const std::smatch&)> handler);

    std::optional<http::response<http::string_body>> handle_request(const http::request<http::string_body>& req) const;

private:
    std::vector<Route> routes_;
    std::string path_to_regex(const std::string& path) const;
};

} // namespace router
} // namespace api

#endif // API_ROUTER_ROUTER_HPP