#include "router.hpp"
#include <iostream>

namespace api {
namespace router {

void Router::add_route(const std::string& method, const std::string& path,
                       std::function<http::response<http::string_body>(const http::request<http::string_body>&, const std::smatch&)> handler) {
    Route route;
    route.method = method;
    route.pattern = path;
    route.regex = std::regex(path_to_regex(path));
    route.handler = handler;
    routes_.push_back(route);
}

std::string Router::path_to_regex(const std::string& path) const {
    // Convert a path like "/api/v1/feed/status" to a regex
    // We'll support simple path parameters like :id
    std::string regex_str = "^";
    size_t pos = 0;
    while (pos < path.size()) {
        if (path[pos] == ':') {
            // Parameter
            size_t end = path.find('/', pos);
            if (end == std::string::npos) {
                end = path.size();
            }
            std::string param = path.substr(pos + 1, end - pos - 1);
            regex_str += "([^/]+)";
            pos = end;
        } else {
            if (path[pos] == '/') {
                regex_str += '/';
            } else {
                // Escape special regex characters? For simplicity, we assume no special chars.
                // But we should escape '.' etc.
                if (path[pos] == '.') {
                    regex_str += "\\.";
                } else {
                    regex_str += path[pos];
                }
            }
            pos++;
        }
    }
    regex_str += "$";
    return regex_str;
}

std::optional<http::response<http::string_body>> Router::handle_request(const http::request<http::string_body>& req) const {
    std::string target(req.target());
    std::string method(req.method_string());

    // Strip query string for route matching
    std::string path = target;
    auto qpos = path.find('?');
    if (qpos != std::string::npos) {
        path = path.substr(0, qpos);
    }

    for (const auto& route : routes_) {
        if (route.method != method) {
            continue;
        }
        std::smatch match;
        if (std::regex_match(path, match, route.regex)) {
            return route.handler(req, match);
        }
    }
    return std::nullopt;
}

} // namespace router
} // namespace api
