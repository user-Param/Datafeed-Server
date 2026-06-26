#ifndef API_CONTROLLERS_MONITOR_CONTROLLER_HPP
#define API_CONTROLLERS_MONITOR_CONTROLLER_HPP

#include <memory>
#include <boost/beast/http.hpp>
#include "../services/MonitorService.hpp"

namespace api {
namespace controllers {

class MonitorController {
public:
    explicit MonitorController(std::shared_ptr<services::MonitorService> service);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveMetrics(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistoryMetrics(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLivePerformance(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistoryPerformance(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveThroughput(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistoryThroughput(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveFeed(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistoryFeed(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetExchanges(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetExchange(const boost::beast::http::request<boost::beast::http::string_body>& req,
                      const std::string& exchange);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetExchangeHistory(const boost::beast::http::request<boost::beast::http::string_body>& req,
                             const std::string& exchange);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveQueues(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistoryQueues(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveNetwork(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistoryNetwork(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveDatabase(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistoryDatabase(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveSystem(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistorySystem(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetLiveSessions(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetHistorySessions(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetAnalytics(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetTimeline(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetDependencies(const boost::beast::http::request<boost::beast::http::string_body>& req);

    boost::beast::http::response<boost::beast::http::string_body>
    handleGetTopology(const boost::beast::http::request<boost::beast::http::string_body>& req);

private:
    static std::string getQueryParam(const boost::beast::http::request<boost::beast::http::string_body>& req,
                                     const std::string& key);
    static boost::beast::http::response<boost::beast::http::string_body>
    makeJsonResponse(const boost::beast::http::request<boost::beast::http::string_body>& req,
                     const nlohmann::json& data);

    std::shared_ptr<services::MonitorService> service_;
};

} // namespace controllers
} // namespace api

#endif
