#include "server.h"
#include "live_source/live_source.h"
#include "backtest_source/backtest_source.h"
#include "utils/error.h"
#include "../market_data.h"

// API layer
#include "../api/router/router.hpp"
#include "../api/controllers/FeedController.hpp"
#include "../api/controllers/DashboardController.hpp"
#include "../api/controllers/MonitorController.hpp"
#include "../api/controllers/AlertsController.hpp"
#include "../api/controllers/ConfigController.hpp"
#include "../api/controllers/SearchController.hpp"
#include "../api/services/FeedService.hpp"
#include "../api/services/ClientService.hpp"
#include "../api/services/MonitorService.hpp"
#include "../api/services/AlertsService.hpp"
#include "../api/services/ConfigService.hpp"
#include "../api/services/SearchService.hpp"
#include "../sadapter.h"
#include "../metrics/metrics_collector.h"
#include "../metrics/state_manager.h"
#include "../metrics/snapshot_converter.h"
#include "../metrics/weekly_rollup.h"

#include <boost/beast/version.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>
#include <fstream>
#include <chrono>

#include <nlohmann/json.hpp>
#include <mutex>

std::shared_ptr<websocket_session> g_adapter_session;
std::mutex g_adapter_mutex;
std::shared_ptr<live_source> g_live_source;
std::shared_ptr<backtest_source> g_backtest_source;

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// ─────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────

static std::string db_connection_string() {
    const char* url = std::getenv("DATABASE_URL");
    if (url && *url) return std::string(url);

    const char* host = std::getenv("PGHOST");
    const char* port = std::getenv("PGPORT");
    const char* db   = std::getenv("PGDATABASE");
    const char* user = std::getenv("PGUSER");
    const char* pass = std::getenv("PGPASSWORD");

    std::ostringstream oss;
    oss << "host="     << (host ? host : "localhost")
        << " port="    << (port ? port : "5432")
        << " dbname="  << (db   ? db   : "datafeed")
        << " user="    << (user ? user : "datafeed")
        << " password="<< (pass ? pass : "datafeed");
    return oss.str();
}

static std::shared_ptr<api::router::Router> build_router(
    std::shared_ptr<datafeed::SAdapter> sadapter,
    StateManager& state_manager,
    MetricsCollector& collector)
{
    auto router = std::make_shared<api::router::Router>();
    auto feedService    = std::make_shared<api::services::FeedService>(sadapter);
    auto feedController = std::make_shared<api::controllers::FeedController>(feedService);

    router->add_route("GET", "/api/v1/feed/status",
        [feedController](const http::request<http::string_body>& req,
                         const std::smatch&) {
            return feedController->handleGetStatus(req);
        });

    // ── Client routes ────────────────────────────────────
    auto clientService = std::make_shared<api::services::ClientService>(sadapter);

    router->add_route("GET", "/api/v1/clients",
        [clientService](const http::request<http::string_body>& req,
                        const std::smatch&) {
            auto clients = clientService->getClients();
            nlohmann::json arr = nlohmann::json::array();
            for (const auto& c : clients) {
                nlohmann::json j;
                j["tenant_id"]   = c.tenant_id;
                j["client_name"] = c.client_name;
                j["plan"]        = c.plan;
                j["status"]      = c.status;
                j["created_at"]  = c.created_at;
                j["updated_at"]  = c.updated_at;
                arr.push_back(j);
            }
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "application/json");
            res.keep_alive(req.keep_alive());
            res.body() = arr.dump();
            res.prepare_payload();
            return res;
        });

    router->add_route("GET", "/api/v1/clients/:id",
        [clientService](const http::request<http::string_body>& req,
                        const std::smatch& match) {
            std::string id = match[1].str();
            auto client = clientService->getClientById(id);
            if (!client) {
                http::response<http::string_body> res{http::status::not_found, req.version()};
                res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
                res.set(http::field::content_type, "application/json");
                res.keep_alive(req.keep_alive());
                res.body() = R"({"error":"client not found"})";
                res.prepare_payload();
                return res;
            }
            nlohmann::json j;
            j["tenant_id"]   = client->tenant_id;
            j["client_name"] = client->client_name;
            j["plan"]        = client->plan;
            j["status"]      = client->status;
            j["created_at"]  = client->created_at;
            j["updated_at"]  = client->updated_at;
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "application/json");
            res.keep_alive(req.keep_alive());
            res.body() = j.dump();
            res.prepare_payload();
            return res;
        });

    router->add_route("POST", "/api/v1/clients",
        [clientService](const http::request<http::string_body>& req,
                        const std::smatch&) {
            try {
                auto body = nlohmann::json::parse(req.body());
                api::dto::ClientResponseDto dto;
                dto.client_name = body.value("client_name", "");
                dto.plan        = body.value("plan", "free");
                dto.status      = body.value("status", "active");
                if (body.contains("auth_subject")) dto.auth_subject = body["auth_subject"].get<std::string>();
                if (body.contains("ip_address"))   dto.ip_address   = body["ip_address"].get<std::string>();
                if (body.contains("user_agent"))   dto.user_agent   = body["user_agent"].get<std::string>();
                auto created = clientService->createClient(dto);
                if (!created) {
                    http::response<http::string_body> res{http::status::internal_server_error, req.version()};
                    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
                    res.set(http::field::content_type, "application/json");
                    res.keep_alive(req.keep_alive());
                    res.body() = R"({"error":"failed to create client"})";
                    res.prepare_payload();
                    return res;
                }
                nlohmann::json j;
                j["tenant_id"]   = created->tenant_id;
                j["client_name"] = created->client_name;
                j["plan"]        = created->plan;
                j["status"]      = created->status;
                j["created_at"]  = created->created_at;
                j["updated_at"]  = created->updated_at;
                http::response<http::string_body> res{http::status::created, req.version()};
                res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
                res.set(http::field::content_type, "application/json");
                res.keep_alive(req.keep_alive());
                res.body() = j.dump();
                res.prepare_payload();
                return res;
            } catch (const std::exception& e) {
                http::response<http::string_body> res{http::status::bad_request, req.version()};
                res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
                res.set(http::field::content_type, "application/json");
                res.keep_alive(req.keep_alive());
                nlohmann::json err = {{"error", e.what()}};
                res.body() = err.dump();
                res.prepare_payload();
                return res;
            }
        });

    router->add_route("DELETE", "/api/v1/clients/:id",
        [clientService](const http::request<http::string_body>& req,
                        const std::smatch& match) {
            std::string id = match[1].str();
            bool ok = clientService->deleteClient(id);
            http::status status = ok ? http::status::no_content : http::status::not_found;
            http::response<http::string_body> res{status, req.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "application/json");
            res.keep_alive(req.keep_alive());
            res.body() = ok ? "" : R"({"error":"client not found"})";
            res.prepare_payload();
            return res;
        });

    // ── Monitoring Services & Controllers ────────────────
    auto monitorService = std::make_shared<api::services::MonitorService>(
        sadapter, state_manager, collector);
    auto monitorController = std::make_shared<api::controllers::MonitorController>(monitorService);
    auto dashboardController = std::make_shared<api::controllers::DashboardController>(monitorService);

    // Dashboard
    router->add_route("GET", "/api/v1/dashboard",
        [dashboardController](const http::request<http::string_body>& req, const std::smatch&) {
            return dashboardController->handleGetDashboard(req);
        });

    // ── Metrics ──────────────────────────────────────────
    router->add_route("GET", "/api/v1/metrics/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveMetrics(req);
        });
    router->add_route("GET", "/api/v1/metrics/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistoryMetrics(req);
        });

    // ── Performance ──────────────────────────────────────
    router->add_route("GET", "/api/v1/performance/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLivePerformance(req);
        });
    router->add_route("GET", "/api/v1/performance/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistoryPerformance(req);
        });

    // ── Throughput ────────────────────────────────────────
    router->add_route("GET", "/api/v1/throughput/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveThroughput(req);
        });
    router->add_route("GET", "/api/v1/throughput/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistoryThroughput(req);
        });

    // ── Feed (live/history) ───────────────────────────────
    router->add_route("GET", "/api/v1/feed/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveFeed(req);
        });
    router->add_route("GET", "/api/v1/feed/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistoryFeed(req);
        });

    // ── Exchanges ─────────────────────────────────────────
    router->add_route("GET", "/api/v1/exchanges",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetExchanges(req);
        });
    router->add_route("GET", "/api/v1/exchange/:exchange",
        [monitorController](const http::request<http::string_body>& req, const std::smatch& match) {
            return monitorController->handleGetExchange(req, match[1].str());
        });
    router->add_route("GET", "/api/v1/exchange/:exchange/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch& match) {
            return monitorController->handleGetExchangeHistory(req, match[1].str());
        });

    // ── Queues ────────────────────────────────────────────
    router->add_route("GET", "/api/v1/queues/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveQueues(req);
        });
    router->add_route("GET", "/api/v1/queues/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistoryQueues(req);
        });

    // ── Network ───────────────────────────────────────────
    router->add_route("GET", "/api/v1/network/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveNetwork(req);
        });
    router->add_route("GET", "/api/v1/network/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistoryNetwork(req);
        });

    // ── Database ──────────────────────────────────────────
    router->add_route("GET", "/api/v1/database/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveDatabase(req);
        });
    router->add_route("GET", "/api/v1/database/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistoryDatabase(req);
        });

    // ── System ────────────────────────────────────────────
    router->add_route("GET", "/api/v1/system/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveSystem(req);
        });
    router->add_route("GET", "/api/v1/system/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistorySystem(req);
        });

    // ── Sessions ──────────────────────────────────────────
    router->add_route("GET", "/api/v1/sessions/live",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetLiveSessions(req);
        });
    router->add_route("GET", "/api/v1/sessions/history",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetHistorySessions(req);
        });

    // ── Analytics ─────────────────────────────────────────
    router->add_route("GET", "/api/v1/analytics",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetAnalytics(req);
        });

    // ── Timeline ──────────────────────────────────────────
    router->add_route("GET", "/api/v1/timeline",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetTimeline(req);
        });

    // ── Dependencies ──────────────────────────────────────
    router->add_route("GET", "/api/v1/dependencies",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetDependencies(req);
        });

    // ── Topology ──────────────────────────────────────────
    router->add_route("GET", "/api/v1/topology",
        [monitorController](const http::request<http::string_body>& req, const std::smatch&) {
            return monitorController->handleGetTopology(req);
        });

    // ── Alerts ────────────────────────────────────────────
    auto alertsService = std::make_shared<api::services::AlertsService>(sadapter);
    auto alertsController = std::make_shared<api::controllers::AlertsController>(alertsService);

    router->add_route("GET", "/api/v1/alerts",
        [alertsController](const http::request<http::string_body>& req, const std::smatch&) {
            return alertsController->handleGetAlerts(req);
        });
    router->add_route("POST", "/api/v1/alerts/:id/ack",
        [alertsController](const http::request<http::string_body>& req, const std::smatch& match) {
            return alertsController->handleAcknowledgeAlert(req, match[1].str());
        });
    router->add_route("GET", "/api/v1/alerts/history",
        [alertsController](const http::request<http::string_body>& req, const std::smatch&) {
            return alertsController->handleGetAlertHistory(req);
        });

    // ── Audit ─────────────────────────────────────────────
    router->add_route("GET", "/api/v1/audit",
        [alertsController](const http::request<http::string_body>& req, const std::smatch&) {
            return alertsController->handleGetAudit(req);
        });

    // ── Config ────────────────────────────────────────────
    auto configService = std::make_shared<api::services::ConfigService>(sadapter);
    auto configController = std::make_shared<api::controllers::ConfigController>(configService);

    router->add_route("GET", "/api/v1/config",
        [configController](const http::request<http::string_body>& req, const std::smatch&) {
            return configController->handleGetConfig(req);
        });
    router->add_route("PUT", "/api/v1/config",
        [configController](const http::request<http::string_body>& req, const std::smatch&) {
            return configController->handlePutConfig(req);
        });
    router->add_route("GET", "/api/v1/thresholds",
        [configController](const http::request<http::string_body>& req, const std::smatch&) {
            return configController->handleGetThresholds(req);
        });
    router->add_route("PUT", "/api/v1/thresholds",
        [configController](const http::request<http::string_body>& req, const std::smatch&) {
            return configController->handlePutThresholds(req);
        });

    // ── Search ────────────────────────────────────────────
    auto searchService = std::make_shared<api::services::SearchService>(sadapter);
    auto searchController = std::make_shared<api::controllers::SearchController>(searchService);

    router->add_route("GET", "/api/v1/search",
        [searchController](const http::request<http::string_body>& req, const std::smatch&) {
            return searchController->handleSearch(req);
        });

    // ── Health check ─────────────────────────────────────
    router->add_route("GET", "/health",
        [sadapter](const http::request<http::string_body>& req, const std::smatch&) {
            const bool db_ok = sadapter && sadapter->is_connected();
            auto instances = db_ok ? sadapter->get_feed_instances_by_condition("") : std::vector<datafeed::FeedInstance>{};
            nlohmann::json j;
            j["status"] = db_ok ? "ok" : "degraded";
            j["db"] = db_ok ? "connected" : "disconnected";
            j["feed_instances"] = instances.size();
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "application/json");
            res.keep_alive(req.keep_alive());
            res.body() = j.dump();
            res.prepare_payload();
            return res;
        });

    return router;
}

// ─────────────────────────────────────────────────────────
// websocket_session
// ─────────────────────────────────────────────────────────

websocket_session::websocket_session(tcp::socket &&socket, std::shared_ptr<session_manager> manager)
    : ws_(std::move(socket)), strand_(net::make_strand(static_cast<net::io_context &>(ws_.get_executor().context()))), manager_(manager)
{
    manager_->add_client(this);
}

websocket_session::~websocket_session()
{
    std::lock_guard<std::mutex> lock(g_adapter_mutex);
    if (this == g_adapter_session.get())
        g_adapter_session.reset();
    manager_->remove_client(this);
}

void websocket_session::send_message(const std::string &msg)
{
    net::post(strand_, [self = shared_from_this(), msg]()
              {
        self->write_queue_.push(msg);
        if (!self->writing_)
            self->do_write(); });
}

void websocket_session::do_write()
{
    if (write_queue_.empty())
    {
        writing_ = false;
        return;
    }

    writing_ = true;
    ws_.text(true);
    ws_.async_write(
        net::buffer(write_queue_.front()),
        net::bind_executor(strand_, beast::bind_front_handler(&websocket_session::on_write, shared_from_this())));
}

template <class Body, class Allocator>
void websocket_session::do_accept(http::request<Body, http::basic_fields<Allocator>> req)
{
    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));

    ws_.async_accept(
        req,
        net::bind_executor(strand_, beast::bind_front_handler(&websocket_session::on_accept, shared_from_this())));
}

void websocket_session::on_accept(beast::error_code ec)
{
    if (ec)
    {
        std::cerr << "[Server] WebSocket accept error: " << ec.message() << std::endl;
        return;
    }
    std::cout << "[Server] Client Connected" << std::endl;
    do_read();
}

void websocket_session::do_read()
{
    ws_.async_read(
        buffer_,
        net::bind_executor(strand_, beast::bind_front_handler(&websocket_session::on_read, shared_from_this())));
}

std::vector<std::string> websocket_session::extract_topics(const std::string &msg)
{
    std::vector<std::string> topics;

    // Market data topics
    if (msg.find("ticker") != std::string::npos)
        topics.push_back("ticker_");
    if (msg.find("price") != std::string::npos)
        topics.push_back("price_");
    if (msg.find("bid") != std::string::npos)
        topics.push_back("bid_");
    if (msg.find("ask") != std::string::npos)
        topics.push_back("ask_");

    // Monitoring topics
    if (msg.find("dashboard") != std::string::npos)
        topics.push_back("dashboard");
    if (msg.find("metrics") != std::string::npos)
        topics.push_back("metrics");
    if (msg.find("performance") != std::string::npos)
        topics.push_back("performance");
    if (msg.find("exchange") != std::string::npos)
        topics.push_back("exchange");
    if (msg.find("system") != std::string::npos)
        topics.push_back("system");
    if (msg.find("network") != std::string::npos)
        topics.push_back("network");
    if (msg.find("feed") != std::string::npos)
        topics.push_back("feed");
    if (msg.find("queues") != std::string::npos)
        topics.push_back("queues");

    // "all" subscribes to everything
    if (msg.find("all") != std::string::npos)
    {
        topics.push_back("ticker_");
        topics.push_back("price_");
        topics.push_back("bid_");
        topics.push_back("ask_");
        topics.push_back("dashboard");
        topics.push_back("metrics");
        topics.push_back("performance");
        topics.push_back("exchange");
        topics.push_back("system");
        topics.push_back("network");
        topics.push_back("feed");
        topics.push_back("queues");
    }
    return topics;
}

void websocket_session::on_read(beast::error_code ec, std::size_t bytes_transferred)
{
    boost::ignore_unused(bytes_transferred);

    if (ec == websocket::error::closed)
    {
        std::cout << "Client disconnected normally" << std::endl;
        return;
    }
    if (ec)
    {
        std::cout << "Read error: " << ec.message() << std::endl;
        return fail(ec, "read");
    }

    std::string msg = beast::buffers_to_string(buffer_.data());

    try
    {
        auto j = nlohmann::json::parse(msg);
        if (j.contains("type") && j["type"] == "adapter")
        {
            std::lock_guard<std::mutex> lock(g_adapter_mutex);
            g_adapter_session = shared_from_this();
            buffer_.consume(buffer_.size());
            do_read();
            return;
        }

        // Messages from the adapter proxy session — broadcast to market topics
        {
            std::lock_guard<std::mutex> lock(g_adapter_mutex);
            if (g_adapter_session && this == g_adapter_session.get())
            {
                manager_->broadcast_to_topic("price_", msg);
                manager_->broadcast_to_topic("backtest_price_", msg);
                manager_->broadcast_to_topic("bid_", msg);
                manager_->broadcast_to_topic("backtest_bid_", msg);
                manager_->broadcast_to_topic("ask_", msg);
                manager_->broadcast_to_topic("backtest_ask_", msg);

                if (j.contains("topic") && j["topic"] == "backtest_complete") {
                    manager_->broadcast_to_topic("backtest_complete", msg);
                }

                buffer_.consume(buffer_.size());
                do_read();
                return;
            }
        }

        if (j.contains("type")) {
            if (j["type"] == "backtest_result") {
                manager_->broadcast_all(msg);
                buffer_.consume(buffer_.size());
                do_read();
                return;
            }
            if (j["type"] == "switch_exchange") {
                std::string ex = j.value("exchange", "BINANCE");
                std::cout << "[Server] Switching exchange to " << ex << std::endl;
                if (g_live_source) {
                    ExchangeType newType = (ex == "BIRDEYE") ? ExchangeType::BIRDEYE :
                                          (ex == "JUPITER") ? ExchangeType::JUPITER : ExchangeType::BINANCE;
                    std::vector<std::string> syms;
                    if (j.contains("symbols") && j["symbols"].is_array() && !j["symbols"].empty()) {
                        for (auto& s : j["symbols"]) syms.push_back(s.get<std::string>());
                    } else if (newType == ExchangeType::BINANCE) {
                        syms = {"BTCUSDT", "ETHUSDT", "SOLUSDT"};
                    } else {
                        syms = {"BTC", "ETH", "SOL"};
                    }
                    g_live_source->switch_exchange(newType, syms);
                }
                buffer_.consume(buffer_.size());
                do_read();
                return;
            }
        }
    }
    catch (...)
    {
        // Not JSON — fall through to plain text handling
    }

    if (msg.find("_Live") != std::string::npos)
    {
        nlohmann::json cmd = {{"command", "stop"}};
        current_mode_ = "_Live";
        manager_->set_client_mode(this, "_Live");
        std::lock_guard<std::mutex> lock(g_adapter_mutex);
        if (g_adapter_session)
            g_adapter_session->send_message(cmd.dump());
    }
    else if (msg.find("_Backtest") != std::string::npos)
    {
        nlohmann::json cmd = {{"command", "start"}};
        current_mode_ = "_Backtest";
        std::string date_range = "2024-01-01,2024-01-01";
        auto date_pos = msg.find("20");
        if (date_pos != std::string::npos && msg.size() >= date_pos + 10) {
            date_range = msg.substr(date_pos, 10);
        }
        manager_->set_client_mode(this, "_Backtest", date_range);
        std::lock_guard<std::mutex> lock(g_adapter_mutex);
        if (g_adapter_session)
            g_adapter_session->send_message(cmd.dump());
        if (g_backtest_source) {
            g_backtest_source->start_replay(date_range);
        }
    }

    if (msg.find("subscribe") != std::string::npos)
    {
        std::vector<std::string> topics = extract_topics(msg);
        manager_->add_client_topics(this, topics);
    }

    buffer_.consume(buffer_.size());
    do_read();
}

void websocket_session::on_write(beast::error_code ec, std::size_t bytes_transferred)
{
    boost::ignore_unused(bytes_transferred);

    if (ec)
    {
        return fail(ec, "write");
    }

    write_queue_.pop();
    do_write();
}

// ─────────────────────────────────────────────────────────
// http_session
// ─────────────────────────────────────────────────────────

http_session::http_session(tcp::socket &&socket,
                           std::shared_ptr<session_manager> manager,
                           std::shared_ptr<api::router::Router> router)
    : stream_(std::move(socket)), manager_(manager), router_(std::move(router))
{
}

void http_session::run()
{
    net::dispatch(
        stream_.get_executor(),
        beast::bind_front_handler(
            &http_session::do_read,
            this->shared_from_this()));
}

void http_session::do_read()
{
    parser_.body_limit(10000);
    stream_.expires_after(std::chrono::seconds(30));

    http::async_read(
        stream_,
        buffer_,
        parser_,
        beast::bind_front_handler(
            &http_session::on_read,
            shared_from_this()));
}

http::response<http::string_body> http_session::make_not_found(unsigned version)
{
    http::response<http::string_body> res{http::status::not_found, version};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, "application/json");
    res.body() = R"({"error":"not found"})";
    res.prepare_payload();
    return res;
}

void http_session::on_read(beast::error_code ec, std::size_t bytes_transferred)
{
    boost::ignore_unused(bytes_transferred);

    if (ec == http::error::end_of_stream)
    {
        stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
        return;
    }

    if (ec)
    {
        return fail(ec, "read");
    }

    // WebSocket upgrade takes priority
    if (websocket::is_upgrade(parser_.get()))
    {
        std::make_shared<websocket_session>(
            stream_.release_socket(), manager_)
            ->do_accept(parser_.release());
        return;
    }

    // Route HTTP request through the API router
    auto req = parser_.release();
    if (router_) {
        auto resp = router_->handle_request(req);
        if (resp) {
            auto resp_ptr = std::make_shared<http::response<http::string_body>>(std::move(*resp));
            resp_ptr->set(http::field::access_control_allow_origin, "*");
            http::async_write(
                stream_,
                *resp_ptr,
                beast::bind_front_handler(
                    [self = shared_from_this(), resp_ptr](beast::error_code ec2, std::size_t) {
                        boost::ignore_unused(resp_ptr);
                        if (ec2) fail(ec2, "write");
                        self->stream_.socket().shutdown(tcp::socket::shutdown_send, ec2);
                    }));
            return;
        }
    }

    // No route matched
    auto resp_ptr = std::make_shared<http::response<http::string_body>>(make_not_found(req.version()));
    resp_ptr->set(http::field::access_control_allow_origin, "*");
    http::async_write(
        stream_,
        *resp_ptr,
        beast::bind_front_handler(
            [self = shared_from_this(), resp_ptr](beast::error_code ec2, std::size_t) {
                boost::ignore_unused(resp_ptr);
                if (ec2) fail(ec2, "write");
                self->stream_.socket().shutdown(tcp::socket::shutdown_send, ec2);
            }));
}

// ─────────────────────────────────────────────────────────
// listener
// ─────────────────────────────────────────────────────────

listener::listener(
    net::io_context &ioc,
    tcp::endpoint endpoint,
    std::shared_ptr<session_manager> manager,
    std::shared_ptr<api::router::Router> router)
    : ioc_(ioc), acceptor_(net::make_strand(ioc)), manager_(manager), router_(std::move(router))
{
    beast::error_code ec;

    acceptor_.open(endpoint.protocol(), ec);
    if (ec) { fail(ec, "open"); return; }

    acceptor_.set_option(net::socket_base::reuse_address(true), ec);
    if (ec) { fail(ec, "set_option"); return; }

    acceptor_.bind(endpoint, ec);
    if (ec) { fail(ec, "bind"); return; }

    acceptor_.listen(net::socket_base::max_listen_connections, ec);
    if (ec) { fail(ec, "listen"); return; }
}

void listener::run()
{
    net::dispatch(
        acceptor_.get_executor(),
        beast::bind_front_handler(
            &listener::do_accept,
            this->shared_from_this()));
}

void listener::do_accept()
{
    socket_ = std::make_shared<tcp::socket>(net::make_strand(ioc_));
    acceptor_.async_accept(
        *socket_,
        beast::bind_front_handler(
            &listener::on_accept,
            shared_from_this()));
}

void listener::on_accept(beast::error_code ec)
{
    if (ec)
    {
        std::cerr << "[Listener] Accept error: " << ec.message() << " (" << ec.value() << ")" << std::endl;
        if (ec == net::error::invalid_argument || ec == net::error::bad_descriptor)
        {
            std::cerr << "Fatal accept error, stopping listener: " << ec.message() << std::endl;
            return;
        }
        fail(ec, "accept");
        do_accept();
    }
    else
    {
        std::cout << "[Listener] Accepted connection" << std::endl;
        std::make_shared<http_session>(std::move(*socket_), manager_, router_)->run();
        do_accept();
    }
}

// ─────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────

int main(int argc, char *argv[])
{
    std::cout << "Datafeed Server Starting..." << std::endl;

    if (argc != 4)
    {
        std::cerr << "Usage: datafeed <address> <port> <threads>\n"
                  << "  e.g. datafeed 0.0.0.0 4444 4\n";
        return EXIT_FAILURE;
    }
    auto const address = net::ip::make_address(argv[1]);
    auto const port    = static_cast<unsigned short>(std::atoi(argv[2]));
    auto const threads = std::max<int>(1, std::atoi(argv[3]));

    // ── Database (SAdapter) ───────────────────────────────
    auto sadapter = std::make_shared<datafeed::SAdapter>(db_connection_string());
    const bool db_connected = sadapter->connect();
    if (db_connected) {
        std::cout << "[Server] Database connected." << std::endl;
    } else {
        std::string conn_info = db_connection_string();
        // Log connection details without password for diagnostics
        {
            std::string safe = conn_info;
            auto pass_pos = safe.find("password=");
            if (pass_pos != std::string::npos) {
                auto end_pos = safe.find(' ', pass_pos);
                safe.replace(pass_pos, (end_pos == std::string::npos ? safe.size() : end_pos) - pass_pos, "password=****");
            }
            std::cerr << "[Server] WARNING: Database connection failed.\n"
                      << "[Server]   Connection string (sanitized): " << safe << "\n";
        }
        std::cerr << "[Server]   Check DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD "
                     "(or DATABASE_URL / PG*) environment variables.\n";
    }

    // #region agent log
    // Disabled hardcoded debug log for deployment compatibility
    // {
    //     std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
    //     dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"A\",\"location\":\"server.cpp:main\","
    //         << "\"message\":\"startup db connection\",\"data\":{\"connected\":" << (db_connected ? "true" : "false")
    //         << "},\"timestamp\":"
    //         << std::chrono::duration_cast<std::chrono::milliseconds>(
    //                std::chrono::system_clock::now().time_since_epoch()).count()
    //         << "}\n";
    // }
    // #endregion

    // ── Metrics Collector, State Manager & Weekly Rollup ──
    MetricsCollector collector;
    StateManager state_manager(collector);
    std::unique_ptr<WeeklyRollup> weekly_rollup;

    // StateManager always starts — it drives the live snapshot for REST/WS endpoints.
    // The summary callback (15-min persistence) is only registered when DB is available.
    if (db_connected) {
        state_manager.setSummaryCallback([sadapter](const FeedMetricsSnapshot& summary) {
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            auto db_snapshot = snapshot_converter::to_db_snapshot(summary, "live-1", now);
            if (!sadapter->create_feed_metrics_snapshot(db_snapshot)) {
                std::cerr << "[Monitor] Failed to persist 15-min summary" << std::endl;
                return;
            }

            for (const auto& entry : snapshot_converter::to_exchange_entries(summary, "live-1", now)) {
                sadapter->create_exchange_metrics_entry(entry);
            }

            sadapter->create_queue_entry(
                snapshot_converter::to_queue_entry(summary, "live-1", now));

            sadapter->create_system_metrics_entry(
                snapshot_converter::to_system_entry(summary, "live-1", now));

            sadapter->create_network_metrics_entry(
                snapshot_converter::to_network_entry(summary, "live-1", now));

            sadapter->create_database_metrics_entry(
                snapshot_converter::to_database_entry(summary, "live-1", now));
        });

        std::cout << "[Server] Summary callback registered (15-min persistence)." << std::endl;
    }

    state_manager.start();
    std::cout << "[Server] StateManager started (live snapshot every 1s)." << std::endl;

    if (db_connected) {
        weekly_rollup = std::make_unique<WeeklyRollup>(sadapter, "live-1");
        weekly_rollup->start();
        std::cout << "[Server] WeeklyRollup started." << std::endl;
    }

    // ── API Router ────────────────────────────────────────
    auto router = build_router(sadapter, state_manager, collector);

    // ── Session manager & data sources ───────────────────
    net::io_context ioc{threads};

    auto manager  = std::make_shared<session_manager>();
    auto live      = std::make_shared<live_source>(manager);
    auto backtest  = std::make_shared<backtest_source>(manager);

    live->set_db_adapter(sadapter, "live-1");
    live->set_collector(&collector);
    backtest->set_db_adapter(sadapter, "backtest-1");

    g_live_source = live;
    g_backtest_source = backtest;
    live->start();

    // ── WebSocket Monitoring Broadcast ────────────────────
    std::function<void()> start_monitor_broadcast;
    auto monitor_timer = std::make_shared<net::steady_timer>(ioc);
    auto pipeline_log_counter = std::make_shared<int>(0);
    start_monitor_broadcast = [&start_monitor_broadcast, monitor_timer, manager, &state_manager, pipeline_log_counter]() {
        monitor_timer->expires_after(std::chrono::seconds(1));
        monitor_timer->async_wait([&start_monitor_broadcast, monitor_timer, manager, &state_manager, pipeline_log_counter](beast::error_code ec) {
            if (ec) return;
            auto s = state_manager.getLiveSnapshot();

            // Pipeline stage log every 5 seconds
            (*pipeline_log_counter)++;
            if (*pipeline_log_counter % 5 == 0) {
                std::cout << "[Pipeline] tick=" << *pipeline_log_counter
                          << " msgs/sec=" << s.messages_received_per_sec
                          << " ticks/sec=" << s.ticks_per_sec
                          << " pkt/sec=" << s.packets_received_per_sec
                          << " trades/sec=" << s.trades_per_sec
                          << " total_msgs=" << (s.total_messages_received + s.total_messages_sent)
                          << " total_ticks=" << s.total_ticks
                          << " health=" << s.feed_health_score
                          << " sessions=" << s.active_sessions
                          << " subs=" << s.active_subscriptions
                          << " exchanges=" << s.exchange_stats.size()
                          << " cpu=" << s.cpu_usage_percent << "%"
                          << " mem=" << (s.memory_rss / 1024 / 1024) << "MB"
                          << std::endl;

                // Log callback invocation counts from collector
                if (s.total_messages_received == 0 && s.total_ticks == 0) {
                    std::cout << "[Pipeline] WARNING: No market data flowing. "
                              << "Check exchange connection and feed registration." << std::endl;
                }
            }

            nlohmann::json dash;
            dash["type"] = "dashboard";
            dash["cpu_usage"] = s.cpu_usage_percent;
            dash["memory_rss"] = s.memory_rss;
            dash["thread_count"] = s.thread_count;
            dash["uptime_seconds"] = s.uptime_seconds;
            dash["health_score"] = s.feed_health_score;
            manager->broadcast_to_topic("dashboard", dash.dump());

            nlohmann::json live_metrics;
            live_metrics["type"] = "metrics";
            live_metrics["cpu"] = s.cpu_usage_percent;
            live_metrics["memory"] = s.memory_rss;
            live_metrics["threads"] = s.thread_count;
            live_metrics["uptime"] = s.uptime_seconds;
            manager->broadcast_to_topic("metrics", live_metrics.dump());

            nlohmann::json live_perf;
            live_perf["type"] = "performance";
            for (const auto& [cat, stats] : s.latency_stats) {
                live_perf[std::to_string(static_cast<int>(cat))] = {
                    {"avg", stats.average}, {"p50", stats.p50},
                    {"p95", stats.p95}, {"p99", stats.p99}, {"count", stats.count}
                };
            }
            manager->broadcast_to_topic("performance", live_perf.dump());

            nlohmann::json live_ex;
            live_ex["type"] = "exchange";
            for (const auto& [name, es] : s.exchange_stats) {
                nlohmann::json e;
                e["connected"] = es.connected;
                e["uptime_seconds"] = es.uptime_seconds;
                e["latency_ms"] = es.exchange_latency_ms;
                e["messages_received"] = es.messages_received;
                live_ex[name] = e;
            }
            manager->broadcast_to_topic("exchange", live_ex.dump());

            nlohmann::json live_sys;
            live_sys["type"] = "system";
            live_sys["cpu"] = s.cpu_usage_percent;
            live_sys["memory"] = s.memory_rss;
            live_sys["peak_rss"] = s.peak_rss;
            live_sys["threads"] = s.thread_count;
            live_sys["uptime"] = s.uptime_seconds;
            manager->broadcast_to_topic("system", live_sys.dump());

            nlohmann::json live_net;
            live_net["type"] = "network";
            live_net["bandwidth_bps"] = s.network_bandwidth_bps;
            live_net["socket_rtt_ms"] = s.socket_rtt_ms;
            manager->broadcast_to_topic("network", live_net.dump());

            nlohmann::json live_feed;
            live_feed["type"] = "feed";
            live_feed["health_score"] = s.feed_health_score;
            live_feed["packet_drops"] = s.packet_drops;
            live_feed["stale"] = s.stale_feed;
            manager->broadcast_to_topic("feed", live_feed.dump());

            nlohmann::json live_queues;
            live_queues["type"] = "queues";
            live_queues["incoming"] = s.incoming_queue_depth;
            live_queues["outgoing"] = s.outgoing_queue_depth;
            live_queues["overflow"] = s.queue_overflow_count;
            live_queues["backpressure"] = s.queue_backpressure;
            manager->broadcast_to_topic("queues", live_queues.dump());

            start_monitor_broadcast();
        });
    };
    start_monitor_broadcast();

    std::make_shared<listener>(ioc, tcp::endpoint{address, port}, manager, router)->run();

    std::cout << "[Server] Listening on " << address << ":" << port
              << " (" << threads << " threads)" << std::endl;

    net::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](beast::error_code const &, int) {
        std::cout << "\n[Server] Shutdown signal received." << std::endl;
        ioc.stop();
    });

    std::vector<std::thread> v;
    v.reserve(threads - 1);
    for (auto i = threads - 1; i > 0; --i)
        v.emplace_back([&ioc] { ioc.run(); });
    ioc.run();

    for (auto &t : v)
        t.join();

    state_manager.stop();
    if (weekly_rollup) weekly_rollup->stop();
    if (g_live_source) g_live_source->stop();
    if (g_backtest_source) g_backtest_source->stop_replay();
    sadapter->disconnect();
    std::cout << "[Server] Stopped." << std::endl;

    return EXIT_SUCCESS;
}
