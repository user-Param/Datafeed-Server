#include "server.h"
#include "live_source/live_source.h"
#include "backtest_source/backtest_source.h"
#include "utils/error.h"
#include "../market_data.h"

// API layer
#include "../api/router/router.hpp"
#include "../api/controllers/FeedController.hpp"
#include "../api/services/FeedService.hpp"
#include "../api/services/ClientService.hpp"
#include "../sadapter.h"

#include <boost/beast/version.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/signal_set.hpp>
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
    std::shared_ptr<datafeed::SAdapter> sadapter)
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

    // GET /api/v1/clients  — list all
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

    // GET /api/v1/clients/:id
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

    // POST /api/v1/clients
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

    // DELETE /api/v1/clients/:id
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

    // ── Health check ─────────────────────────────────────
    router->add_route("GET", "/health",
        [sadapter](const http::request<http::string_body>& req, const std::smatch&) {
            const bool db_ok = sadapter && sadapter->is_connected();
            auto instances = db_ok ? sadapter->get_feed_instances_by_condition("") : std::vector<datafeed::FeedInstance>{};
            nlohmann::json j;
            j["status"] = db_ok ? "ok" : "degraded";
            j["db"] = db_ok ? "connected" : "disconnected";
            j["feed_instances"] = instances.size();
            http::response<http::string_body> res{
                db_ok ? http::status::ok : http::status::service_unavailable, req.version()};
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
    if (msg.find("ticker") != std::string::npos)
        topics.push_back("ticker_");
    if (msg.find("price") != std::string::npos)
        topics.push_back("price_");
    if (msg.find("bid") != std::string::npos)
        topics.push_back("bid_");
    if (msg.find("ask") != std::string::npos)
        topics.push_back("ask_");
    if (msg.find("all") != std::string::npos)
    {
        topics.push_back("ticker_");
        topics.push_back("price_");
        topics.push_back("bid_");
        topics.push_back("ask_");
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
    acceptor_.async_accept(
        net::make_strand(ioc_),
        beast::bind_front_handler(
            &listener::on_accept,
            shared_from_this()));
}

void listener::on_accept(beast::error_code ec, tcp::socket socket)
{
    if (ec)
    {
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
        std::make_shared<http_session>(std::move(socket), manager_, router_)->run();
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
        std::cerr << "[Server] WARNING: Database connection failed. "
                     "API endpoints that require DB will return errors.\n";
    }

    // #region agent log
    {
        std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
        dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"A\",\"location\":\"server.cpp:main\","
            << "\"message\":\"startup db connection\",\"data\":{\"connected\":" << (db_connected ? "true" : "false")
            << "},\"timestamp\":"
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()).count()
            << "}\n";
    }
    // #endregion

    // ── API Router ────────────────────────────────────────
    auto router = build_router(sadapter);

    // ── Session manager & data sources ───────────────────
    net::io_context ioc{threads};

    auto manager  = std::make_shared<session_manager>();
    auto live      = std::make_shared<live_source>(manager);
    auto backtest  = std::make_shared<backtest_source>(manager);

    live->set_db_adapter(sadapter, "live-1");
    backtest->set_db_adapter(sadapter, "backtest-1");

    g_live_source = live;
    g_backtest_source = backtest;
    live->start();

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

    if (g_live_source) g_live_source->stop();
    if (g_backtest_source) g_backtest_source->stop_replay();
    sadapter->disconnect();
    std::cout << "[Server] Stopped." << std::endl;

    return EXIT_SUCCESS;
}
