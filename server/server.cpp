#include "server.h"
#include "live_source/live_source.h"
#include "backtest_source/backtest_source.h"
#include "utils/error.h"
#include "../market_data.h"

#include <boost/beast/version.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/signal_set.hpp>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>
#include <mutex>

std::shared_ptr<websocket_session> g_adapter_session;
std::mutex g_adapter_mutex;
std::shared_ptr<live_source> g_live_source; // Global to access eadapter

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

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
        std::cerr << "[Server] WebSocket accept error: " << ec.message() << " (code " << ec.value() << ")" << std::endl;
        return; // Do not call fail() if it might terminate
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

    //std::cout << "[SERVER] Received from Dadapter: " << msg << std::endl;

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

        // If this message comes from the adapter session, broadcast it
        {
            std::lock_guard<std::mutex> lock(g_adapter_mutex);
            if (g_adapter_session && this == g_adapter_session.get())
            {
                // Broadcast to all market topics
                manager_->broadcast_to_topic("price_", msg);
                manager_->broadcast_to_topic("backtest_price_", msg);
                manager_->broadcast_to_topic("bid_", msg);
                manager_->broadcast_to_topic("backtest_bid_", msg);
                manager_->broadcast_to_topic("ask_", msg);
                manager_->broadcast_to_topic("backtest_ask_", msg);

                // Forward backtest_complete to its own topic so engine receives it
                if (j.contains("topic") && j["topic"] == "backtest_complete") {
                    manager_->broadcast_to_topic("backtest_complete", msg);
                }

                buffer_.consume(buffer_.size());
                do_read();
                return;
            }
        }

        // If it's a typed message from a non-adapter client, handle it
        if (j.contains("type")) {
            // Forward backtest_result from engine to all frontend clients
            if (j["type"] == "backtest_result") {
                manager_->broadcast_all(msg);
                buffer_.consume(buffer_.size());
                do_read();
                return;
            }
            if (j["type"] == "switch_exchange") {
    std::string ex = j["exchange"];
    std::cout << "[Server] Switching exchange to " << ex << std::endl;
    if (g_live_source) {
        ExchangeType newType = (ex == "BIRDEYE") ? ExchangeType::BIRDEYE : 
                               (ex == "JUPITER") ? ExchangeType::JUPITER : ExchangeType::BINANCE;
        std::vector<std::string> syms;
        // Use symbols from message if provided, otherwise fall back to defaults
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
        // Not JSON or parse error – normal client message
    }

    if (msg.find("_Live") != std::string::npos)
    {

        nlohmann::json cmd = {{"command", "stop"}};
        current_mode_ = "_Live";
        manager_->set_client_mode(this, "_Live");
        // send_message("Switched to Live mode");
        std::lock_guard<std::mutex> lock(g_adapter_mutex);
        if (g_adapter_session)
            g_adapter_session->send_message(cmd.dump());
    }
    else if (msg.find("_Backtest") != std::string::npos)
    {

        nlohmann::json cmd = {{"command", "start"}};
        current_mode_ = "_Backtest";
        manager_->set_client_mode(this, "_Backtest");
        // send_message("Switched to Backtest mode");
        std::lock_guard<std::mutex> lock(g_adapter_mutex);
        if (g_adapter_session)
            g_adapter_session->send_message(cmd.dump());
    }

    if (msg.find("subscribe") != std::string::npos)
    {
        std::vector<std::string> topics = extract_topics(msg);
        manager_->add_client_topics(this, topics);
        // send_message("subscribed to topics");
    }

    buffer_.consume(buffer_.size());
    do_read();
}

void websocket_session::on_write(beast::error_code ec, std::size_t bytes_transferred)
{
    boost::ignore_unused(bytes_transferred);

    if (ec)
    {
        // std::cout << "Write error: " << ec.message() << std::endl;
        return fail(ec, "write");
    }

    //std::cout << "Message sent (" << bytes_transferred << " bytes)" << std::endl;

    write_queue_.pop();
    do_write();
}

http_session::http_session(tcp::socket &&socket, std::shared_ptr<session_manager> manager)
    : stream_(std::move(socket)), manager_(manager)
{
    // std::cout << "New HTTP session created" << std::endl;
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

void http_session::on_read(beast::error_code ec, std::size_t bytes_transferred)
{
    boost::ignore_unused(bytes_transferred);

    if (ec == http::error::end_of_stream)
    {
        // std::cout << "HTTP session ended" << std::endl;
        stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
        return;
    }

    if (ec)
    {
        // std::cout << "HTTP read error: " << ec.message() << std::endl;
        return fail(ec, "read");
    }

    if (websocket::is_upgrade(parser_.get()))
    {
        // std::cout << "HTTP upgrade to WebSocket detected" << std::endl;
        std::make_shared<websocket_session>(
            stream_.release_socket(), manager_)
            ->do_accept(parser_.release());
        return;
    }
}

listener::listener(
    net::io_context &ioc,
    tcp::endpoint endpoint,
    std::shared_ptr<session_manager> manager)
    : ioc_(ioc), acceptor_(net::make_strand(ioc)), manager_(manager)
{
    beast::error_code ec;

    acceptor_.open(endpoint.protocol(), ec);
    if (ec)
    {
        fail(ec, "open");
        return;
    }

    acceptor_.set_option(net::socket_base::reuse_address(true), ec);
    if (ec)
    {
        fail(ec, "set_option");
        return;
    }

    acceptor_.bind(endpoint, ec);
    if (ec)
    {
        fail(ec, "bind");
        return;
    }

    acceptor_.listen(net::socket_base::max_listen_connections, ec);
    if (ec)
    {
        fail(ec, "listen");
        return;
    }

    // std::cout << "Server listening on " << endpoint.address() << ":" << endpoint.port() << std::endl;
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
        // Fatal errors: stop retrying
        if (ec == net::error::invalid_argument || ec == net::error::bad_descriptor)
        {
            std::cerr << "Fatal accept error, stopping listener: " << ec.message() << std::endl;
            return; // Do NOT call do_accept() again
        }
        fail(ec, "accept");
        // For recoverable errors, still retry
        do_accept();
    }
    else
    {
        std::make_shared<http_session>(std::move(socket), manager_)->run();
        do_accept(); // continue accepting
    }
}

int main(int argc, char *argv[])
{

    std::cout << "Blink Started" << std::endl;

    if (argc != 4)
    {
        std::cerr << "Usage: advanced-server <address> <port> <threads>\n";
        return EXIT_FAILURE;
    }
    auto const address = net::ip::make_address(argv[1]);
    auto const port = static_cast<unsigned short>(std::atoi(argv[2]));
    auto const threads = std::max<int>(1, std::atoi(argv[3]));

    // std::cout << "Address: " << address << std::endl;
    // std::cout << "Port: " << port << std::endl;
    // std::cout << "Threads: " << threads << std::endl;

    net::io_context ioc{threads};

    auto manager = std::make_shared<session_manager>();
    auto live = std::make_shared<live_source>(manager);
    auto backtest = std::make_shared<backtest_source>(manager);

    g_live_source = live; // Initialize global
    live->start();

    std::make_shared<listener>(ioc, tcp::endpoint{address, port}, manager)->run();

    net::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](beast::error_code const &, int)
                       { std::cout << "\n Shutdown signal received" << std::endl; 
                        ioc.stop(); });

    std::vector<std::thread> v;
    v.reserve(threads - 1);
    for (auto i = threads - 1; i > 0; --i)
        v.emplace_back([&ioc]
                       { ioc.run(); });
    ioc.run();

    for (auto &t : v)
        t.join();

    // std::cout << "Server Stopped" << std::endl;

    return EXIT_SUCCESS;
}
