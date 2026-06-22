#ifndef SERVER_H
#define SERVER_H

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>  
#include <boost/asio/any_io_executor.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <queue>
#include <string>
#include <vector>
#include <memory>
#include "../session/session_manager.h"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;


class http_session;
class listener;


class websocket_session : public std::enable_shared_from_this<websocket_session>
{
    websocket::stream<beast::tcp_stream> ws_;
    net::any_io_executor strand_;
    beast::flat_buffer buffer_;
    std::string current_mode_;
    std::vector<std::string> symbols_;
    std::queue<std::string> write_queue_;
    bool writing_ = false;
    std::shared_ptr<session_manager> manager_;

public:
    websocket_session(tcp::socket &&socket, std::shared_ptr<session_manager> manager);
    ~websocket_session();

    template <class Body, class Allocator>
    void do_accept(http::request<Body, http::basic_fields<Allocator>> req);

    void send_message(const std::string &msg);
    void do_write();

private:
    void on_accept(beast::error_code ec);
    void do_read();
    std::vector<std::string> extract_topics(const std::string& msg);
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void on_write(beast::error_code ec, std::size_t bytes_transferred);
};

// HTTP session class declaration
class http_session : public std::enable_shared_from_this<http_session>
{
    beast::tcp_stream stream_;
    beast::flat_buffer buffer_;
    http::request_parser<http::string_body> parser_;
    std::shared_ptr<session_manager> manager_;

public:
    http_session(tcp::socket &&socket, std::shared_ptr<session_manager> manager);
    void run();

private:
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
};

// Listener class declaration
class listener : public std::enable_shared_from_this<listener>
{
    net::io_context &ioc_;
    tcp::acceptor acceptor_;
    std::shared_ptr<session_manager> manager_;

public:
    listener(net::io_context &ioc, tcp::endpoint endpoint, std::shared_ptr<session_manager> manager);
    void run();

private:
    void do_accept();
    void on_accept(beast::error_code ec, tcp::socket socket);
};

#endif