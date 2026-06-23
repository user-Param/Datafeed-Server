#ifndef EXCHANGE1_H
#define EXCHANGE1_H

#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <thread>
#include <atomic>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <nlohmann/json.hpp>

using PriceCallback = std::function<void(const std::string& symbol, double price, double bid, double ask, long timestamp)>;

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

class Exchange1 {
public:
    Exchange1();
    ~Exchange1();
    
    void connect();                             
    void subscribe(const std::vector<std::string>& symbols);  
    void set_callback(PriceCallback callback);  
    
private:
    void run_io_context();
    void read_loop();
    void send_subscribe(const std::vector<std::string>& symbols);
    
    PriceCallback callback_;
    std::atomic<bool> connected_{false};
    std::atomic<bool> running_{false};
    
    net::io_context ioc_;
    ssl::context ctx_{ssl::context::tlsv12_client};
    websocket::stream<ssl::stream<tcp::socket>> ws_{ioc_, ctx_};
    
    std::thread io_thread_;
    std::thread reader_thread_;
};

#endif