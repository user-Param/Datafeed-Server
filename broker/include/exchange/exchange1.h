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
#include <mutex>

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
    void send_pong();
    void perform_connect();
    void start_reader();
    void log_ws_response(const std::string& context);
    
    PriceCallback callback_;
    std::atomic<bool> connected_{false};
    std::atomic<bool> running_{false};
    std::vector<std::string> symbols_;
    
    std::string sni_hostname_{"stream.binance.com"};
    std::string ws_host_{"stream.binance.com:9443"};
    std::string ws_target_{"/stream"};
    std::string ws_port_{"9443"};
    
    net::io_context ioc_;
    ssl::context ctx_{ssl::context::tlsv12_client};
    websocket::stream<ssl::stream<tcp::socket>> ws_{ioc_, ctx_};
    
    std::thread io_thread_;
    std::thread reader_thread_;
    std::mutex ws_mutex_;
};

#endif
