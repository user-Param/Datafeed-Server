#ifndef SESSION_MANAGER_H
#define SESSION_MANAGER_H

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <mutex>


class websocket_session;

class session_manager {
public:
    std::mutex mutex_;
    struct client_info {
        websocket_session* session;
        std::string mode;              
        std::string backtest_date;     
        std::vector<std::string> topics;
    };

    std::map<std::string, std::vector<websocket_session*>> topic_subscribers_;

    void add_client_topics(websocket_session* client, const std::vector<std::string>& topics);
    void remove_client_topics(websocket_session* session, const std::vector<std::string>& topics);
    std::vector<websocket_session*> get_subscribers(const std::string& topic);

    void add_client(websocket_session* session);
    void remove_client(websocket_session* session);
    void set_client_mode(websocket_session* session, const std::string& mode, const std::string& date = "");
    std::vector<websocket_session*> get_clients_by_mode(const std::string& mode);
    void broadcast_to_topic(const std::string& topic, const std::string& message); void broadcast_all(const std::string& message);

private:
    std::map<websocket_session*, client_info> clients_;
};

#endif