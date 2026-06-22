#include "session_manager.h"
#include "../server/server.h"
#include <algorithm>
#include <iostream>

void session_manager::add_client(websocket_session* session) {
    std::lock_guard<std::mutex> lock(mutex_);
    client_info info;
    info.session = session;
    info.mode = "unknown";
    clients_[session] = info;
}

void session_manager::remove_client(websocket_session* session) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(session);
    if (it != clients_.end()) {
        // Remove from all topics the client was subscribed to
        for (const auto& topic : it->second.topics) {
            auto& subscribers = topic_subscribers_[topic];
            subscribers.erase(
                std::remove(subscribers.begin(), subscribers.end(), session),
                subscribers.end()
            );
        }
        
        // Also ensure it's removed from some hardcoded topics if "all_" was used
        std::vector<std::string> extra_topics = {"price_", "bid_", "ask_", "quantity_", "time_"};
        for (const auto& topic : extra_topics) {
            auto& subscribers = topic_subscribers_[topic];
            subscribers.erase(
                std::remove(subscribers.begin(), subscribers.end(), session),
                subscribers.end()
            );
        }
        
        clients_.erase(it);
    }
}

void session_manager::set_client_mode(websocket_session* session, 
                                      const std::string& mode, 
                                      const std::string& date) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(session);
    if (it != clients_.end()) {
        it->second.mode = mode;
        it->second.backtest_date = date;
    }
}

std::vector<websocket_session*> session_manager::get_clients_by_mode(const std::string& mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<websocket_session*> result;
    for (auto& [session, info] : clients_) {
        if (info.mode == mode) {
            result.push_back(session);
        }
    }
    return result;
}

void session_manager::add_client_topics(websocket_session* session, const std::vector<std::string>& topics){
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(session);
    if (it == clients_.end()) return;

    // Merge new topics with existing ones
    for (const auto& topic : topics) {
        if (std::find(it->second.topics.begin(), it->second.topics.end(), topic) == it->second.topics.end()) {
            it->second.topics.push_back(topic);
        }

        if (topic == "all_") {
            std::vector<std::string> all_topics = {"price_", "bid_", "ask_", "quantity_", "time_"};
            for (const auto& t : all_topics) {
                auto& subscribers = topic_subscribers_[t];
                if (std::find(subscribers.begin(), subscribers.end(), session) == subscribers.end()) {
                    subscribers.push_back(session);
                }
            }
        } else {
            auto& subscribers = topic_subscribers_[topic];
            if (std::find(subscribers.begin(), subscribers.end(), session) == subscribers.end()) {
                subscribers.push_back(session);
            }
        }
    }
}

void session_manager::remove_client_topics(websocket_session* session, const std::vector<std::string>& topics) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(session);
    if (it == clients_.end()) return;

    for (const auto& topic : topics) {
        // Remove from client's topic list
        it->second.topics.erase(
            std::remove(it->second.topics.begin(), it->second.topics.end(), topic),
            it->second.topics.end()
        );

        // Remove from topic subscribers map
        auto& subscribers = topic_subscribers_[topic];
        subscribers.erase(
            std::remove(subscribers.begin(), subscribers.end(), session),
            subscribers.end()
        );
    }
}

void session_manager::broadcast_to_topic(const std::string& topic, const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topic_subscribers_.find(topic);
    if (it != topic_subscribers_.end()) {
        for (auto session : it->second) {
            if (session) {
                session->send_message(message);
            }
        }
    }
}

std::vector<websocket_session*> session_manager::get_subscribers(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topic_subscribers_.find(topic);
    if (it != topic_subscribers_.end()) {
        return it->second;
    }
    return {};
}

void session_manager::broadcast_all(const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [session, info] : clients_) {
        if (session) {
            session->send_message(message);
        }
    }
}
