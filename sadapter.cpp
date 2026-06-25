#include "sadapter.h"
#include <pqxx/pqxx>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <optional>

namespace datafeed {

// Helper function implementations

std::string SAdapter::timestamp_to_string(uint64_t ms) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::gmtime(&time_t);
    int milliseconds = ms % 1000;
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << milliseconds;
    return oss.str();
}

uint64_t SAdapter::string_to_timestamp(const std::string& str) {
    // Expected format: "YYYY-MM-DD HH:MM:SS.fff"
    std::tm tm = {};
    std::istringstream iss(str);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (iss.fail()) {
        throw std::runtime_error("Failed to parse timestamp: " + str);
    }
    std::chrono::time_point<std::chrono::system_clock> tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    // Extract milliseconds
    std::string fraction;
    char dot;
    if (iss >> dot && dot == '.' && iss >> fraction) {
        if (fraction.size() > 3) {
            fraction = fraction.substr(0, 3);
        }
        while (fraction.size() < 3) {
            fraction += '0';
        }
        int ms = std::stoi(fraction);
        auto duration = tp.time_since_epoch();
        auto ms_duration = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        ms_duration += std::chrono::milliseconds(ms);
        return ms_duration.count();
    }
    // If no fraction, assume 0 milliseconds
    auto duration = tp.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

std::string SAdapter::interval_to_string(int64_t seconds) {
    return std::to_string(seconds) + " seconds";
}

int64_t SAdapter::string_to_interval(const std::string& str) {
    // Expected format: "<number> seconds" or similar that starts with a number
    std::istringstream iss(str);
    int64_t seconds;
    iss >> seconds;
    // Ignore the rest
    return seconds;
}

// SAdapter implementation

SAdapter::SAdapter(const std::string& connection_string)
    : connection_string_(connection_string), conn_(nullptr) {}

SAdapter::~SAdapter() {
    disconnect();
}

bool SAdapter::connect() {
    try {
        conn_ = std::make_unique<pqxx::connection>(connection_string_);
        return conn_->is_open();
    } catch (const std::exception& e) {
        std::cerr << "Database connection failed: " << e.what() << std::endl;
        return false;
    }
}

void SAdapter::disconnect() {
    if (conn_ && conn_->is_open()) {
        conn_->close();
    }
}

bool SAdapter::is_connected() const {
    return conn_ && conn_->is_open();
}

// Client operations

std::optional<Client> SAdapter::create_client(const Client& client) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::params params;
        params.append(client.client_name);
        params.append(client.plan);
        params.append(client.status);
        if (client.auth_subject) {
            params.append(*client.auth_subject);
        } else {
            params.append();
        }
        if (client.ip_address) {
            params.append(*client.ip_address);
        } else {
            params.append();
        }
        if (client.user_agent) {
            params.append(*client.user_agent);
        } else {
            params.append();
        }
        pqxx::result r = txn.exec_params(
            "INSERT INTO clients (client_name, plan, status, auth_subject, ip_address, user_agent) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "RETURNING tenant_id, created_at, updated_at",
            params
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        Client created = client;
        created.tenant_id = r[0]["tenant_id"].as<std::string>();
        created.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        created.updated_at = string_to_timestamp(r[0]["updated_at"].as<std::string>());
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating client: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<Client> SAdapter::get_client_by_id(const std::string& tenant_id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM clients WHERE tenant_id = $1",
            tenant_id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        Client client;
        client.tenant_id = r[0]["tenant_id"].as<std::string>();
        client.client_name = r[0]["client_name"].as<std::string>();
        client.plan = r[0]["plan"].as<std::string>();
        client.status = r[0]["status"].as<std::string>();
        client.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        client.updated_at = string_to_timestamp(r[0]["updated_at"].as<std::string>());
        if (!r[0]["last_seen_at"].is_null()) {
            client.last_seen_at = string_to_timestamp(r[0]["last_seen_at"].as<std::string>());
        }
        client.auth_subject = r[0]["auth_subject"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["auth_subject"].as<std::string>());
        client.ip_address = r[0]["ip_address"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["ip_address"].as<std::string>());
        client.user_agent = r[0]["user_agent"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["user_agent"].as<std::string>());
        return client;
    } catch (const std::exception& e) {
        std::cerr << "Error getting client by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<Client> SAdapter::get_clients_by_condition(const std::string& where_clause) {
    std::vector<Client> clients;
    if (!is_connected()) {
        return clients;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM clients";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            Client client;
            client.tenant_id = row["tenant_id"].as<std::string>();
            client.client_name = row["client_name"].as<std::string>();
            client.plan = row["plan"].as<std::string>();
            client.status = row["status"].as<std::string>();
            client.created_at = string_to_timestamp(row["created_at"].as<std::string>());
            client.updated_at = string_to_timestamp(row["updated_at"].as<std::string>());
            if (!row["last_seen_at"].is_null()) {
                client.last_seen_at = string_to_timestamp(row["last_seen_at"].as<std::string>());
            }
            client.auth_subject = row["auth_subject"].is_null() ? std::nullopt : std::optional<std::string>(row["auth_subject"].as<std::string>());
            client.ip_address = row["ip_address"].is_null() ? std::nullopt : std::optional<std::string>(row["ip_address"].as<std::string>());
            client.user_agent = row["user_agent"].is_null() ? std::nullopt : std::optional<std::string>(row["user_agent"].as<std::string>());
            clients.push_back(client);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting clients by condition: " << e.what() << std::endl;
    }
    return clients;
}

bool SAdapter::update_client(const Client& client) {
    if (!is_connected() || client.tenant_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::params params;
        params.append(client.client_name);
        params.append(client.plan);
        params.append(client.status);
        if (client.auth_subject) {
            params.append(*client.auth_subject);
        } else {
            params.append();
        }
        if (client.ip_address) {
            params.append(*client.ip_address);
        } else {
            params.append();
        }
        if (client.user_agent) {
            params.append(*client.user_agent);
        } else {
            params.append();
        }
        params.append(client.tenant_id);

        pqxx::result r = txn.exec_params(
            "UPDATE clients SET "
            "client_name = $1, plan = $2, status = $3, auth_subject = $4, ip_address = $5, user_agent = $6, updated_at = NOW() "
            "WHERE tenant_id = $7 "
            "RETURNING updated_at",
            params
        );
        txn.commit();
        return !r.empty();
    } catch (const std::exception& e) {
        std::cerr << "Error updating client: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_client(const std::string& tenant_id) {
    if (!is_connected() || tenant_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM clients WHERE tenant_id = $1",
            tenant_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting client: " << e.what() << std::endl;
        return false;
    }
}

// Session operations

std::optional<Session> SAdapter::create_session(const Session& session) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO sessions (connection_id, connected_at, disconnected_at, disconnect_reason, auth_status, reconnect_count, heartbeat_interval, protocol, instance_id, tenant_id) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            "RETURNING session_id",
            session.connection_id ? *session.connection_id : nullptr,
            timestamp_to_string(session.connected_at),
            session.disconnected_at ? timestamp_to_string(*session.disconnected_at) : nullptr,
            session.disconnect_reason ? *session.disconnect_reason : nullptr,
            session.auth_status ? *session.auth_status : nullptr,
            session.reconnect_count ? *session.reconnect_count : nullptr,
            session.heartbeat_interval ? interval_to_string(*session.heartbeat_interval) : nullptr,
            session.protocol,
            session.instance_id,
            session.tenant_id ? *session.tenant_id : nullptr
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        Session created = session;
        created.session_id = r[0]["session_id"].as<std::string>();
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating session: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<Session> SAdapter::get_session_by_id(const std::string& session_id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM sessions WHERE session_id = $1",
            session_id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        Session session;
        session.session_id = r[0]["session_id"].as<std::string>();
        session.connection_id = r[0]["connection_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["connection_id"].as<std::string>());
        session.connected_at = string_to_timestamp(r[0]["connected_at"].as<std::string>());
        if (!r[0]["disconnected_at"].is_null()) {
            session.disconnected_at = string_to_timestamp(r[0]["disconnected_at"].as<std::string>());
        }
        session.disconnect_reason = r[0]["disconnect_reason"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["disconnect_reason"].as<std::string>());
        session.auth_status = r[0]["auth_status"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["auth_status"].as<std::string>());
        session.reconnect_count = r[0]["reconnect_count"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["reconnect_count"].as<int64_t>());
        if (!r[0]["heartbeat_interval"].is_null()) {
            session.heartbeat_interval = string_to_interval(r[0]["heartbeat_interval"].as<std::string>());
        }
        session.protocol = r[0]["protocol"].as<std::string>();
        session.instance_id = r[0]["instance_id"].as<std::string>();
        session.tenant_id = r[0]["tenant_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["tenant_id"].as<std::string>());
        return session;
    } catch (const std::exception& e) {
        std::cerr << "Error getting session by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<Session> SAdapter::get_sessions_by_condition(const std::string& where_clause) {
    std::vector<Session> sessions;
    if (!is_connected()) {
        return sessions;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM sessions";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            Session session;
            session.session_id = row["session_id"].as<std::string>();
            session.connection_id = row["connection_id"].is_null() ? std::nullopt : std::optional<std::string>(row["connection_id"].as<std::string>());
            session.connected_at = string_to_timestamp(row["connected_at"].as<std::string>());
            if (!row["disconnected_at"].is_null()) {
                session.disconnected_at = string_to_timestamp(row["disconnected_at"].as<std::string>());
            }
            session.disconnect_reason = row["disconnect_reason"].is_null() ? std::nullopt : std::optional<std::string>(row["disconnect_reason"].as<std::string>());
            session.auth_status = row["auth_status"].is_null() ? std::nullopt : std::optional<std::string>(row["auth_status"].as<std::string>());
            session.reconnect_count = row["reconnect_count"].is_null() ? std::nullopt : std::optional<int64_t>(row["reconnect_count"].as<int64_t>());
            if (!row["heartbeat_interval"].is_null()) {
                session.heartbeat_interval = string_to_interval(row["heartbeat_interval"].as<std::string>());
            }
            session.protocol = row["protocol"].as<std::string>();
            session.instance_id = row["instance_id"].as<std::string>();
            session.tenant_id = row["tenant_id"].is_null() ? std::nullopt : std::optional<std::string>(row["tenant_id"].as<std::string>());
            sessions.push_back(session);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting sessions by condition: " << e.what() << std::endl;
    }
    return sessions;
}

bool SAdapter::update_session(const Session& session) {
    if (!is_connected() || session.session_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE sessions SET "
            "connection_id = $1, connected_at = $2, disconnected_at = $3, disconnect_reason = $4, auth_status = $5, reconnect_count = $6, heartbeat_interval = $7, protocol = $8, instance_id = $9, tenant_id = $10 "
            "WHERE session_id = $11",
            session.connection_id ? *session.connection_id : nullptr,
            timestamp_to_string(session.connected_at),
            session.disconnected_at ? timestamp_to_string(*session.disconnected_at) : nullptr,
            session.disconnect_reason ? *session.disconnect_reason : nullptr,
            session.auth_status ? *session.auth_status : nullptr,
            session.reconnect_count ? *session.reconnect_count : nullptr,
            session.heartbeat_interval ? interval_to_string(*session.heartbeat_interval) : nullptr,
            session.protocol,
            session.instance_id,
            session.tenant_id ? *session.tenant_id : nullptr,
            session.session_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating session: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_session(const std::string& session_id) {
    if (!is_connected() || session_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM sessions WHERE session_id = $1",
            session_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting session: " << e.what() << std::endl;
        return false;
    }
}

// Subscription operations

std::optional<Subscription> SAdapter::create_subscription(const Subscription& subscription) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO subscriptions (symbol, topic, stream_type, mode, filters_json, priority, tenant_id, session_id) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
            "RETURNING subscription_id, created_at",
            subscription.symbol,
            subscription.topic,
            subscription.stream_type ? *subscription.stream_type : pqxx::null,
            subscription.mode ? *subscription.mode : pqxx::null,
            subscription.filters_json ? *subscription.filters_json : pqxx::null,
            subscription.priority ? *subscription.priority : pqxx::null,
            subscription.tenant_id ? *subscription.tenant_id : pqxx::null,
            subscription.session_id ? *subscription.session_id : pqxx::null
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        Subscription created = subscription;
        created.subscription_id = r[0]["subscription_id"].as<std::string>();
        created.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating subscription: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<Subscription> SAdapter::get_subscription_by_id(const std::string& subscription_id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM subscriptions WHERE subscription_id = $1",
            subscription_id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        Subscription subscription;
        subscription.subscription_id = r[0]["subscription_id"].as<std::string>();
        subscription.symbol = r[0]["symbol"].as<std::string>();
        subscription.topic = r[0]["topic"].as<std::string>();
        subscription.stream_type = r[0]["stream_type"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["stream_type"].as<std::string>());
        subscription.mode = r[0]["mode"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["mode"].as<std::string>());
        subscription.filters_json = r[0]["filters_json"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["filters_json"].as<std::string>());
        subscription.priority = r[0]["priority"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["priority"].as<int64_t>());
        subscription.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        if (!r[0]["removed_at"].is_null()) {
            subscription.removed_at = string_to_timestamp(r[0]["removed_at"].as<std::string>());
        }
        subscription.is_active = r[0]["is_active"].as<bool>();
        subscription.tenant_id = r[0]["tenant_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["tenant_id"].as<std::string>());
        subscription.session_id = r[0]["session_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["session_id"].as<std::string>());
        return subscription;
    } catch (const std::exception& e) {
        std::cerr << "Error getting subscription by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<Subscription> SAdapter::get_subscriptions_by_condition(const std::string& where_clause) {
    std::vector<Subscription> subscriptions;
    if (!is_connected()) {
        return subscriptions;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM subscriptions";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            Subscription subscription;
            subscription.subscription_id = row["subscription_id"].as<std::string>();
            subscription.symbol = row["symbol"].as<std::string>();
            subscription.topic = row["topic"].as<std::string>();
            subscription.stream_type = row["stream_type"].is_null() ? std::nullopt : std::optional<std::string>(row["stream_type"].as<std::string>());
            subscription.mode = row["mode"].is_null() ? std::nullopt : std::optional<std::string>(row["mode"].as<std::string>());
            subscription.filters_json = row["filters_json"].is_null() ? std::nullopt : std::optional<std::string>(row["filters_json"].as<std::string>());
            subscription.priority = row["priority"].is_null() ? std::nullopt : std::optional<int64_t>(row["priority"].as<int64_t>());
            subscription.created_at = string_to_timestamp(row["created_at"].as<std::string>());
            if (!row["removed_at"].is_null()) {
                subscription.removed_at = string_to_timestamp(row["removed_at"].as<std::string>());
            }
            subscription.is_active = row["is_active"].as<bool>();
            subscription.tenant_id = row["tenant_id"].is_null() ? std::nullopt : std::optional<std::string>(row["tenant_id"].as<std::string>());
            subscription.session_id = row["session_id"].is_null() ? std::nullopt : std::optional<std::string>(row["session_id"].as<std::string>());
            subscriptions.push_back(subscription);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting subscriptions by condition: " << e.what() << std::endl;
    }
    return subscriptions;
}

bool SAdapter::update_subscription(const Subscription& subscription) {
    if (!is_connected() || subscription.subscription_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE subscriptions SET "
            "symbol = $1, topic = $2, stream_type = $3, mode = $4, filters_json = $5, priority = $6, tenant_id = $7, session_id = $8, is_active = $9, removed_at = $10 "
            "WHERE subscription_id = $11 "
            "RETURNING removed_at",
            subscription.symbol,
            subscription.topic,
            subscription.stream_type ? *subscription.stream_type : pqxx::null,
            subscription.mode ? *subscription.mode : pqxx::null,
            subscription.filters_json ? *subscription.filters_json : pqxx::null,
            subscription.priority ? *subscription.priority : pqxx::null,
            subscription.tenant_id ? *subscription.tenant_id : pqxx::null,
            subscription.session_id ? *subscription.session_id : pqxx::null,
            subscription.is_active,
            subscription.removed_at ? timestamp_to_string(*subscription.removed_at) : pqxx::null,
            subscription.subscription_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating subscription: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_subscription(const std::string& subscription_id) {
    if (!is_connected() || subscription_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM subscriptions WHERE subscription_id = $1",
            subscription_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting subscription: " << e.what() << std::endl;
        return false;
    }
}

// FeedInstance operations

std::optional<FeedInstance> SAdapter::create_feed_instance(const FeedInstance& instance) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO feed_instances (instance_id, exchange, adapter_type, feed_status, last_tick_at, stale_seconds, reconnect_attempts, message_rate_in, message_rate_out, queue_depth, backpressure_active, serialization_ms, parse_error_count, gap_count, duplicate_count, out_of_order_count, tenant_id) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) "
            "RETURNING instance_id",
            instance.instance_id,
            instance.exchange,
            instance.adapter_type ? *instance.adapter_type : pqxx::null,
            instance.feed_status,
            instance.last_tick_at ? timestamp_to_string(*instance.last_tick_at) : pqxx::null,
            instance.stale_seconds ? interval_to_string(*instance.stale_seconds) : pqxx::null,
            instance.reconnect_attempts,
            instance.message_rate_in ? *instance.message_rate_in : pqxx::null,
            instance.message_rate_out ? *instance.message_rate_out : pqxx::null,
            instance.queue_depth ? *instance.queue_depth : pqxx::null,
            instance.backpressure_active ? *instance.backpressure_active : pqxx::null,
            instance.serialization_ms ? *instance.serialization_ms : pqxx::null,
            instance.parse_error_count ? *instance.parse_error_count : pqxx::null,
            instance.gap_count ? *instance.gap_count : pqxx::null,
            instance.duplicate_count ? *instance.duplicate_count : pqxx::null,
            instance.out_of_order_count ? *instance.out_of_order_count : pqxx::null,
            instance.tenant_id ? *instance.tenant_id : pqxx::null
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        FeedInstance created = instance;
        // instance_id is already set (provided by caller, not generated by DB in this case?
        // But note: in the schema, instance_id is VARCHAR(50) PRIMARY KEY, so we expect the caller to provide it.
        // We don't return it from the DB because it's the input.
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating feed instance: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<FeedInstance> SAdapter::get_feed_instance_by_id(const std::string& instance_id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM feed_instances WHERE instance_id = $1",
            instance_id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        FeedInstance instance;
        instance.instance_id = r[0]["instance_id"].as<std::string>();
        instance.exchange = r[0]["exchange"].as<std::string>();
        instance.adapter_type = r[0]["adapter_type"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["adapter_type"].as<std::string>());
        instance.feed_status = r[0]["feed_status"].as<std::string>();
        if (!r[0]["last_tick_at"].is_null()) {
            instance.last_tick_at = string_to_timestamp(r[0]["last_tick_at"].as<std::string>());
        }
        instance.stale_seconds = r[0]["stale_seconds"].is_null() ? std::nullopt : std::optional<int64_t>(string_to_interval(r[0]["stale_seconds"].as<std::string>()));
        instance.reconnect_attempts = r[0]["reconnect_attempts"].as<int64_t>();
        instance.message_rate_in = r[0]["message_rate_in"].is_null() ? std::nullopt : std::optional<double>(r[0]["message_rate_in"].as<double>());
        instance.message_rate_out = r[0]["message_rate_out"].is_null() ? std::nullopt : std::optional<double>(r[0]["message_rate_out"].as<double>());
        instance.queue_depth = r[0]["queue_depth"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["queue_depth"].as<int64_t>());
        instance.backpressure_active = r[0]["backpressure_active"].is_null() ? std::nullopt : std::optional<bool>(r[0]["backpressure_active"].as<bool>());
        instance.serialization_ms = r[0]["serialization_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["serialization_ms"].as<double>());
        instance.parse_error_count = r[0]["parse_error_count"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["parse_error_count"].as<int64_t>());
        instance.gap_count = r[0]["gap_count"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["gap_count"].as<int64_t>());
        instance.duplicate_count = r[0]["duplicate_count"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["duplicate_count"].as<int64_t>());
        instance.out_of_order_count = r[0]["out_of_order_count"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["out_of_order_count"].as<int64_t>());
        instance.tenant_id = r[0]["tenant_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["tenant_id"].as<std::string>());
        return instance;
    } catch (const std::exception& e) {
        std::cerr << "Error getting feed instance by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<FeedInstance> SAdapter::get_feed_instances_by_condition(const std::string& where_clause) {
    std::vector<FeedInstance> instances;
    if (!is_connected()) {
        return instances;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM feed_instances";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            FeedInstance instance;
            instance.instance_id = row["instance_id"].as<std::string>();
            instance.exchange = row["exchange"].as<std::string>();
            instance.adapter_type = row["adapter_type"].is_null() ? std::nullopt : std::optional<std::string>(row["adapter_type"].as<std::string>());
            instance.feed_status = row["feed_status"].as<std::string>();
            if (!row["last_tick_at"].is_null()) {
                instance.last_tick_at = string_to_timestamp(row["last_tick_at"].as<std::string>());
            }
            instance.stale_seconds = row["stale_seconds"].is_null() ? std::nullopt : std::optional<int64_t>(string_to_interval(row["stale_seconds"].as<std::string>()));
            instance.reconnect_attempts = row["reconnect_attempts"].as<int64_t>();
            instance.message_rate_in = row["message_rate_in"].is_null() ? std::nullopt : std::optional<double>(row["message_rate_in"].as<double>());
            instance.message_rate_out = row["message_rate_out"].is_null() ? std::nullopt : std::optional<double>(row["message_rate_out"].as<double>());
            instance.queue_depth = row["queue_depth"].is_null() ? std::nullopt : std::optional<int64_t>(row["queue_depth"].as<int64_t>());
            instance.backpressure_active = row["backpressure_active"].is_null() ? std::nullopt : std::optional<bool>(row["backpressure_active"].as<bool>());
            instance.serialization_ms = row["serialization_ms"].is_null() ? std::nullopt : std::optional<double>(row["serialization_ms"].as<double>());
            instance.parse_error_count = row["parse_error_count"].is_null() ? std::nullopt : std::optional<int64_t>(row["parse_error_count"].as<int64_t>());
            instance.gap_count = row["gap_count"].is_null() ? std::nullopt : std::optional<int64_t>(row["gap_count"].as<int64_t>());
            instance.duplicate_count = row["duplicate_count"].is_null() ? std::nullopt : std::optional<int64_t>(row["duplicate_count"].as<int64_t>());
            instance.out_of_order_count = row["out_of_order_count"].is_null() ? std::nullopt : std::optional<int64_t>(row["out_of_order_count"].as<int64_t>());
            instance.tenant_id = row["tenant_id"].is_null() ? std::nullopt : std::optional<std::string>(row["tenant_id"].as<std::string>());
            instances.push_back(instance);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting feed instances by condition: " << e.what() << std::endl;
    }
    return instances;
}

bool SAdapter::update_feed_instance(const FeedInstance& instance) {
    if (!is_connected() || instance.instance_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE feed_instances SET "
            "exchange = $1, adapter_type = $2, feed_status = $3, last_tick_at = $4, stale_seconds = $5, reconnect_attempts = $6, message_rate_in = $7, message_rate_out = $8, queue_depth = $9, backpressure_active = $10, serialization_ms = $11, parse_error_count = $12, gap_count = $13, duplicate_count = $14, out_of_order_count = $15, tenant_id = $16 "
            "WHERE instance_id = $17",
            instance.exchange,
            instance.adapter_type ? *instance.adapter_type : pqxx::null,
            instance.feed_status,
            instance.last_tick_at ? timestamp_to_string(*instance.last_tick_at) : pqxx::null,
            instance.stale_seconds ? interval_to_string(*instance.stale_seconds) : pqxx::null,
            instance.reconnect_attempts,
            instance.message_rate_in ? *instance.message_rate_in : pqxx::null,
            instance.message_rate_out ? *instance.message_rate_out : pqxx::null,
            instance.queue_depth ? *instance.queue_depth : pqxx::null,
            instance.backpressure_active ? *instance.backpressure_active : pqxx::null,
            instance.serialization_ms ? *instance.serialization_ms : pqxx::null,
            instance.parse_error_count ? *instance.parse_error_count : pqxx::null,
            instance.gap_count ? *instance.gap_count : pqxx::null,
            instance.duplicate_count ? *instance.duplicate_count : pqxx::null,
            instance.out_of_order_count ? *instance.out_of_order_count : pqxx::null,
            instance.tenant_id ? *instance.tenant_id : pqxx::null,
            instance.instance_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating feed instance: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_feed_instance(const std::string& instance_id) {
    if (!is_connected() || instance_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM feed_instances WHERE instance_id = $1",
            instance_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting feed instance: " << e.what() << std::endl;
        return false;
    }
}

// FeedMetricsSnapshot operations

std::optional<FeedMetricsSnapshot> SAdapter::create_feed_metrics_snapshot(const FeedMetricsSnapshot& snapshot) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO feed_metrics_snapshots (instance_id, measured_at, p50_latency_ms, p95_latency_ms, p99_latency_ms, avg_latency_ms, drop_rate, packet_loss_rate, msgs_sent, msgs_received, bytes_sent, bytes_received, cpu_usage, memory_usage, thread_count, event_loop_lag_ms, uptime_seconds) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) "
            "RETURNING id",
            snapshot.instance_id,
            timestamp_to_string(snapshot.measured_at),
            snapshot.p50_latency_ms ? *snapshot.p50_latency_ms : pqxx::null,
            snapshot.p95_latency_ms ? *snapshot.p95_latency_ms : pqxx::null,
            snapshot.p99_latency_ms ? *snapshot.p99_latency_ms : pqxx::null,
            snapshot.avg_latency_ms ? *snapshot.avg_latency_ms : pqxx::null,
            snapshot.drop_rate ? *snapshot.drop_rate : pqxx::null,
            packet_loss_rate ? *packet_loss_rate : pqxx::null,
            snapshot.msgs_sent ? *snapshot.msgs_sent : pqxx::null,
            snapshot.msgs_received ? *snapshot.msgs_received : pqxx::null,
            snapshot.bytes_sent ? *snapshot.bytes_sent : pqxx::null,
            snapshot.bytes_received ? *snapshot.bytes_received : pqxx::null,
            snapshot.cpu_usage ? *snapshot.cpu_usage : pqxx::null,
            snapshot.memory_usage ? *snapshot.memory_usage : pqxx::null,
            snapshot.thread_count ? *snapshot.thread_count : pqxx::null,
            snapshot.event_loop_lag_ms ? *snapshot.event_loop_lag_ms : pqxx::null,
            snapshot.uptime_seconds ? *snapshot.uptime_seconds : pqxx::null
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        FeedMetricsSnapshot created = snapshot;
        created.id = r[0]["id"].as<int64_t>();
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating feed metrics snapshot: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshot_by_id(int64_t id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM feed_metrics_snapshots WHERE id = $1",
            id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        FeedMetricsSnapshot snapshot;
        snapshot.id = r[0]["id"].as<int64_t>();
        snapshot.instance_id = r[0]["instance_id"].as<std::string>();
        snapshot.measured_at = string_to_timestamp(r[0]["measured_at"].as<std::string>());
        snapshot.p50_latency_ms = r[0]["p50_latency_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["p50_latency_ms"].as<double>());
        snapshot.p95_latency_ms = r[0]["p95_latency_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["p95_latency_ms"].as<double>());
        snapshot.p99_latency_ms = r[0]["p99_latency_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["p99_latency_ms"].as<double>());
        snapshot.avg_latency_ms = r[0]["avg_latency_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["avg_latency_ms"].as<double>());
        snapshot.drop_rate = r[0]["drop_rate"].is_null() ? std::nullopt : std::optional<double>(r[0]["drop_rate"].as<double>());
        snapshot.packet_loss_rate = r[0]["packet_loss_rate"].is_null() ? std::nullopt : std::optional<double>(r[0]["packet_loss_rate"].as<double>());
        snapshot.msgs_sent = r[0]["msgs_sent"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["msgs_sent"].as<int64_t>());
        snapshot.msgs_received = r[0]["msgs_received"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["msgs_received"].as<int64_t>());
        snapshot.bytes_sent = r[0]["bytes_sent"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["bytes_sent"].as<int64_t>());
        snapshot.bytes_received = r[0]["bytes_received"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["bytes_received"].as<int64_t>());
        snapshot.cpu_usage = r[0]["cpu_usage"].is_null() ? std::nullopt : std::optional<double>(r[0]["cpu_usage"].as<double>());
        snapshot.memory_usage = r[0]["memory_usage"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["memory_usage"].as<int64_t>());
        snapshot.thread_count = r[0]["thread_count"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["thread_count"].as<int64_t>());
        snapshot.event_loop_lag_ms = r[0]["event_loop_lag_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["event_loop_lag_ms"].as<double>());
        snapshot.uptime_seconds = r[0]["uptime_seconds"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["uptime_seconds"].as<int64_t>());
        return snapshot;
    } catch (const std::exception& e) {
        std::cerr << "Error getting feed metrics snapshot by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshots_by_condition(const std::string& where_clause) {
    std::vector<FeedMetricsSnapshot> snapshots;
    if (!is_connected()) {
        return snapshots;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM feed_metrics_snapshots";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            FeedMetricsSnapshot snapshot;
            snapshot.id = row["id"].as<int64_t>();
            snapshot.instance_id = row["instance_id"].as<std::string>();
            snapshot.measured_at = string_to_timestamp(row["measured_at"].as<std::string>());
            snapshot.p50_latency_ms = row["p50_latency_ms"].is_null() ? std::nullopt : std::optional<double>(row["p50_latency_ms"].as<double>());
            snapshot.p95_latency_ms = row["p95_latency_ms"].is_null() ? std::nullopt : std::optional<double>(row["p95_latency_ms"].as<double>());
            snapshot.p99_latency_ms = row["p99_latency_ms"].is_null() ? std::nullopt : std::optional<double>(row["p99_latency_ms"].as<double>());
            snapshot.avg_latency_ms = row["avg_latency_ms"].is_null() ? std::nullopt : std::optional<double>(row["avg_latency_ms"].as<double>());
            snapshot.drop_rate = row["drop_rate"].is_null() ? std::nullopt : std::optional<double>(row["drop_rate"].as<double>());
            snapshot.packet_loss_rate = row["packet_loss_rate"].is_null() ? std::nullopt : std::optional<double>(row["packet_loss_rate"].as<double>());
            snapshot.msgs_sent = row["msgs_sent"].is_null() ? std::nullopt : std::optional<int64_t>(row["msgs_sent"].as<int64_t>());
            snapshot.msgs_received = row["msgs_received"].is_null() ? std::nullopt : std::optional<int64_t>(row["msgs_received"].as<int64_t>());
            snapshot.bytes_sent = row["bytes_sent"].is_null() ? std::nullopt : std::optional<int64_t>(row["bytes_sent"].as<int64_t>());
            snapshot.bytes_received = row["bytes_received"].is_null() ? std::nullopt : std::optional<int64_t>(row["bytes_received"].as<int64_t>());
            snapshot.cpu_usage = row["cpu_usage"].is_null() ? std::nullopt : std::optional<double>(row["cpu_usage"].as<double>());
            snapshot.memory_usage = row["memory_usage"].is_null() ? std::nullopt : std::optional<int64_t>(row["memory_usage"].as<int64_t>());
            snapshot.thread_count = row["thread_count"].is_null() ? std::nullopt : std::optional<int64_t>(row["thread_count"].as<int64_t>());
            snapshot.event_loop_lag_ms = row["event_loop_lag_ms"].is_null() ? std::nullopt : std::optional<double>(row["event_loop_lag_ms"].as<double>());
            snapshot.uptime_seconds = row["uptime_seconds"].is_null() ? std::nullopt : std::optional<int64_t>(row["uptime_seconds"].as<int64_t>());
            snapshots.push_back(snapshot);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting feed metrics snapshots by condition: " << e.what() << std::endl;
    }
    return snapshots;
}

bool SAdapter::update_feed_metrics_snapshot(const FeedMetricsSnapshot& snapshot) {
    if (!is_connected() || snapshot.id <= 0) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE feed_metrics_snapshots SET "
            "instance_id = $1, measured_at = $2, p50_latency_ms = $3, p95_latency_ms = $4, p99_latency_ms = $5, avg_latency_ms = $6, drop_rate = $7, packet_loss_rate = $8, msgs_sent = $9, msgs_received = $10, bytes_sent = $11, bytes_received = $12, cpu_usage = $13, memory_usage = $14, thread_count = $15, event_loop_lag_ms = $16, uptime_seconds = $17 "
            "WHERE id = $18",
            snapshot.instance_id,
            timestamp_to_string(snapshot.measured_at),
            snapshot.p50_latency_ms ? *snapshot.p50_latency_ms : pqxx::null,
            snapshot.p95_latency_ms ? *snapshot.p95_latency_ms : pqxx::null,
            snapshot.p99_latency_ms ? *snapshot.p99_latency_ms : pqxx::null,
            snapshot.avg_latency_ms ? *snapshot.avg_latency_ms : pqxx::null,
            snapshot.drop_rate ? *snapshot.drop_rate : pqxx::null,
            packet_loss_rate ? *packet_loss_rate : pqxx::null,
            snapshot.msgs_sent ? *snapshot.msgs_sent : pqxx::null,
            snapshot.msgs_received ? *snapshot.msgs_received : pqxx::null,
            snapshot.bytes_sent ? *snapshot.bytes_sent : pqxx::null,
            snapshot.bytes_received ? *snapshot.bytes_received : pqxx::null,
            snapshot.cpu_usage ? *snapshot.cpu_usage : pqxx::null,
            snapshot.memory_usage ? *snapshot.memory_usage : pqxx::null,
            snapshot.thread_count ? *snapshot.thread_count : pqxx::null,
            snapshot.event_loop_lag_ms ? *snapshot.event_loop_lag_ms : pqxx::null,
            snapshot.uptime_seconds ? *snapshot.uptime_seconds : pqxx::null,
            snapshot.id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating feed metrics snapshot: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_feed_metrics_snapshot(int64_t id) {
    if (!is_connected() || id <= 0) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM feed_metrics_snapshots WHERE id = $1",
            id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting feed metrics snapshot: " << e.what() << std::endl;
        return false;
    }
}

// FeedEvent operations

std::optional<FeedEvent> SAdapter::create_feed_event(const FeedEvent& event) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO feed_events (actor_type, actor_id, action_type, target_type, target_id, result, error_code, trace_id, correlation_id, metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            "RETURNING event_id, occurred_at",
            event.actor_type ? *event.actor_type : pqxx::null,
            event.actor_id ? *event.actor_id : pqxx::null,
            event.action_type,
            event.target_type ? *event.target_type : pqxx::null,
            event.target_id ? *event.target_id : pqxx::null,
            event.result ? *event.result : pqxx::null,
            event.error_code ? *event.error_code : pqxx::null,
            event.trace_id ? *event.trace_id : pqxx::null,
            event.correlation_id ? *event.correlation_id : pqxx::null,
            event.metadata ? *event.metadata : pqxx::null
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        FeedEvent created = event;
        created.event_id = r[0]["event_id"].as<std::string>();
        created.occurred_at = string_to_timestamp(r[0]["occurred_at"].as<std::string>());
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating feed event: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<FeedEvent> SAdapter::get_feed_event_by_id(const std::string& event_id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM feed_events WHERE event_id = $1",
            event_id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        FeedEvent event;
        event.event_id = r[0]["event_id"].as<std::string>();
        event.actor_type = r[0]["actor_type"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["actor_type"].as<std::string>());
        event.actor_id = r[0]["actor_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["actor_id"].as<std::string>());
        event.action_type = r[0]["action_type"].as<std::string>();
        event.target_type = r[0]["target_type"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["target_type"].as<std::string>());
        event.target_id = r[0]["target_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["target_id"].as<std::string>());
        event.result = r[0]["result"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["result"].as<std::string>());
        event.error_code = r[0]["error_code"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["error_code"].as<std::string>());
        event.trace_id = r[0]["trace_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["trace_id"].as<std::string>());
        event.correlation_id = r[0]["correlation_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["correlation_id"].as<std::string>());
        event.occurred_at = string_to_timestamp(r[0]["occurred_at"].as<std::string>());
        event.metadata = r[0]["metadata"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["metadata"].as<std::string>());
        return event;
    } catch (const std::exception& e) {
        std::cerr << "Error getting feed event by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<FeedEvent> SAdapter::get_feed_events_by_condition(const std::string& where_clause) {
    std::vector<FeedEvent> events;
    if (!is_connected()) {
        return events;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM feed_events";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            FeedEvent event;
            event.event_id = row["event_id"].as<std::string>();
            event.actor_type = row["actor_type"].is_null() ? std::nullopt : std::optional<std::string>(row["actor_type"].as<std::string>());
            event.actor_id = row["actor_id"].is_null() ? std::nullopt : std::optional<std::string>(row["actor_id"].as<std::string>());
            event.action_type = row["action_type"].as<std::string>();
            event.target_type = row["target_type"].is_null() ? std::nullopt : std::optional<std::string>(row["target_type"].as<std::string>());
            event.target_id = row["target_id"].is_null() ? std::nullopt : std::optional<std::string>(row["target_id"].as<std::string>());
            event.result = row["result"].is_null() ? std::nullopt : std::optional<std::string>(row["result"].as<std::string>());
            event.error_code = row["error_code"].is_null() ? std::nullopt : std::optional<std::string>(row["error_code"].as<std::string>());
            event.trace_id = row["trace_id"].is_null() ? std::nullopt : std::optional<std::string>(row["trace_id"].as<std::string>());
            event.correlation_id = row["correlation_id"].is_null() ? std::nullopt : std::optional<std::string>(row["correlation_id"].as<std::string>());
            event.occurred_at = string_to_timestamp(row["occurred_at"].as<std::string>());
            event.metadata = row["metadata"].is_null() ? std::nullopt : std::optional<std::string>(row["metadata"].as<std::string>());
            events.push_back(event);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting feed events by condition: " << e.what() << std::endl;
    }
    return events;
}

bool SAdapter::update_feed_event(const FeedEvent& event) {
    if (!is_connected() || event.event_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE feed_events SET "
            "actor_type = $1, actor_id = $2, action_type = $3, target_type = $4, target_id = $5, result = $6, error_code = $7, trace_id = $8, correlation_id = $9, metadata = $10 "
            "WHERE event_id = $11",
            event.actor_type ? *event.actor_type : pqxx::null,
            event.actor_id ? *event.actor_id : pqxx::null,
            event.action_type,
            event.target_type ? *event.target_type : pqxx::null,
            event.target_id ? *event.target_id : pqxx::null,
            event.result ? *event.result : pqxx::null,
            event.error_code ? *event.error_code : pqxx::null,
            event.trace_id ? *event.trace_id : pqxx::null,
            event.correlation_id ? *event.correlation_id : pqxx::null,
            event.metadata ? *event.metadata : pqxx::null,
            event.event_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating feed event: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_feed_event(const std::string& event_id) {
    if (!is_connected() || event_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM feed_events WHERE event_id = $1",
            event_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting feed event: " << e.what() << std::endl;
        return false;
    }
}

// ApiRequest operations

std::optional<ApiRequest> SAdapter::create_api_request(const ApiRequest& request) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO api_requests (endpoint, method, status_code, latency_ms, request_size, response_size, client_id, session_id, instance_id) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
            "RETURNING request_id, timestamp",
            request.endpoint,
            request.method,
            request.status_code ? *request.status_code : pqxx::null,
            request.latency_ms ? *request.latency_ms : pqxx::null,
            request.request_size ? *request.request_size : pqxx::null,
            request.response_size ? *request.response_size : pqxx::null,
            request.client_id ? *request.client_id : pqxx::null,
            request.session_id ? *request.session_id : pqxx::null,
            request.instance_id ? *request.instance_id : pqxx::null
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        ApiRequest created = request;
        created.request_id = r[0]["request_id"].as<std::string>();
        created.timestamp = string_to_timestamp(r[0]["timestamp"].as<std::string>());
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating api request: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<ApiRequest> SAdapter::get_api_request_by_id(const std::string& request_id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM api_requests WHERE request_id = $1",
            request_id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        ApiRequest request;
        request.request_id = r[0]["request_id"].as<std::string>();
        request.endpoint = r[0]["endpoint"].as<std::string>();
        request.method = r[0]["method"].as<std::string>();
        request.status_code = r[0]["status_code"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["status_code"].as<int64_t>());
        request.latency_ms = r[0]["latency_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["latency_ms"].as<double>());
        request.request_size = r[0]["request_size"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["request_size"].as<int64_t>());
        request.response_size = r[0]["response_size"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["response_size"].as<int64_t>());
        request.client_id = r[0]["client_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["client_id"].as<std::string>());
        request.session_id = r[0]["session_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["session_id"].as<std::string>());
        request.instance_id = r[0]["instance_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["instance_id"].as<std::string>());
        request.timestamp = string_to_timestamp(r[0]["timestamp"].as<std::string>());
        return request;
    } catch (const std::exception& e) {
        std::cerr << "Error getting api request by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<ApiRequest> SAdapter::get_api_requests_by_condition(const std::string& where_clause) {
    std::vector<ApiRequest> requests;
    if (!is_connected()) {
        return requests;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM api_requests";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            ApiRequest request;
            request.request_id = row["request_id"].as<std::string>();
            request.endpoint = row["endpoint"].as<std::string>();
            request.method = row["method"].as<std::string>();
            request.status_code = row["status_code"].is_null() ? std::nullopt : std::optional<int64_t>(row["status_code"].as<int64_t>());
            request.latency_ms = row["latency_ms"].is_null() ? std::nullopt : std::optional<double>(row["latency_ms"].as<double>());
            request.request_size = row["request_size"].is_null() ? std::nullopt : std::optional<int64_t>(row["request_size"].as<int64_t>());
            request.response_size = row["response_size"].is_null() ? std::nullopt : std::optional<int64_t>(row["response_size"].as<int64_t>());
            request.client_id = row["client_id"].is_null() ? std::nullopt : std::optional<std::string>(row["client_id"].as<std::string>());
            request.session_id = row["session_id"].is_null() ? std::nullopt : std::optional<std::string>(row["session_id"].as<std::string>());
            request.instance_id = row["instance_id"].is_null() ? std::nullopt : std::optional<std::string>(row["instance_id"].as<std::string>());
            request.timestamp = string_to_timestamp(row["timestamp"].as<std::string>());
            requests.push_back(request);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting api requests by condition: " << e.what() << std::endl;
    }
    return requests;
}

bool SAdapter::update_api_request(const ApiRequest& request) {
    if (!is_connected() || request.request_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE api_requests SET "
            "endpoint = $1, method = $2, status_code = $3, latency_ms = $4, request_size = $5, response_size = $6, client_id = $7, session_id = $8, instance_id = $9 "
            "WHERE request_id = $10",
            request.endpoint,
            request.method,
            request.status_code ? *request.status_code : pqxx::null,
            request.latency_ms ? *request.latency_ms : pqxx::null,
            request.request_size ? *request.request_size : pqxx::null,
            request.response_size ? *request.response_size : pqxx::null,
            request.client_id ? *request.client_id : pqxx::null,
            request.session_id ? *request.session_id : pqxx::null,
            request.instance_id ? *request.instance_id : pqxx::null,
            request.request_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating api request: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_api_request(const std::string& request_id) {
    if (!is_connected() || request_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM api_requests WHERE request_id = $1",
            request_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting api request: " << e.what() << std::endl;
        return false;
    }
}

// ExchangeHealth operations

std::optional<ExchangeHealth> SAdapter::create_exchange_health(const ExchangeHealth& health) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO exchange_health (exchange_name, endpoint, status, last_success_at, last_error_at, error_count, rate_limit_hits, latency_ms, symbols_active, feed_lag_ms) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            "RETURNING id",
            health.exchange_name,
            health.endpoint ? *health.endpoint : pqxx::null,
            health.status,
            health.last_success_at ? timestamp_to_string(*health.last_success_at) : pqxx::null,
            health.last_error_at ? timestamp_to_string(*health.last_error_at) : pqxx::null,
            health.error_count ? *health.error_count : pqxx::null,
            health.rate_limit_hits ? *health.rate_limit_hits : pqxx::null,
            health.latency_ms ? *health.latency_ms : pqxx::null,
            health.symbols_active ? *health.symbols_active : pqxx::null,
            health.feed_lag_ms ? *health.feed_lag_ms : pqxx::null
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        ExchangeHealth created = health;
        created.id = r[0]["id"].as<int64_t>();
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating exchange health: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<ExchangeHealth> SAdapter::get_exchange_health_by_id(int64_t id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM exchange_health WHERE id = $1",
            id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        ExchangeHealth health;
        health.id = r[0]["id"].as<int64_t>();
        health.exchange_name = r[0]["exchange_name"].as<std::string>();
        health.endpoint = r[0]["endpoint"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["endpoint"].as<std::string>());
        health.status = r[0]["status"].as<std::string>();
        if (!r[0]["last_success_at"].is_null()) {
            health.last_success_at = string_to_timestamp(r[0]["last_success_at"].as<std::string>());
        }
        if (!r[0]["last_error_at"].is_null()) {
            health.last_error_at = string_to_timestamp(r[0]["last_error_at"].as<std::string>());
        }
        health.error_count = r[0]["error_count"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["error_count"].as<int64_t>());
        health.rate_limit_hits = r[0]["rate_limit_hits"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["rate_limit_hits"].as<int64_t>());
        health.latency_ms = r[0]["latency_ms"].is_null() ? std::nullopt : std::optional<double>(r[0]["latency_ms"].as<double>());
        health.symbols_active = r[0]["symbols_active"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["symbols_active"].as<int64_t>());
        health.feed_lag_ms = r[0]["feed_lag_ms"].is_null() ? std::nullopt : std::optional<int64_t>(r[0]["feed_lag_ms"].as<int64_t>());
        health.checked_at = string_to_timestamp(r[0]["checked_at"].as<std::string>());
        return health;
    } catch (const std::exception& e) {
        std::cerr << "Error getting exchange health by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<ExchangeHealth> SAdapter::get_exchange_healths_by_condition(const std::string& where_clause) {
    std::vector<ExchangeHealth> healths;
    if (!is_connected()) {
        return healths;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM exchange_health";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            ExchangeHealth health;
            health.id = row["id"].as<int64_t>();
            health.exchange_name = row["exchange_name"].as<std::string>();
            health.endpoint = row["endpoint"].is_null() ? std::nullopt : std::optional<std::string>(row["endpoint"].as<std::string>());
            health.status = row["status"].as<std::string>();
            if (!row["last_success_at"].is_null()) {
                health.last_success_at = string_to_timestamp(row["last_success_at"].as<std::string>());
            }
            if (!row["last_error_at"].is_null()) {
                health.last_error_at = string_to_timestamp(row["last_error_at"].as<std::string>());
            }
            health.error_count = row["error_count"].is_null() ? std::nullopt : std::optional<int64_t>(row["error_count"].as<int64_t>());
            health.rate_limit_hits = row["rate_limit_hits"].is_null() ? std::nullopt : std::optional<int64_t>(row["rate_limit_hits"].as<int64_t>());
            health.latency_ms = row["latency_ms"].is_null() ? std::nullopt : std::optional<double>(row["latency_ms"].as<double>());
            health.symbols_active = row["symbols_active"].is_null() ? std::nullopt : std::optional<int64_t>(row["symbols_active"].as<int64_t>());
            health.feed_lag_ms = row["feed_lag_ms"].is_null() ? std::nullopt : std::optional<int64_t>(row["feed_lag_ms"].as<int64_t>());
            health.checked_at = string_to_timestamp(row["checked_at"].as<std::string>());
            healths.push_back(health);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting exchange healths by condition: " << e.what() << std::endl;
    }
    return healths;
}

bool SAdapter::update_exchange_health(const ExchangeHealth& health) {
    if (!is_connected() || health.id <= 0) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE exchange_health SET "
            "exchange_name = $1, endpoint = $2, status = $3, last_success_at = $4, last_error_at = $5, error_count = $6, rate_limit_hits = $7, latency_ms = $8, symbols_active = $9, feed_lag_ms = $10 "
            "WHERE id = $11",
            health.exchange_name,
            health.endpoint ? *health.endpoint : pqxx::null,
            health.status,
            health.last_success_at ? timestamp_to_string(*health.last_success_at) : pqxx::null,
            health.last_error_at ? timestamp_to_string(*health.last_error_at) : pqxx::null,
            health.error_count ? *health.error_count : pqxx::null,
            health.rate_limit_hits ? *health.rate_limit_hits : pqxx::null,
            health.latency_ms ? *health.latency_ms : pqxx::null,
            health.symbols_active ? *health.symbols_active : pqxx::null,
            health.feed_lag_ms ? *health.feed_lag_ms : pqxx::null
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating exchange health: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_exchange_health(int64_t id) {
    if (!is_connected() || id <= 0) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM exchange_health WHERE id = $1",
            id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting exchange health: " << e.what() << std::endl;
        return false;
    }
}

// BacktestJob operations

std::optional<BacktestJob> SAdapter::create_backtest_job(const BacktestJob& job) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO backtest_jobs (dataset_id, date_range, symbols, mode, status, progress, started_at, finished_at, result_summary, result_artifact_url) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            "RETURNING job_id",
            job.dataset_id ? *job.dataset_id : pqxx::null,
            job.date_range ? *job.date_range : pqxx::null,
            job.symbols ? *job.symbols : pqxx::null,
            job.mode ? *job.mode : pqxx::null,
            job.status,
            job.progress ? *job.progress : pqxx::null,
            job.started_at ? timestamp_to_string(*job.started_at) : pqxx::null,
            job.finished_at ? timestamp_to_string(*job.finished_at) : pqxx::null,
            job.result_summary ? *job.result_summary : pqxx::null,
            job.result_artifact_url ? *job.result_artifact_url : pqxx::null
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        BacktestJob created = job;
        created.job_id = r[0]["job_id"].as<std::string>();
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating backtest job: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<BacktestJob> SAdapter::get_backtest_job_by_id(const std::string& job_id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "SELECT * FROM backtest_jobs WHERE job_id = $1",
            job_id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        BacktestJob job;
        job.job_id = r[0]["job_id"].as<std::string>();
        job.dataset_id = r[0]["dataset_id"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["dataset_id"].as<std::string>());
        job.date_range = r[0]["date_range"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["date_range"].as<std::string>());
        job.symbols = r[0]["symbols"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["symbols"].as<std::string>());
        job.mode = r[0]["mode"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["mode"].as<std::string>());
        job.status = r[0]["status"].as<std::string>();
        job.progress = r[0]["progress"].is_null() ? std::nullopt : std::optional<double>(r[0]["progress"].as<double>());
        if (!r[0]["started_at"].is_null()) {
            job.started_at = string_to_timestamp(r[0]["started_at"].as<std::string>());
        }
        if (!r[0]["finished_at"].is_null()) {
            job.finished_at = string_to_timestamp(r[0]["finished_at"].as<std::string>());
        }
        job.result_summary = r[0]["result_summary"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["result_summary"].as<std::string>());
        job.result_artifact_url = r[0]["result_artifact_url"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["result_artifact_url"].as<std::string>());
        return job;
    } catch (const std::exception& e) {
        std::cerr << "Error getting backtest job by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<BacktestJob> SAdapter::get_backtest_jobs_by_condition(const std::string& where_clause) {
    std::vector<BacktestJob> jobs;
    if (!is_connected()) {
        return jobs;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM backtest_jobs";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            BacktestJob job;
            job.job_id = row["job_id"].as<std::string>();
            job.dataset_id = row["dataset_id"].is_null() ? std::nullopt : std::optional<std::string>(row["dataset_id"].as<std::string>());
            job.date_range = row["date_range"].is_null() ? std::nullopt : std::optional<std::string>(row["date_range"].as<std::string>());
            job.symbols = row["symbols"].is_null() ? std::nullopt : std::optional<std::string>(row["symbols"].as<std::string>());
            job.mode = row["mode"].is_null() ? std::nullopt : std::optional<std::string>(row["mode"].as<std::string>());
            job.status = row["status"].as<std::string>();
            job.progress = row["progress"].is_null() ? std::nullopt : std::optional<double>(row["progress"].as<double>());
            if (!row["started_at"].is_null()) {
                job.started_at = string_to_timestamp(row["started_at"].as<std::string>());
            }
            if (!row["finished_at"].is_null()) {
                job.finished_at = string_to_timestamp(row["finished_at"].as<std::string>());
            }
            job.result_summary = row["result_summary"].is_null() ? std::nullopt : std::optional<std::string>(row["result_summary"].as<std::string>());
            job.result_artifact_url = row["result_artifact_url"].is_null() ? std::nullopt : std::optional<std::string>(row["result_artifact_url"].as<std::string>());
            jobs.push_back(job);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting backtest jobs by condition: " << e.what() << std::endl;
    }
    return jobs;
}

bool SAdapter::update_backtest_job(const BacktestJob& job) {
    if (!is_connected() || job.job_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE backtest_jobs SET "
            "dataset_id = $1, date_range = $2, symbols = $3, mode = $4, status = $5, progress = $6, started_at = $7, finished_at = $8, result_summary = $9, result_artifact_url = $10 "
            "WHERE job_id = $11",
            job.dataset_id ? *job.dataset_id : pqxx::null,
            job.date_range ? *job.date_range : pqxx::null,
            job.symbols ? *job.symbols : pqxx::null,
            job.mode ? *job.mode : pqxx::null,
            job.status,
            job.progress ? *job.progress : pqxx::null,
            job.started_at ? timestamp_to_string(*job.started_at) : pqxx::null,
            job.finished_at ? timestamp_to_string(*job.finished_at) : pqxx::null,
            job.result_summary ? *job.result_summary : pqxx::null,
            job.result_artifact_url ? *job.result_artifact_url : pqxx::null,
            job.job_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating backtest job: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_backtest_job(const std::string& job_id) {
    if (!is_connected() || job_id.empty()) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM backtest_jobs WHERE job_id = $1",
            job_id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting backtest job: " << e.what() << std::endl;
        return false;
    }
}

// ConfigVersion operations

std::optional<ConfigVersion> SAdapter::create_config_version(const ConfigVersion& config) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "INSERT INTO config_versions (config_version, build_sha, adapter_version, deployment_id, feature_flags, schema_version) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "RETURNING id, applied_at",
            config.config_version,
            config.build_sha,
            config.adapter_version,
            config.deployment_id,
            config.feature_flags ? *config.feature_flags : pqxx::null,
            config.schema_version
        );
        txn.commit();

        if (r.empty()) {
            return std::nullopt;
        }

        ConfigVersion created = config;
        created.id = r[0]["id"].as<int64_t>();
        created.applied_at = string_to_timestamp(r[0]["applied_at"].as<std::string>());
        return created;
    } catch (const std::exception& e) {
        std::cerr << "Error creating config version: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::optional<ConfigVersion> SAdapter::get_config_version_by_id(int64_t id) {
    if (!is_connected()) {
        return std::nullopt;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx.0 result r = txn.exec_params(
            "SELECT * FROM config_versions WHERE id = $1",
            id
        );
        if (r.empty()) {
            return std::nullopt;
        }

        ConfigVersion config;
        config.id = r[0]["id"].as<int64_t>();
        config.config_version = r[0]["config_version"].as<std::string>();
        config.build_sha = r[0]["build_sha"].as<std::string>();
        config.adapter_version = r[0]["adapter_version"].as<std::string>();
        config.deployment_id = r[0]["deployment_id"].as<std::string>();
        config.feature_flags = r[0]["feature_flags"].is_null() ? std::nullopt : std::optional<std::string>(r[0]["feature_flags"].as<std::string>());
        config.schema_version = r[0]["schema_version"].as<int64_t>();
        config.applied_at = string_to_timestamp(r[0]["applied_at"].as<std::string>());
        return config;
    } catch (const std::exception& e) {
        std::cerr << "Error getting config version by id: " << e.what() << std::endl;
        return std::nullopt;
    }
}

std::vector<ConfigVersion> SAdapter::get_config_versions_by_condition(const std::string& where_clause) {
    std::vector<ConfigVersion> configs;
    if (!is_connected()) {
        return configs;
    }

    try {
        pqxx::work txn(*conn_);
        std::string query = "SELECT * FROM config_versions";
        if (!where_clause.empty()) {
            query += " WHERE " + where_clause;
        }
        pqxx::result r = txn.exec(query);
        for (const auto& row : r) {
            ConfigVersion config;
            config.id = row["id"].as<int64_t>();
            config.config_version = row["config_version"].as<std::string>();
            config.build_sha = row["build_sha"].as<std::string>();
            config.adapter_version = row["adapter_version"].as<std::string>();
            config.deployment_id = row["deployment_id"].as<std::string>();
            config.feature_flags = row["feature_flags"].is_null() ? std::nullopt : std::optional<std::string>(row["feature_flags"].as<std::string>());
            config.schema_version = row["schema_version"].as<int64_t>();
            config.applied_at = string_to_timestamp(row["applied_at"].as<std::string>());
            configs.push_back(config);
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "Error getting config versions by condition: " << e.what() << std::endl;
    }
    return configs;
}

bool SAdapter::update_config_version(const ConfigVersion& config) {
    if (!is_connected() || config.id <= 0) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "UPDATE config_versions SET "
            "config_version = $1, build_sha = $2, adapter_version = $3, deployment_id = $4, feature_flags = $5, schema_version = $6 "
            "WHERE id = $7",
            config.config_version,
            config.build_sha,
            config.adapter_version,
            config.deployment_id,
            config.feature_flags ? *config.feature_flags : pqxx::null,
            config.schema_version,
            config.id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error updating config version: " << e.what() << std::endl;
        return false;
    }
}

bool SAdapter::delete_config_version(int64_t id) {
    if (!is_connected() || id <= 0) {
        return false;
    }

    try {
        pqxx::work txn(*conn_);
        pqxx::result r = txn.exec_params(
            "DELETE FROM config_versions WHERE id = $1",
            id
        );
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error deleting config version: " << e.what() << std::endl;
        return false;
    }
}

} // namespace datafeed