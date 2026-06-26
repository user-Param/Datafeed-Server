#include "SearchService.hpp"

namespace api {
namespace services {

SearchService::SearchService(std::shared_ptr<datafeed::SAdapter> sadapter)
    : sadapter_(sadapter) {}

nlohmann::json SearchService::search(const std::string& query)
{
    nlohmann::json results;
    results["exchanges"] = nlohmann::json::array();
    results["clients"] = nlohmann::json::array();
    results["sessions"] = nlohmann::json::array();
    results["alerts"] = nlohmann::json::array();
    results["events"] = nlohmann::json::array();

    if (!sadapter_ || !sadapter_->is_connected() || query.empty()) {
        return results;
    }

    std::string like = "%" + query + "%";

    // Search exchanges
    auto instances = sadapter_->get_feed_instances_by_condition(
        "exchange ILIKE '" + like + "' LIMIT 10");
    for (const auto& inst : instances) {
        nlohmann::json j;
        j["type"] = "exchange";
        j["instance_id"] = inst.instance_id;
        j["exchange"] = inst.exchange;
        j["status"] = inst.feed_status;
        results["exchanges"].push_back(j);
    }

    // Search clients
    auto clients = sadapter_->get_clients_by_condition(
        "client_name ILIKE '" + like + "' OR tenant_id ILIKE '" + like + "' LIMIT 10");
    for (const auto& c : clients) {
        nlohmann::json j;
        j["type"] = "client";
        j["tenant_id"] = c.tenant_id;
        j["client_name"] = c.client_name;
        j["status"] = c.status;
        results["clients"].push_back(j);
    }

    // Search sessions
    auto sessions = sadapter_->get_sessions_by_condition(
        "session_id ILIKE '" + like + "' LIMIT 10");
    for (const auto& s : sessions) {
        nlohmann::json j;
        j["type"] = "session";
        j["session_id"] = s.session_id;
        j["protocol"] = s.protocol;
        results["sessions"].push_back(j);
    }

    // Search alerts
    auto alerts = sadapter_->get_alerts_by_condition(
        "metric_name ILIKE '" + like + "' OR message ILIKE '" + like + "' LIMIT 10");
    for (const auto& a : alerts) {
        nlohmann::json j;
        j["type"] = "alert";
        j["alert_id"] = a.alert_id;
        j["severity"] = a.severity;
        j["metric_name"] = a.metric_name;
        j["acknowledged"] = a.acknowledged;
        results["alerts"].push_back(j);
    }

    // Search events
    auto events = sadapter_->get_feed_events_by_condition(
        "action_type ILIKE '" + like + "' OR target_id ILIKE '" + like + "' LIMIT 10");
    for (const auto& ev : events) {
        nlohmann::json j;
        j["type"] = "event";
        j["event_id"] = ev.event_id;
        j["action_type"] = ev.action_type;
        j["actor_id"] = ev.actor_id;
        j["occurred_at"] = ev.occurred_at;
        results["events"].push_back(j);
    }

    return results;
}

} // namespace services
} // namespace api
