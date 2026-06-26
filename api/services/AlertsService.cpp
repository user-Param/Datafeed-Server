#include "AlertsService.hpp"
#include <chrono>

namespace api {
namespace services {

AlertsService::AlertsService(std::shared_ptr<datafeed::SAdapter> sadapter)
    : sadapter_(sadapter) {}

nlohmann::json AlertsService::getAlerts(const std::string& status)
{
    nlohmann::json arr = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return arr;

    std::string where;
    if (status == "active") where = "acknowledged = false AND resolved_at IS NULL";
    else if (status == "acknowledged") where = "acknowledged = true";
    else if (!status.empty()) where = "acknowledged = false";
    if (!where.empty()) where += " ORDER BY created_at DESC LIMIT 100";
    else where = "ORDER BY created_at DESC LIMIT 100";

    auto alerts = sadapter_->get_alerts_by_condition(where);
    for (const auto& a : alerts) {
        nlohmann::json j;
        j["alert_id"] = a.alert_id;
        j["instance_id"] = a.instance_id;
        j["severity"] = a.severity;
        j["source"] = a.source;
        j["metric_name"] = a.metric_name;
        j["current_value"] = a.current_value;
        j["threshold"] = a.threshold;
        j["message"] = a.message;
        j["acknowledged"] = a.acknowledged;
        j["created_at"] = a.created_at;
        j["resolved_at"] = a.resolved_at;
        arr.push_back(j);
    }
    return arr;
}

nlohmann::json AlertsService::acknowledgeAlert(const std::string& alert_id)
{
    if (!sadapter_ || !sadapter_->is_connected()) {
        return {{"error", "database not connected"}};
    }

    auto existing = sadapter_->get_alert_by_id(alert_id);
    if (!existing) {
        return nullptr;
    }

    existing->acknowledged = true;
    bool ok = sadapter_->update_alert(*existing);
    if (!ok) {
        return {{"error", "failed to acknowledge alert"}};
    }
    return {{"status", "acknowledged"}, {"alert_id", alert_id}};
}

nlohmann::json AlertsService::getAlertHistory()
{
    return getAlerts("all");
}

nlohmann::json AlertsService::getAudit(const std::string& from, const std::string& to,
    const std::string& actor, const std::string& action,
    const std::string& exchange, const std::string& client)
{
    nlohmann::json arr = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return arr;

    std::string where = "1=1";
    if (!from.empty()) where += " AND occurred_at >= " + from;
    if (!to.empty()) where += " AND occurred_at <= " + to;
    if (!actor.empty()) where += " AND actor_id LIKE '%" + actor + "%'";
    if (!action.empty()) where += " AND action_type = '" + action + "'";
    where += " ORDER BY occurred_at DESC LIMIT 200";

    auto events = sadapter_->get_feed_events_by_condition(where);
    for (const auto& ev : events) {
        nlohmann::json j;
        j["event_id"] = ev.event_id;
        j["actor_type"] = ev.actor_type;
        j["actor_id"] = ev.actor_id;
        j["action_type"] = ev.action_type;
        j["target_type"] = ev.target_type;
        j["target_id"] = ev.target_id;
        j["result"] = ev.result;
        j["error_code"] = ev.error_code;
        j["occurred_at"] = ev.occurred_at;
        j["trace_id"] = ev.trace_id;
        j["metadata"] = ev.metadata;
        arr.push_back(j);
    }
    return arr;
}

} // namespace services
} // namespace api
