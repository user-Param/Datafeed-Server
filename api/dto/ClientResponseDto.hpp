#ifndef API_DTO_CLIENT_RESPONSE_DTO_HPP
#define API_DTO_CLIENT_RESPONSE_DTO_HPP

#include <string>
#include <cstdint>
#include <optional>

namespace api {
namespace dto {

struct ClientResponseDto {
    std::string tenant_id;
    std::string client_name;
    std::string plan;
    std::string status;
    std::uint64_t created_at; // milliseconds since epoch
    std::uint64_t updated_at; // milliseconds since epoch
    std::optional<std::uint64_t> last_seen_at; // milliseconds since epoch
    std::optional<std::string> auth_subject;
    std::optional<std::string> ip_address;
    std::optional<std::string> user_agent;
};

} // namespace dto
} // namespace api

#endif // API_DTO_CLIENT_RESPONSE_DTO_HPP