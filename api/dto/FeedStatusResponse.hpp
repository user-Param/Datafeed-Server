#ifndef API_DTO_FEED_STATUS_RESPONSE_HPP
#define API_DTO_FEED_STATUS_RESPONSE_HPP

#include <string>
#include <cstdint>

namespace api {
namespace dto {

struct FeedStatusResponse {
    std::string instance_id;
    std::string exchange;
    std::string status;
    std::uint64_t uptime; // in seconds
};

} // namespace dto
} // namespace api

#endif // API_DTO_FEED_STATUS_RESPONSE_HPP