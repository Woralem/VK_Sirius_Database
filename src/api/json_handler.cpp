#include "api/json_handler.h"
#include <string>
#include "nlohmann/json.hpp"

namespace JsonHandler {
    using json = nlohmann::json;

    std::string serializeSuccess(const std::string& message) {
        json j;
        j["status"] = "success";
        j["data"]["message"] = message;
        return j.dump(4);
    }

    std::string serializeError(const std::string& error_message) {
        json j;
        j["status"] = "error";
        j["error"] = error_message;
        return j.dump(4);
    }
}