#include "api/json_handler.h"
#include <string>
#include "nlohmann/json.hpp"
#include "utils/logger.h"

namespace JsonHandler {
    using json = nlohmann::json;

    std::string serializeSuccess(const std::string& message) {//возвращает json-строку
        json j;
        j["status"] = "success";
        j["data"]["message"] = message;
        LOG_SUCCESS("Json handler", "Data was converted to JSON successfully");
        return j.dump(4);
    }

    std::string serializeError(const std::string& error_message, const std::vector<std::string>& errors) {
        json j;
        j["status"] = "error";
        j["error"] = error_message;
        j["errors"] = errors;
        LOG_SUCCESS("Json handler", "Data abou ERROR was converted to JSON successfully");
        return j.dump(4);
    }
}