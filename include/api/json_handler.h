#pragma once
#include <string>
#include <vector>
namespace JsonHandler {
    std::string serializeSuccess(const std::string& message);
    std::string serializeError(const std::string& error_message, const std::vector<std::string>& errors = {});
}