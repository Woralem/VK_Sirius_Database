#pragma once
#include <string>

namespace JsonHandler {
    std::string serializeSuccess(const std::string& message);
    std::string serializeError(const std::string& error_message);
}