
#pragma once
#include "config.h"
#include "utils.h"
#include <iostream>
#include <string>
#include <string_view>
#include <format>
#include <type_traits>

namespace utils {

class Logger {
public:
    enum class Level {
        DEBUG,
        INFO,
        SUCCESS,
        WARNING,
        ERROR
    };

    enum class Color : int {
        RESET = 0,
        BLACK = 30,
        RED = 31,
        GREEN = 32,
        YELLOW = 33,
        BLUE = 34,
        MAGENTA = 35,
        CYAN = 36,
        WHITE = 37,
        BRIGHT_BLACK = 90,
        BRIGHT_RED = 91,
        BRIGHT_GREEN = 92,
        BRIGHT_YELLOW = 93,
        BRIGHT_BLUE = 94,
        BRIGHT_MAGENTA = 95,
        BRIGHT_CYAN = 96,
        BRIGHT_WHITE = 97
    };

    struct ColorAndPrefix {
        Color color;
        std::string_view prefix;
    };

    static void log(Level level, std::string_view component, std::string_view message) {
        const auto [color, prefix] = getColorAndPrefix(level);

        auto formatted = std::format(
            "\033[{}m{} {:15} {}\033[0m",
            static_cast<int>(color),
            prefix,
            std::format("[{}]", component),
            message
        );

        std::cout << formatted << std::endl;
    }

    static void separator() {
        std::cout << "\033[90m" << std::string(80, '-') << "\033[0m" << std::endl;
    }

    static void header(std::string_view text) {
        separator();
        std::cout << std::format("\033[1;36m>>> {} <<<\033[0m", text) << std::endl;
        separator();
    }

    static void printBox(std::string_view title, std::string_view content) {
        utils::StringBuilder builder(1024);

        builder << std::format("\033[94m+- {} {:-<{}}\033[0m\n",
                              title, "+", 75 - title.length());

        auto lines = content | std::views::split('\n') |
                    std::views::transform([](auto&& range) {
                        return std::string_view{range.begin(), range.end()};
                    });

        for (const auto& line : lines) {
            builder << std::format("\033[94m|\033[0m {:77} \033[94m|\033[0m\n", line);
        }

        builder << std::format("\033[94m+{:-<78}+\033[0m", "");

        std::cout << std::move(builder).str() << std::endl;
    }

private:
    static constexpr ColorAndPrefix getColorAndPrefix(Level level) noexcept {
        switch (level) {
            case Level::DEBUG:   return {Color::BRIGHT_BLACK, "[DEBUG]"};
            case Level::INFO:    return {Color::BRIGHT_CYAN, "[INFO ]"};
            case Level::SUCCESS: return {Color::BRIGHT_GREEN, "[OK   ]"};
            case Level::WARNING: return {Color::BRIGHT_YELLOW, "[WARN ]"};
            case Level::ERROR:   return {Color::BRIGHT_RED, "[ERROR]"};
        }
        return {Color::RESET, "[?????]"};
    }
};

} // namespace utils

#define LOG_DEBUG(component, message) ::utils::Logger::log(::utils::Logger::Level::DEBUG, component, message)
#define LOG_INFO(component, message) ::utils::Logger::log(::utils::Logger::Level::INFO, component, message)
#define LOG_SUCCESS(component, message) ::utils::Logger::log(::utils::Logger::Level::SUCCESS, component, message)
#define LOG_WARNING(component, message) ::utils::Logger::log(::utils::Logger::Level::WARNING, component, message)
#define LOG_ERROR(component, message) ::utils::Logger::log(::utils::Logger::Level::ERROR, component, message)
#define LOGF_DEBUG(component, fmt, ...)   ::utils::Logger::log(::utils::Logger::Level::DEBUG, component, std::format(fmt, __VA_ARGS__))
#define LOGF_INFO(component, fmt, ...)    ::utils::Logger::log(::utils::Logger::Level::INFO, component, std::format(fmt, __VA_ARGS__))
#define LOGF_SUCCESS(component, fmt, ...) ::utils::Logger::log(::utils::Logger::Level::SUCCESS, component, std::format(fmt, __VA_ARGS__))
#define LOGF_WARNING(component, fmt, ...) ::utils::Logger::log(::utils::Logger::Level::WARNING, component, std::format(fmt, __VA_ARGS__))
#define LOGF_ERROR(component, fmt, ...)   ::utils::Logger::log(::utils::Logger::Level::ERROR, component, std::format(fmt, __VA_ARGS__))