#pragma once
#include <iostream>
#include <string>
#include <iomanip>
#include <sstream>

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

    enum class Color {
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

    static void log(Level level, const std::string& component, const std::string& message) {
        Color color;
        std::string prefix;
        
        switch (level) {
            case Level::DEBUG:
                color = Color::BRIGHT_BLACK;
                prefix = "[DEBUG]";
                break;
            case Level::INFO:
                color = Color::BRIGHT_CYAN;
                prefix = "[INFO ]";
                break;
            case Level::SUCCESS:
                color = Color::BRIGHT_GREEN;
                prefix = "[OK   ]";
                break;
            case Level::WARNING:
                color = Color::BRIGHT_YELLOW;
                prefix = "[WARN ]";
                break;
            case Level::ERROR:
                color = Color::BRIGHT_RED;
                prefix = "[ERROR]";
                break;
        }
        
        std::cout << "\033[" << static_cast<int>(color) << "m"
                  << prefix << " "
                  << std::left << std::setw(15) << ("[" + component + "]")
                  << message
                  << "\033[0m" << std::endl;
    }
    
    static void separator() {
        std::cout << "\033[90m" << std::string(80, '-') << "\033[0m" << std::endl;
    }
    
    static void header(const std::string& text) {
        separator();
        std::cout << "\033[1;36m" << ">>> " << text << " <<<\033[0m" << std::endl;
        separator();
    }
    
    static void printBox(const std::string& title, const std::string& content) {
        std::cout << "\033[94m+- " << title << " " << std::string(75 - title.length(), '-') << "+\033[0m" << std::endl;

        std::istringstream iss(content);
        std::string line;
        while (std::getline(iss, line)) {
            std::cout << "\033[94m|\033[0m " << std::left << std::setw(77) << line << "\033[94m|\033[0m" << std::endl;
        }

        std::cout << "\033[94m+" << std::string(78, '-') << "+\033[0m" << std::endl;
    }
};

}

#define LOG_DEBUG(component, message) ::utils::Logger::log(::utils::Logger::Level::DEBUG, component, message)
#define LOG_INFO(component, message) ::utils::Logger::log(::utils::Logger::Level::INFO, component, message)
#define LOG_SUCCESS(component, message) ::utils::Logger::log(::utils::Logger::Level::SUCCESS, component, message)
#define LOG_WARNING(component, message) ::utils::Logger::log(::utils::Logger::Level::WARNING, component, message)
#define LOG_ERROR(component, message) ::utils::Logger::log(::utils::Logger::Level::ERROR, component, message)