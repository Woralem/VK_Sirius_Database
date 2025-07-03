#pragma once
#include <iostream>
#include <string>
#include <set>

template<typename T>
std::ostream& operator<<(std::ostream& os, const std::set<T>& s) {
    os << "{ ";
    for (auto it = s.begin(); it != s.end(); ) {
        os << *it;
        if (++it != s.end()) {
            os << ", ";
        }
    }
    os << " }";
    return os;
}

inline void logTestStart(const std::string& testName) {
    std::cout << "\n\033[1;36m=== TEST: " << testName << " ===\033[0m\n";
}

inline void logStep(const std::string& step) {
    std::cout << "\033[94m[STEP]\033[0m " << step << "\n";
}

inline void logSuccess(const std::string& message) {
    std::cout << "\033[92m[SUCCESS]\033[0m " << message << "\n";
}

inline void logError(const std::string& message) {
    std::cout << "\033[91m[ERROR]\033[0m " << message << "\n";
}

inline void logDebug(const std::string& message) {
    std::cout << "\033[90m[DEBUG]\033[0m " << message << "\n";
}