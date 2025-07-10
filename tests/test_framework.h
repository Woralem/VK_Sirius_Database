#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <iomanip>

namespace tests {

class TestFramework {
public:
    struct TestCase {
        std::string name;
        std::function<bool()> testFunc;
        std::string description;
    };

private:
    std::vector<TestCase> testCases;
    int passed = 0;
    int failed = 0;

public:
    void addTest(const std::string& name, std::function<bool()> testFunc,
                 const std::string& description = "") {
        testCases.push_back({name, testFunc, description});
    }

    void runAllTests() {
        std::cout << "\n\033[1;36m";
        std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
        std::cout << "║                      RUNNING TESTS                           ║\n";
        std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
        std::cout << "\033[0m\n";

        for (const auto& test : testCases) {
            runTest(test);
        }

        printSummary();
    }

private:
    void runTest(const TestCase& test) {
        std::cout << "\033[94m[TEST]\033[0m " << std::left << std::setw(50) << test.name;

        try {
            bool result = test.testFunc();
            if (result) {
                std::cout << "\033[92m[PASS]\033[0m\n";
                passed++;
            } else {
                std::cout << "\033[91m[FAIL]\033[0m\n";
                failed++;
            }
        } catch (const std::exception& e) {
            std::cout << "\033[91m[ERROR]\033[0m " << e.what() << "\n";
            failed++;
        }
    }

    void printSummary() {
        std::cout << "\n\033[1;36m" << std::string(60, '=') << "\033[0m\n";
        std::cout << "\033[1;32mPassed: " << passed << "\033[0m | ";
        std::cout << "\033[1;31mFailed: " << failed << "\033[0m | ";
        std::cout << "\033[1;34mTotal: " << (passed + failed) << "\033[0m\n";

        if (failed == 0) {
            std::cout << "\033[1;32m🎉 All tests passed!\033[0m\n";
        } else {
            std::cout << "\033[1;31m❌ Some tests failed!\033[0m\n";
        }
        std::cout << "\033[1;36m" << std::string(60, '=') << "\033[0m\n\n";
    }
};

#define ASSERT_TRUE(condition) \
    do { \
        if (!(condition)) { \
            std::cerr << "\033[91mASSERTION FAILED: " << #condition << " at " << __FILE__ << ":" << __LINE__ << "\033[0m" << std::endl; \
            return false; \
        } \
    } while(0)

#define ASSERT_FALSE(condition) \
    do { \
        if (condition) { \
            std::cerr << "\033[91mASSERTION FAILED: !(" << #condition << ") at " << __FILE__ << ":" << __LINE__ << "\033[0m" << std::endl; \
            return false; \
        } \
    } while(0)

#define ASSERT_EQ(expected, actual) \
    do { \
        if ((expected) != (actual)) { \
            std::cerr << "\033[91mASSERTION FAILED: Expected " << (expected) << " but got " << (actual) << " at " << __FILE__ << ":" << __LINE__ << "\033[0m" << std::endl; \
            return false; \
        } \
    } while(0)

#define ASSERT_NOT_NULL(ptr) \
    do { \
        if ((ptr) == nullptr) { \
            std::cerr << "\033[91mASSERTION FAILED: " << #ptr << " is null at " << __FILE__ << ":" << __LINE__ << "\033[0m" << std::endl; \
            return false; \
        } \
    } while(0)

} // namespace tests