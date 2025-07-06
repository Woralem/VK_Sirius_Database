#include "api/http_server.h"
#include <iostream>
#include <numeric>
#include <chrono>
#include <format>

// Проверка доступности C++20 features
void checkCpp20Features() {
    std::cout << "\n=== C++20 Features Status ===" << std::endl;

#ifdef __cpp_lib_format
    std::cout << "✅ std::format is available" << std::endl;
#else
    std::cout << "❌ std::format is NOT available" << std::endl;
#endif

#ifdef __cpp_lib_ranges
    std::cout << "✅ std::ranges is available" << std::endl;
#else
    std::cout << "❌ std::ranges is NOT available" << std::endl;
#endif

#ifdef __cpp_lib_span
    std::cout << "✅ std::span is available" << std::endl;
#else
    std::cout << "❌ std::span is NOT available" << std::endl;
#endif

#ifdef __cpp_concepts
    std::cout << "✅ Concepts are available" << std::endl;
#else
    std::cout << "❌ Concepts are NOT available" << std::endl;
#endif

#ifdef __cpp_lib_three_way_comparison
    std::cout << "✅ Three-way comparison is available" << std::endl;
#else
    std::cout << "❌ Three-way comparison is NOT available" << std::endl;
#endif

    std::cout << "============================\n" << std::endl;
}

void runPerformanceBenchmark() {
    std::cout << "\n=== Performance Benchmark ===" << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 100000; ++i) {
        auto str = std::format("Table {} with {} columns and {} rows",
                              "test_table", i % 100, i * 2);
        volatile auto len = str.length();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << std::format("std::format benchmark: {}ms", duration.count()) << std::endl;

    // Тест std::ranges
    start = std::chrono::high_resolution_clock::now();

    std::vector<int> numbers(100000);
    std::iota(numbers.begin(), numbers.end(), 1);

    for (int i = 0; i < 1000; ++i) {
        auto count = std::ranges::count_if(numbers, [](int n) { return n % 2 == 0; });
        volatile auto result = count; // Предотвращаем оптимизацию
    }

    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << std::format("std::ranges benchmark: {}ms", duration.count()) << std::endl;
    std::cout << "==============================\n" << std::endl;
}

int main() {
    std::cout << "=== Compiler Info ===" << std::endl;

#ifdef _MSC_VER
    std::cout << std::format("MSVC Version: {}", _MSC_VER) << std::endl;
    #if _MSC_VER >= 1930
        std::cout << "MSVC 2022 or newer detected" << std::endl;
    #endif
#endif

    std::cout << std::format("C++ Standard: {}", __cplusplus) << std::endl;

#if __cplusplus >= 202002L
    std::cout << "🚀 C++20 is enabled!" << std::endl;
#elif __cplusplus >= 201703L
    std::cout << "C++17 is enabled" << std::endl;
#else
    std::cout << "⚠️  WARNING: Old C++ standard!" << std::endl;
#endif

    checkCpp20Features();
    runPerformanceBenchmark();

    HttpServer server;
    server.run(8080);
    return 0;
}