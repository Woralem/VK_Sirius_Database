#include "test_framework.h"

// Объявления только тех функций, которые реально реализованы
void addBasicOperationTests(tests::TestFramework& framework);
void addDataTypeTests(tests::TestFramework& framework);
void addNewJsonFormatTests(tests::TestFramework& framework);

int main() {
    tests::TestFramework framework;

    std::cout << "\033[1;35m";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║               COMPREHENSIVE BACKEND TEST SUITE               ║\n";
    std::cout << "║              Testing Basic Functionality                     ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "\033[0m\n";

    // Добавляем только реализованные тесты
    addBasicOperationTests(framework);
    addDataTypeTests(framework);
    addNewJsonFormatTests(framework);

    framework.runAllTests();
    return 0;
}