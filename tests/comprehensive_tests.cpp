#include "test_framework.h"
#include <iostream>

void addDataTypeTests(tests::TestFramework& framework);
void addSubqueryTests(tests::TestFramework& framework);
void addAdditionalSubqueryTests(tests::TestFramework& framework);

int main() {
    tests::TestFramework framework;

    std::cout << "\033[1;35m";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║               COMPREHENSIVE BACKEND TEST SUITE               ║\n";
    std::cout << "║         Testing Basic Functionality + Subqueries            ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "\033[0m\n";

    addDataTypeTests(framework);
    addSubqueryTests(framework);
    addAdditionalSubqueryTests(framework);

    framework.runAllTests();
    return 0;
}