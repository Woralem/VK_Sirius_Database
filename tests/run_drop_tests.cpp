#include "test_framework.h"

void addDropTests(tests::TestFramework& framework);

int main() {
    tests::TestFramework framework;

    std::cout << "\033[1;35m";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                  DROP OPERATIONS TEST SUITE                  ║\n";
    std::cout << "║              Testing DROP TABLE and DROP COLUMN              ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "\033[0m\n";

    addDropTests(framework);

    framework.runAllTests();
    return 0;
}