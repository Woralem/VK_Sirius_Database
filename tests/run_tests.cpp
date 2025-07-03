#include "test_framework.h"

void addFinalBossTest(tests::TestFramework& framework);

int main() {
    tests::TestFramework framework;

    std::cout << "\033[1;35m";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                   LIKE OPERATOR TEST SUITE                   ║\n";
    std::cout << "║                     Testing % and _ wildcards                ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "\033[0m\n";

    addFinalBossTest(framework);

    framework.runAllTests();
    return 0;
}