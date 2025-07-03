#include "test_framework.h"

void addFinalBossTest(tests::TestFramework& framework);

int main() {
    tests::TestFramework framework;

    addFinalBossTest(framework);

    framework.runAllTests();
    return 0;
}