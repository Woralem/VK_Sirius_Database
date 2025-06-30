#include "test_framework.h"

void addLexerTests(tests::TestFramework& framework);
void addParserTests(tests::TestFramework& framework);
void addExecutorTests(tests::TestFramework& framework);

int main() {
    tests::TestFramework framework;

    addLexerTests(framework);
    addParserTests(framework);
    addExecutorTests(framework);

    framework.runAllTests();

    return 0;
}