#pragma once
#include "token.h"
#include <vector>
#include <unordered_map>
#include <string_view>

namespace query_engine {

    class Lexer {
    public:
        explicit Lexer(const std::string& source);
        std::vector<Token> tokenize();

    private:
        std::string source;
        std::string_view sourceView;
        size_t current = 0;
        size_t line = 1;
        size_t column = 1;
        static thread_local char upperBuffer[256];

        static const std::unordered_map<std::string_view, TokenType> keywords;

        bool isAtEnd() const { return current >= source.length(); }
        char advance();
        char peek() const;
        char peekNext() const;
        bool match(char expected);

        void skipWhitespace();
        void skipComment();

        Token scanToken();
        Token identifier();
        Token number();
        Token string();

        Token makeToken(TokenType type, const std::string& lexeme);
        Token errorToken(const std::string& message);
    };

}