#pragma once
#include "token.h"
#include <vector>
#include <unordered_map>
#include <string_view>
#include <array>
#include <algorithm>

namespace query_engine {

    class Lexer {
    public:
        explicit Lexer(std::string_view source);
        [[nodiscard]] std::vector<Token> tokenize();

    private:
        std::string source;
        std::string_view sourceView;
        size_t current = 0;
        size_t line = 1;
        size_t column = 1;
        static thread_local std::array<char, 256> upperBuffer;

        static const std::unordered_map<std::string_view, TokenType> keywords;

        [[nodiscard]] constexpr bool isAtEnd() const noexcept { return current >= source.length(); }
        char advance();
        [[nodiscard]] char peek() const;
        [[nodiscard]] char peekNext() const;
        bool match(char expected);

        void skipWhitespace();
        void skipComment();

        [[nodiscard]] Token scanToken();
        [[nodiscard]] Token identifier();
        [[nodiscard]] Token number();
        [[nodiscard]] Token string();

        [[nodiscard]] Token makeToken(TokenType type, std::string_view lexeme);
        [[nodiscard]] Token errorToken(std::string_view message);
    };

}