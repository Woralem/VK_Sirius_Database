#pragma once
#include "token.h"
#include <vector>
#include <unordered_map>
namespace query_engine { std::string tokenTypeToString(TokenType type); }
namespace query_engine {

    class Lexer {
    public:
        explicit Lexer(const std::string& source);
        std::vector<Token> tokenize();

    private:
        std::string source;
        size_t current = 0;
        size_t line = 1;
        size_t column = 1;

        static const std::unordered_map<std::string, TokenType> keywords;

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