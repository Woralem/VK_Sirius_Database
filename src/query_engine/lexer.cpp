#include "query_engine/lexer.h"
#include "utils/logger.h"
#include <cctype>
#include <algorithm>
#include <sstream>

namespace query_engine {

// Функция для преобразования типа токена в строку
std::string tokenTypeToString(TokenType type) {
    switch (type) {
        case TokenType::SELECT: return "SELECT";
        case TokenType::FROM: return "FROM";
        case TokenType::WHERE: return "WHERE";
        case TokenType::INSERT: return "INSERT";
        case TokenType::INTO: return "INTO";
        case TokenType::VALUES: return "VALUES";
        case TokenType::UPDATE_KEYWORD: return "UPDATE";
        case TokenType::SET: return "SET";
        case TokenType::DELETE_KEYWORD: return "DELETE";
        case TokenType::CREATE: return "CREATE";
        case TokenType::TABLE: return "TABLE";
        case TokenType::AND: return "AND";
        case TokenType::OR: return "OR";
        case TokenType::NOT: return "NOT";
        case TokenType::NULL_TOKEN: return "NULL";
        case TokenType::IDENTIFIER: return "IDENTIFIER";
        case TokenType::STRING_LITERAL: return "STRING_LITERAL";
        case TokenType::NUMBER_LITERAL: return "NUMBER_LITERAL";
        case TokenType::EQUALS: return "EQUALS";
        case TokenType::NOT_EQUALS: return "NOT_EQUALS";
        case TokenType::LESS_THAN: return "LESS_THAN";
        case TokenType::GREATER_THAN: return "GREATER_THAN";
        case TokenType::LESS_EQUALS: return "LESS_EQUALS";
        case TokenType::GREATER_EQUALS: return "GREATER_EQUALS";
        case TokenType::LEFT_PAREN: return "LEFT_PAREN";
        case TokenType::RIGHT_PAREN: return "RIGHT_PAREN";
        case TokenType::COMMA: return "COMMA";
        case TokenType::SEMICOLON: return "SEMICOLON";
        case TokenType::ASTERISK: return "ASTERISK";
        case TokenType::END_OF_FILE: return "EOF";
        case TokenType::UNKNOWN: return "UNKNOWN";
        default: return "?";
    }
}

const std::unordered_map<std::string, TokenType> Lexer::keywords = {
    {"SELECT", TokenType::SELECT},
    {"FROM", TokenType::FROM},
    {"WHERE", TokenType::WHERE},
    {"INSERT", TokenType::INSERT},
    {"INTO", TokenType::INTO},
    {"VALUES", TokenType::VALUES},
    {"UPDATE", TokenType::UPDATE_KEYWORD},
    {"SET", TokenType::SET},
    {"DELETE", TokenType::DELETE_KEYWORD},
    {"CREATE", TokenType::CREATE},
    {"TABLE", TokenType::TABLE},
    {"AND", TokenType::AND},
    {"OR", TokenType::OR},
    {"NOT", TokenType::NOT},
    {"NULL", TokenType::NULL_TOKEN}
};

Lexer::Lexer(const std::string& source) : source(source) {
    // LOG_INFO("Lexer", "Initialized with input: \"" + source + "\"");
}

std::vector<Token> Lexer::tokenize() {
    // LOG_INFO("Lexer", "Starting tokenization process");
    std::vector<Token> tokens;

    while (!isAtEnd()) {
        skipWhitespace();
        if (isAtEnd()) break;

        Token token = scanToken();
        tokens.push_back(token);
    }

    tokens.push_back(makeToken(TokenType::END_OF_FILE, ""));
    // LOG_SUCCESS("Lexer", "Tokenization complete. Total tokens: " + std::to_string(tokens.size()));

    return tokens;
}

char Lexer::advance() {
    char c = source[current++];
    if (c == '\n') {
        line++;
        column = 1;
    } else {
        column++;
    }
    return c;
}

char Lexer::peek() const {
    if (isAtEnd()) return '\0';
    return source[current];
}

char Lexer::peekNext() const {
    if (current + 1 >= source.length()) return '\0';
    return source[current + 1];
}

bool Lexer::match(char expected) {
    if (isAtEnd()) return false;
    if (source[current] != expected) return false;
    advance();
    return true;
}

void Lexer::skipWhitespace() {
    while (!isAtEnd()) {
        char c = peek();
        switch (c) {
            case ' ':
            case '\r':
            case '\t':
            case '\n':
                advance();
                break;
            case '-':
                if (peekNext() == '-') {
                    // LOG_DEBUG("Lexer", "Skipping comment");
                    while (!isAtEnd() && peek() != '\n') advance();
                } else {
                    return;
                }
                break;
            default:
                return;
        }
    }
}

Token Lexer::scanToken() {
    char c = advance();

    switch (c) {
        case '(': return makeToken(TokenType::LEFT_PAREN, "(");
        case ')': return makeToken(TokenType::RIGHT_PAREN, ")");
        case ',': return makeToken(TokenType::COMMA, ",");
        case ';': return makeToken(TokenType::SEMICOLON, ";");
        case '*': return makeToken(TokenType::ASTERISK, "*");

        case '=': return makeToken(TokenType::EQUALS, "=");
        case '<':
            if (match('=')) return makeToken(TokenType::LESS_EQUALS, "<=");
            if (match('>')) return makeToken(TokenType::NOT_EQUALS, "<>");
            return makeToken(TokenType::LESS_THAN, "<");
        case '>':
            if (match('=')) return makeToken(TokenType::GREATER_EQUALS, ">=");
            return makeToken(TokenType::GREATER_THAN, ">");
        case '!':
            if (match('=')) return makeToken(TokenType::NOT_EQUALS, "!=");
            return errorToken("Unexpected character '!'");

        case '\'':
            return string();

        default:
            if (std::isdigit(c)) {
                return number();
            }
            if (std::isalpha(c) || c == '_') {
                return identifier();
            }
            return errorToken("Unexpected character");
    }
}

Token Lexer::identifier() {
    size_t start = current - 1;

    while (!isAtEnd() && (std::isalnum(peek()) || peek() == '_')) {
        advance();
    }

    std::string text = source.substr(start, current - start);
    std::string upperText = text;
    std::transform(upperText.begin(), upperText.end(), upperText.begin(), ::toupper);

    auto it = keywords.find(upperText);
    TokenType type = (it != keywords.end()) ? it->second : TokenType::IDENTIFIER;

    return makeToken(type, text);
}

Token Lexer::number() {
    size_t start = current - 1;
    bool isFloat = false;

    while (!isAtEnd() && std::isdigit(peek())) {
        advance();
    }

    if (!isAtEnd() && peek() == '.' && std::isdigit(peekNext())) {
        isFloat = true;
        advance(); // consume '.'
        while (!isAtEnd() && std::isdigit(peek())) {
            advance();
        }
    }

    std::string text = source.substr(start, current - start);
    Token token = makeToken(TokenType::NUMBER_LITERAL, text);

    try {
        if (isFloat) {
            token.value = std::stod(text);
        } else {
            token.value = std::stoi(text);
        }
    } catch(const std::out_of_range&) {
        return errorToken("Number literal is out of range");
    }

    return token;
}

Token Lexer::string() {
    std::string value;
    size_t startLexeme = current - 1;

    while (!isAtEnd()) {
        if (peek() == '\'') {
            if (peekNext() == '\'') {
                advance();
                value += '\'';
            } else {
                break;
            }
        } else {
            value += peek();
        }
        advance();
    }


    if (isAtEnd()) {
        return errorToken("Unterminated string");
    }

    advance();

    std::string lexeme = source.substr(startLexeme, current - startLexeme);
    Token token = makeToken(TokenType::STRING_LITERAL, lexeme);
    token.value = value;
    return token;
}

Token Lexer::makeToken(TokenType type, const std::string& lexeme) {
    return Token(type, lexeme, line, column - lexeme.length());
}

Token Lexer::errorToken(const std::string& message) {
    //LOG_ERROR("Lexer", message + " at " + std::to_string(line) + ":" + std::to_string(column));
    return Token(TokenType::UNKNOWN, source.substr(current - 1, 1), line, column - 1);
}

}