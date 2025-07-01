#pragma once
#include "token.h"
#include "ast.h"
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <functional>

namespace query_engine {

class Parser {
public:
    explicit Parser(const std::vector<Token>& tokens);
    ASTNodePtr parse();

    bool hasError() const { return !errors.empty(); }
    const std::vector<std::string>& getErrors() const { return errors; }

private:
    std::vector<Token> tokens;
    size_t current = 0;
    std::vector<std::string> errors;

    // Optimized precedence parsing
    enum class Precedence {
        NONE = 0,
        OR,
        AND,
        EQUALITY,
        COMPARISON,
        TERM,
        FACTOR,
        UNARY,
        PRIMARY
    };

    using PrefixFn = std::function<ASTNodePtr(Parser*)>;
    using InfixFn = std::function<ASTNodePtr(Parser*, ASTNodePtr)>;

    struct ParseRule {
        PrefixFn prefix;
        InfixFn infix;
        Precedence precedence;
    };

    static const std::unordered_map<TokenType, ParseRule> parseRules;
    static const std::unordered_map<TokenType, BinaryExpr::Operator> binaryOps;

    // Utility methods
    bool isAtEnd() const;
    Token peek() const;
    Token previous() const;
    Token advance();
    bool check(TokenType type) const;
    bool match(std::initializer_list<TokenType> types);
    Token consume(TokenType type, const std::string& message);
    void error(const std::string& message);
    void synchronize();

    // Statement parsers
    ASTNodePtr statement();
    std::unique_ptr<SelectStmt> selectStatement();
    std::unique_ptr<InsertStmt> insertStatement();
    std::unique_ptr<UpdateStmt> updateStatement();
    std::unique_ptr<DeleteStmt> deleteStatement();
    std::unique_ptr<CreateTableStmt> createTableStatement();

    // Expression parsing
    ASTNodePtr expression();
    ASTNodePtr parsePrecedence(Precedence precedence);

    // Prefix parsers
    static ASTNodePtr primary(Parser* parser);
    static ASTNodePtr grouping(Parser* parser);

    // Infix parsers
    static ASTNodePtr binary(Parser* parser, ASTNodePtr left);

    // Helper parsers
    std::vector<std::string> parseColumnList();
    std::vector<Value> parseValueList();
    std::unique_ptr<ColumnDef> parseColumnDef();
};

}