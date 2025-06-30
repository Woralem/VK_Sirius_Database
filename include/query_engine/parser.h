#pragma once
#include "token.h"
#include "ast.h"
#include <vector>
#include <memory>
#include <string>

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

        ASTNodePtr expression();
        ASTNodePtr logic_or();
        ASTNodePtr logic_and();
        ASTNodePtr equality();
        ASTNodePtr comparison();
        ASTNodePtr term();      // Для сложения/вычитания (пока не используется, но для будущего)
        ASTNodePtr factor();    // Для умножения/деления (пока не используется, но для будущего)
        ASTNodePtr primary();

        // Helper parsers
        std::vector<std::string> parseColumnList();
        std::vector<Value> parseValueList();
        std::unique_ptr<ColumnDef> parseColumnDef();
    };

}