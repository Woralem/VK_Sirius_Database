#pragma once
#include "../config.h"
#include "token.h"
#include "ast.h"
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <functional>
#include <set>
#include <span>
#include <concepts>

namespace query_engine {

template<typename T>
concept TokenRange = std::ranges::range<T> &&
                    std::same_as<std::ranges::range_value_t<T>, Token>;

class Parser {
public:
    explicit Parser(std::span<const Token> tokens);
    explicit Parser(const std::vector<Token>& tokens);

    [[nodiscard]] ASTNodePtr parse();

    [[nodiscard]] constexpr bool hasError() const noexcept { return !errors.empty(); }
    [[nodiscard]] constexpr const std::vector<std::string>& getErrors() const noexcept { return errors; }

private:
    std::vector<Token> tokens;
    size_t current = 0;
    std::vector<std::string> errors;

    [[nodiscard]] std::vector<ASTNodePtr> parse_all();

    enum class Precedence : int {
        NONE = 0,
        OR = 1,
        AND = 2,
        EQUALITY = 3,
        COMPARISON = 4,
        TERM = 5,
        FACTOR = 6,
        UNARY = 7,
        PRIMARY = 8
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
    [[nodiscard]] constexpr bool isAtEnd() const noexcept;
    [[nodiscard]] const Token& peek() const noexcept;
    [[nodiscard]] const Token& previous() const noexcept;
    Token advance();
    [[nodiscard]] bool check(TokenType type) const noexcept;
    bool match(std::initializer_list<TokenType> types);
    Token consume(TokenType type, std::string_view message);
    void error(std::string_view message);
    void synchronize();

    // Statement parsers
    [[nodiscard]] ASTNodePtr statement();
    [[nodiscard]] std::unique_ptr<SelectStmt> selectStatement();
    [[nodiscard]] std::unique_ptr<InsertStmt> insertStatement();
    [[nodiscard]] std::unique_ptr<UpdateStmt> updateStatement();
    [[nodiscard]] std::unique_ptr<DeleteStmt> deleteStatement();
    [[nodiscard]] std::unique_ptr<CreateTableStmt> createTableStatement();
    [[nodiscard]] std::unique_ptr<AlterTableStmt> alterTableStatement();
    [[nodiscard]] std::unique_ptr<DropTableStmt> dropTableStatement();

    // Expression parsing
    [[nodiscard]] ASTNodePtr expression();
    [[nodiscard]] ASTNodePtr parsePrecedence(Precedence precedence);

    // Prefix parsers
    [[nodiscard]] static ASTNodePtr primary(Parser* parser);
    [[nodiscard]] static ASTNodePtr grouping(Parser* parser);

    // Infix parsers
    [[nodiscard]] static ASTNodePtr binary(Parser* parser, ASTNodePtr left);

    // Helper parsers with optimizations
    [[nodiscard]] std::vector<std::string> parseColumnList();
    [[nodiscard]] std::vector<Value> parseValueList();
    [[nodiscard]] std::unique_ptr<ColumnDef> parseColumnDef();

    // Parse table options
    [[nodiscard]] TableOptions parseTableOptions();
    [[nodiscard]] std::set<DataType> parseDataTypeList();
    [[nodiscard]] std::set<char> parseCharacterList();

    // Performance optimizations
    void reserveErrorCapacity();
};

}