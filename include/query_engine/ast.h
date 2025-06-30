#pragma once
#include <memory>
#include <vector>
#include <string>
#include <variant>
#include <ostream>

namespace query_engine {

// Forward declarations
struct ASTNode;
using ASTNodePtr = std::unique_ptr<ASTNode>;

// Value types
using Value = std::variant<std::monostate, int, double, std::string, bool>;

// Base AST Node
struct ASTNode {
    enum class Type {
        // Statements
        SELECT_STMT, INSERT_STMT, UPDATE_STMT, DELETE_STMT, CREATE_TABLE_STMT,

        // Expressions
        BINARY_EXPR, UNARY_EXPR, LITERAL_EXPR, IDENTIFIER_EXPR,

        // Others
        COLUMN_DEF, TABLE_REF, COLUMN_REF, VALUE_LIST
    };

    Type type;
    virtual ~ASTNode() = default;

protected:
    ASTNode(Type type) : type(type) {}
};

// Column Definition for CREATE TABLE
struct ColumnDef : public ASTNode {
    std::string name;
    std::string dataType;
    bool notNull = false;
    bool primaryKey = false;

    ColumnDef() : ASTNode(Type::COLUMN_DEF) {}
};

// Expressions
struct Expression : public ASTNode {
    Expression(Type type) : ASTNode(type) {}
};

struct LiteralExpr : public Expression {
    Value value;

    LiteralExpr(const Value& val) : Expression(Type::LITERAL_EXPR), value(val) {}
};

struct IdentifierExpr : public Expression {
    std::string name;

    IdentifierExpr(const std::string& name)
        : Expression(Type::IDENTIFIER_EXPR), name(name) {}
};

struct BinaryExpr : public Expression {
    enum class Operator {
        EQ, NE, LT, GT, LE, GE, AND, OR
    };

    Operator op;
    ASTNodePtr left;
    ASTNodePtr right;

    BinaryExpr(Operator op, ASTNodePtr left, ASTNodePtr right)
        : Expression(Type::BINARY_EXPR), op(op),
          left(std::move(left)), right(std::move(right)) {}
};

// Statements
struct Statement : public ASTNode {
    Statement(Type type) : ASTNode(type) {}
};

struct CreateTableStmt : public Statement {
    std::string tableName;
    std::vector<std::unique_ptr<ColumnDef>> columns;

    CreateTableStmt() : Statement(Type::CREATE_TABLE_STMT) {}
};

struct SelectStmt : public Statement {
    std::vector<std::string> columns;
    std::string tableName;
    ASTNodePtr whereClause;

    SelectStmt() : Statement(Type::SELECT_STMT) {}
};

struct InsertStmt : public Statement {
    std::string tableName;
    std::vector<std::string> columns;
    std::vector<std::vector<Value>> values;

    InsertStmt() : Statement(Type::INSERT_STMT) {}
};

struct UpdateStmt : public Statement {
    std::string tableName;
    std::vector<std::pair<std::string, Value>> assignments;
    ASTNodePtr whereClause;

    UpdateStmt() : Statement(Type::UPDATE_STMT) {}
};

struct DeleteStmt : public Statement {
    std::string tableName;
    ASTNodePtr whereClause;

    DeleteStmt() : Statement(Type::DELETE_STMT) {}
};

inline std::ostream& operator<<(std::ostream& os, const ASTNode::Type& type) {
    switch (type) {
        case ASTNode::Type::SELECT_STMT:        os << "SELECT_STMT"; break;
        case ASTNode::Type::INSERT_STMT:        os << "INSERT_STMT"; break;
        case ASTNode::Type::UPDATE_STMT:        os << "UPDATE_STMT"; break;
        case ASTNode::Type::DELETE_STMT:        os << "DELETE_STMT"; break;
        case ASTNode::Type::CREATE_TABLE_STMT:  os << "CREATE_TABLE_STMT"; break;
        case ASTNode::Type::BINARY_EXPR:       os << "BINARY_EXPR"; break;
        case ASTNode::Type::UNARY_EXPR:        os << "UNARY_EXPR"; break;
        case ASTNode::Type::LITERAL_EXPR:     os << "LITERAL_EXPR"; break;
        case ASTNode::Type::IDENTIFIER_EXPR:  os << "IDENTIFIER_EXPR"; break;
        case ASTNode::Type::COLUMN_DEF:         os << "COLUMN_DEF"; break;
        case ASTNode::Type::TABLE_REF:          os << "TABLE_REF"; break;
        case ASTNode::Type::COLUMN_REF:         os << "COLUMN_REF"; break;
        case ASTNode::Type::VALUE_LIST:         os << "VALUE_LIST"; break;
        default: os << "UNKNOWN_AST_NODE(" << static_cast<int>(type) << ")"; break;
    }
    return os;
}

}