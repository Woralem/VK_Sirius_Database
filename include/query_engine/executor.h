#pragma once
#include "ast.h"
#include <nlohmann/json.hpp>
#include <functional>
#include <memory>

namespace query_engine {

// Forward declaration for storage interface
struct StorageInterface {
    virtual ~StorageInterface() = default;

    virtual bool createTable(const std::string& tableName,
                           const std::vector<ColumnDef*>& columns) = 0;
    virtual bool insertRow(const std::string& tableName,
                          const std::vector<std::string>& columns,
                          const std::vector<Value>& values) = 0;
    virtual nlohmann::json selectRows(const std::string& tableName,
                                     const std::vector<std::string>& columns,
                                     std::function<bool(const nlohmann::json&)> predicate) = 0;
    virtual int updateRows(const std::string& tableName,
                          const std::vector<std::pair<std::string, Value>>& assignments,
                          std::function<bool(const nlohmann::json&)> predicate) = 0;
    virtual int deleteRows(const std::string& tableName,
                          std::function<bool(const nlohmann::json&)> predicate) = 0;
};

class QueryExecutor {
public:
    explicit QueryExecutor(std::shared_ptr<StorageInterface> storage);

    nlohmann::json execute(const ASTNodePtr& ast);

private:
    std::shared_ptr<StorageInterface> storage;

    // Execute different statement types
    nlohmann::json executeSelect(const SelectStmt* stmt);
    nlohmann::json executeInsert(const InsertStmt* stmt);
    nlohmann::json executeUpdate(const UpdateStmt* stmt);
    nlohmann::json executeDelete(const DeleteStmt* stmt);
    nlohmann::json executeCreateTable(const CreateTableStmt* stmt);

    // Evaluate expressions
    Value evaluateExpression(const ASTNode* expr, const nlohmann::json& row);
    bool evaluatePredicate(const ASTNode* expr, const nlohmann::json& row);

    // Helper methods
    std::function<bool(const nlohmann::json&)> createPredicate(const ASTNode* whereClause);
    std::string valueToString(const Value& value);
};

}