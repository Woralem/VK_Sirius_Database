#pragma once
#include "../config.h"
#include "../utils.h"
#include "ast.h"
#include <nlohmann/json.hpp>
#include <functional>
#include <memory>
#include <unordered_map>

namespace query_engine {

#if HAS_CONCEPTS
// Concept for predicate functions
template<typename T>
concept JsonPredicate = std::invocable<T, const nlohmann::json&> &&
                       std::same_as<std::invoke_result_t<T, const nlohmann::json&>, bool>;

template<typename T>
concept StorageType = requires(T t) {
    { t.createTable(std::string{}, std::vector<ColumnDef*>{}) } -> std::same_as<bool>;
    { t.insertRow(std::string{}, std::vector<std::string>{}, std::vector<Value>{}) } -> std::same_as<bool>;
};
#endif

struct StorageInterface {
    virtual ~StorageInterface() = default;

    [[nodiscard]] virtual bool createTable(const std::string& tableName,
                           const std::vector<ColumnDef*>& columns,
                           const TableOptions& options = TableOptions()) = 0;
    [[nodiscard]] virtual bool insertRow(const std::string& tableName,
                          const std::vector<std::string>& columns,
                          const std::vector<Value>& values) = 0;
    [[nodiscard]] virtual nlohmann::json selectRows(const std::string& tableName,
                                     const std::vector<std::string>& columns,
                                     std::function<bool(const nlohmann::json&)> predicate) = 0;
    [[nodiscard]] virtual int updateRows(const std::string& tableName,
                          const std::vector<std::pair<std::string, Value>>& assignments,
                          std::function<bool(const nlohmann::json&)> predicate) = 0;
    [[nodiscard]] virtual int deleteRows(const std::string& tableName,
                          std::function<bool(const nlohmann::json&)> predicate) = 0;

    // ALTER TABLE operations
    [[nodiscard]] virtual bool renameTable(const std::string& oldName, const std::string& newName) = 0;
    [[nodiscard]] virtual bool renameColumn(const std::string& tableName, const std::string& oldColumnName, const std::string& newColumnName) = 0;
    [[nodiscard]] virtual bool alterColumnType(const std::string& tableName, const std::string& columnName, DataType newType) = 0;
    [[nodiscard]] virtual bool dropColumn(const std::string& tableName, const std::string& columnName) = 0;

    // DROP TABLE operation
    [[nodiscard]] virtual bool dropTable(const std::string& tableName) = 0;
};

class QueryExecutor {
public:
    explicit QueryExecutor(std::shared_ptr<StorageInterface> storage);
    virtual ~QueryExecutor() = default;

    [[nodiscard]] nlohmann::json execute(const ASTNodePtr& ast);

protected:
    std::shared_ptr<StorageInterface> storage;
    bool enableLogging = false;

    // Execute different statement types
    [[nodiscard]] nlohmann::json executeSelect(const SelectStmt* stmt);
    [[nodiscard]] nlohmann::json executeInsert(const InsertStmt* stmt);
    [[nodiscard]] nlohmann::json executeUpdate(const UpdateStmt* stmt);
    [[nodiscard]] nlohmann::json executeDelete(const DeleteStmt* stmt);
    [[nodiscard]] nlohmann::json executeCreateTable(const CreateTableStmt* stmt);
    [[nodiscard]] nlohmann::json executeAlterTable(const AlterTableStmt* stmt);
    [[nodiscard]] nlohmann::json executeDropTable(const DropTableStmt* stmt);

    // Evaluate expressions
    [[nodiscard]] Value evaluateExpression(const ASTNode* expr, const nlohmann::json& row);
    [[nodiscard]] bool evaluatePredicate(const ASTNode* expr, const nlohmann::json& row);

    // Helper methods
    [[nodiscard]] std::function<bool(const nlohmann::json&)> createPredicate(const ASTNode* whereClause);

    // Optimized string building
    void appendValueToString(utils::StringBuilder& builder, const Value& value);
    [[nodiscard]] std::string valueToString(const Value& value);

private:
    // Cache for compiled predicates with smart cleanup
    mutable std::unordered_map<const ASTNode*,
        std::function<bool(const nlohmann::json&)>> predicateCache;
    mutable size_t cacheCleanupCounter = 0;
    static constexpr size_t CACHE_CLEANUP_INTERVAL = 1000;
    void cleanupCacheIfNeeded() const;
};

// Optimized executor with enhanced caching
class OptimizedQueryExecutor : public QueryExecutor {
public:
    explicit OptimizedQueryExecutor(std::shared_ptr<StorageInterface> storage)
        : QueryExecutor(storage) {
        enableLogging = false;
    }

    void setLoggingEnabled(bool enabled) { enableLogging = enabled; }

    [[nodiscard]] nlohmann::json executeBatch(const std::vector<ASTNodePtr>& statements);
};

}