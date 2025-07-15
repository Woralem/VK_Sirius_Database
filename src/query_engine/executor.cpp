#include "query_engine/executor.h"
#include "utils/logger.h"
#include "query_engine/ast.h"
#include "config.h"
#include "utils.h"
#include <regex>
#include <format>
#include <ranges>
#include <type_traits>

namespace query_engine {

template<typename T>
concept StringLike = std::convertible_to<T, std::string_view>;

bool matchLikePattern(std::string_view text, std::string_view pattern) {
    size_t tLen = text.length();
    size_t pLen = pattern.length();
    size_t tIdx = 0;
    size_t pIdx = 0;
    size_t tStar = std::string::npos;
    size_t pStar = std::string::npos;

    while (tIdx < tLen) {
        if (pIdx < pLen && (pattern[pIdx] == text[tIdx] || pattern[pIdx] == '_')) {
            tIdx++;
            pIdx++;
        }
        else if (pIdx < pLen && pattern[pIdx] == '%') {
            tStar = tIdx;
            pStar = pIdx;
            pIdx++;
        }
        else if (pStar != std::string::npos) {
            pIdx = pStar + 1;
            tStar++;
            tIdx = tStar;
        }
        else {
            return false;
        }
    }

    while (pIdx < pLen && pattern[pIdx] == '%') {
        pIdx++;
    }

    return pIdx == pLen;
}

std::string astNodeTypeToString(ASTNode::Type type) {
    switch (type) {
        case ASTNode::Type::SELECT_STMT: return "SELECT";
        case ASTNode::Type::INSERT_STMT: return "INSERT";
        case ASTNode::Type::UPDATE_STMT: return "UPDATE";
        case ASTNode::Type::DELETE_STMT: return "DELETE";
        case ASTNode::Type::CREATE_TABLE_STMT: return "CREATE TABLE";
        case ASTNode::Type::ALTER_TABLE_STMT: return "ALTER TABLE";
        case ASTNode::Type::DROP_TABLE_STMT: return "DROP TABLE";
        default: return "UNKNOWN";
    }
}

QueryExecutor::QueryExecutor(std::shared_ptr<StorageInterface> storage)
    : storage(storage), enableLogging(true) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Initialized with storage interface");
    }
}

nlohmann::json QueryExecutor::execute(const ASTNodePtr& ast) {
    if (!ast) {
        if (enableLogging) LOGF_ERROR("Executor", "Received null AST");
        return {{"status", "error"}, {"message", "Invalid AST"}};
    }

    if (enableLogging) {
        LOGF_INFO("Executor", "Executing {} statement", astNodeTypeToString(ast->type));
    }

    try {
        nlohmann::json result;

        switch (ast->type) {
            case ASTNode::Type::SELECT_STMT:
                result = executeSelect(static_cast<SelectStmt*>(ast.get()));
                break;
            case ASTNode::Type::INSERT_STMT:
                result = executeInsert(static_cast<InsertStmt*>(ast.get()));
                break;
            case ASTNode::Type::UPDATE_STMT:
                result = executeUpdate(static_cast<UpdateStmt*>(ast.get()));
                break;
            case ASTNode::Type::DELETE_STMT:
                result = executeDelete(static_cast<DeleteStmt*>(ast.get()));
                break;
            case ASTNode::Type::CREATE_TABLE_STMT:
                result = executeCreateTable(static_cast<CreateTableStmt*>(ast.get()));
                break;
            case ASTNode::Type::ALTER_TABLE_STMT:
                result = executeAlterTable(static_cast<AlterTableStmt*>(ast.get()));
                break;
            case ASTNode::Type::DROP_TABLE_STMT:
                result = executeDropTable(static_cast<DropTableStmt*>(ast.get()));
                break;
            default:
                if (enableLogging) LOGF_ERROR("Executor", "Unknown statement type");
                return {{"status", "error"}, {"message", "Unknown statement type"}};
        }

        if (result.contains("status") && result["status"] == "error") {
            if (enableLogging) LOGF_ERROR("Executor", "Execution failed");
            return result;
        }

        if (enableLogging) LOGF_SUCCESS("Executor", "Execution completed successfully");
        return result;

    } catch (const std::runtime_error& e) {
        if (enableLogging) LOGF_ERROR("Executor", "Execution failed: {}", e.what());
        return {{"status", "error"}, {"message", e.what()}};
    } catch (const std::exception& e) {
        if (enableLogging) LOGF_ERROR("Executor", "Unexpected error: {}", e.what());
        return {{"status", "error"}, {"message", std::string("Unexpected error: ") + e.what()}};
    }
}
nlohmann::json QueryExecutor::executeSelect(const SelectStmt* stmt) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Executing SELECT from table: {}", stmt->tableName);

        if (stmt->columns.empty()) {
            LOGF_DEBUG("Executor", "Selecting all columns (*)");
        } else {
            LOGF_DEBUG("Executor", "Selecting {} columns", stmt->columns.size());
        }
    }

    auto predicate = createPredicate(stmt->whereClause.get());
    auto result = storage->selectRows(stmt->tableName, stmt->columns, predicate);

    if (enableLogging && result.contains("data") && result["data"].is_array()) {
        LOGF_SUCCESS("Executor", "Selected {} rows", result["data"].size());
    }

    return result;
}

nlohmann::json QueryExecutor::executeInsert(const InsertStmt* stmt) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Executing INSERT into table: {}", stmt->tableName);
        LOGF_DEBUG("Executor", "Inserting {} row(s)", stmt->values.size());
    }

    nlohmann::json result;
    int rowsInserted = 0;
    int totalRows = stmt->values.size();
    std::vector<std::string> errors;

    for (size_t i = 0; i < stmt->values.size(); ++i) {
        const auto& valueList = stmt->values[i];
        if (storage->insertRow(stmt->tableName, stmt->columns, valueList)) {
            rowsInserted++;
        } else {
            errors.push_back(std::format("Failed to insert row {}", i + 1));
        }
    }

    if (enableLogging) {
        LOGF_SUCCESS("Executor", "Inserted {} out of {} row(s)", rowsInserted, totalRows);
    }

    if (rowsInserted == 0) {
        result["status"] = "error";
        result["message"] = "Failed to insert any rows";
        if (!errors.empty()) {
            result["details"] = errors;
        }
    } else if (rowsInserted < totalRows) {
        result["status"] = "warning";
        result["message"] = std::format("Inserted {} out of {} rows", rowsInserted, totalRows);
        result["details"] = errors;
    } else {
        result["status"] = "success";
        result["message"] = "All rows inserted successfully";
    }

    result["rows_affected"] = rowsInserted;
    result["total_rows"] = totalRows;
    return result;
}

nlohmann::json QueryExecutor::executeUpdate(const UpdateStmt* stmt) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Executing UPDATE on table: {}", stmt->tableName);
        LOGF_DEBUG("Executor", "Setting {} column(s)", stmt->assignments.size());

        if (!stmt->whereClause) {
            LOGF_WARNING("Executor", "UPDATE without WHERE will affect all rows!");
        }
    }

    auto predicate = createPredicate(stmt->whereClause.get());
    int rowsUpdated = storage->updateRows(stmt->tableName, stmt->assignments, predicate);

    if (enableLogging) {
        LOGF_SUCCESS("Executor", "Updated {} row(s)", rowsUpdated);
    }

    nlohmann::json result;
    result["status"] = "success";
    result["rows_affected"] = rowsUpdated;
    return result;
}

nlohmann::json QueryExecutor::executeDelete(const DeleteStmt* stmt) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Executing DELETE from table: {}", stmt->tableName);

        if (!stmt->whereClause) {
            LOGF_WARNING("Executor", "DELETE without WHERE will delete ALL rows!");
        }
    }

    auto predicate = createPredicate(stmt->whereClause.get());
    int rowsDeleted = storage->deleteRows(stmt->tableName, predicate);

    if (enableLogging) {
        LOGF_SUCCESS("Executor", "Deleted {} row(s)", rowsDeleted);
    }

    nlohmann::json result;
    result["status"] = "success";
    result["rows_affected"] = rowsDeleted;
    return result;
}

nlohmann::json QueryExecutor::executeCreateTable(const CreateTableStmt* stmt) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Executing CREATE TABLE: {}", stmt->tableName);
        LOGF_DEBUG("Executor", "Creating table with {} column(s)", stmt->columns.size());

        if (!stmt->options.allowedTypes.empty()) {
            utils::StringBuilder typesStr(256);
            typesStr << "Allowed types: ";
            for (auto type : stmt->options.allowedTypes) {
                typesStr << std::format("{} ", dataTypeToString(type));
            }
            LOGF_DEBUG("Executor", "{}", std::move(typesStr).str());
        }
        LOGF_DEBUG("Executor", "Max column name length: {}", stmt->options.maxColumnNameLength);
        LOGF_DEBUG("Executor", "Max string length: {}", stmt->options.maxStringLength);
        LOGF_DEBUG("Executor", "GC frequency: {} days", stmt->options.gcFrequencyDays);
    }

    if (!stmt->options.validate()) {
        LOGF_ERROR("Executor", "Invalid table options");
        nlohmann::json result;
        result["status"] = "error";
        result["message"] = "Invalid table options";
        return result;
    }

    std::vector<ColumnDef*> columnPtrs;
    columnPtrs.reserve(stmt->columns.size());

    for (const auto& col : stmt->columns) {
        columnPtrs.push_back(col.get());
    }

    bool success = storage->createTable(stmt->tableName, columnPtrs, stmt->options);

    nlohmann::json result;
    result["status"] = success ? "success" : "error";
    result["message"] = success ? "Table created successfully" : "Failed to create table";

    if (enableLogging) {
        if (success) {
            LOGF_SUCCESS("Executor", "Table '{}' created successfully", stmt->tableName);
        } else {
            LOGF_ERROR("Executor", "Failed to create table '{}'", stmt->tableName);
        }
    }

    return result;
}

nlohmann::json QueryExecutor::executeAlterTable(const AlterTableStmt* stmt) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Executing ALTER TABLE on: {}", stmt->tableName);
    }

    nlohmann::json result;
    bool success = false;
    std::string message;

    switch (stmt->alterType) {
        case AlterTableStmt::AlterType::RENAME_TABLE:
            if (enableLogging) {
                LOGF_DEBUG("Executor", "Renaming table to: {}", stmt->newTableName);
            }
            success = storage->renameTable(stmt->tableName, stmt->newTableName);
            message = success ? "Table renamed successfully" : "Failed to rename table";
            break;

        case AlterTableStmt::AlterType::RENAME_COLUMN:
            if (enableLogging) {
                LOGF_DEBUG("Executor", "Renaming column '{}' to '{}'", stmt->columnName, stmt->newColumnName);
            }
            success = storage->renameColumn(stmt->tableName, stmt->columnName, stmt->newColumnName);
            message = success ? "Column renamed successfully" : "Failed to rename column";
            break;

        case AlterTableStmt::AlterType::ALTER_COLUMN_TYPE:
            if (enableLogging) {
                LOGF_DEBUG("Executor", "Changing column '{}' type to: {}", stmt->columnName, stmt->newDataType);
            }
            success = storage->alterColumnType(stmt->tableName, stmt->columnName, stmt->newParsedType);
            message = success ? "Column type changed successfully" : "Failed to change column type";
            break;

        case AlterTableStmt::AlterType::DROP_COLUMN:
            if (enableLogging) {
                LOGF_DEBUG("Executor", "Dropping column '{}'", stmt->columnName);
            }
            success = storage->dropColumn(stmt->tableName, stmt->columnName);
            message = success ? "Column dropped successfully" : "Failed to drop column";
            break;

        case AlterTableStmt::AlterType::ADD_COLUMN:
            if (enableLogging) {
                LOGF_DEBUG("Executor", "Adding column '{}' to table", stmt->newColumn->name);
            }
            success = storage->addColumn(stmt->tableName, stmt->newColumn.get());
            message = success ? "Column added successfully" : "Failed to add column";
            break;

    }

    result["status"] = success ? "success" : "error";
    result["message"] = message;

    if (enableLogging) {
        if (success) {
            LOGF_SUCCESS("Executor", "{}", message);
        } else {
            LOGF_ERROR("Executor", "{}", message);
        }
    }

    return result;
}

nlohmann::json QueryExecutor::executeDropTable(const DropTableStmt* stmt) {
    if (enableLogging) {
        LOGF_INFO("Executor", "Executing DROP TABLE: {}", stmt->tableName);
    }

    bool success = storage->dropTable(stmt->tableName);
    nlohmann::json result;

    if (success) {
        result["status"] = "success";
        result["message"] = std::format("Table '{}' dropped successfully", stmt->tableName);
        if (enableLogging) {
            LOGF_SUCCESS("Executor", "Table '{}' dropped successfully", stmt->tableName);
        }
    } else {
        if (stmt->ifExists) {
            result["status"] = "success";
            result["message"] = std::format("Table '{}' does not exist (IF EXISTS specified)", stmt->tableName);
            if (enableLogging) {
                LOGF_INFO("Executor", "Table '{}' does not exist (IF EXISTS specified)", stmt->tableName);
            }
        } else {
            result["status"] = "error";
            result["message"] = std::format("Table '{}' does not exist", stmt->tableName);
            if (enableLogging) {
                LOGF_ERROR("Executor", "Table '{}' does not exist", stmt->tableName);
            }
        }
    }

    return result;
}

std::vector<Value> QueryExecutor::executeSubquery(const SubqueryExpr* subquery) {
    //bool oldLogging = enableLogging;
    //enableLogging = true;

    LOGF_DEBUG("Executor", "=== Executing subquery ===");

    auto result = executeSelect(subquery->selectStmt.get());

    if (!result.contains("cells") || !result["cells"].is_array()) {
        // enableLogging = oldLogging;
        throw std::runtime_error("Subquery did not return valid data");
    }

    if (result.contains("header")) {
        LOGF_DEBUG("Executor", "Subquery header exists, size: {}", result["header"].size());
        if (result["header"].is_array()) {
            for (size_t i = 0; i < result["header"].size(); ++i) {
                if (result["header"][i].contains("content")) {
                    LOGF_DEBUG("Executor", "Header[{}]: {}", i, result["header"][i]["content"].get<std::string>());
                }
            }
        }
    } else {
        LOGF_DEBUG("Executor", "Subquery header does not exist");
    }

    auto& cells = result["cells"];
    if (!cells.empty()) {
        const auto& firstRow = cells[0];
        if (firstRow.is_array()) {
            LOGF_DEBUG("Executor", "First row size: {}", firstRow.size());
        }
    }

    if (result.contains("header") && result["header"].is_array()) {
        size_t headerSize = result["header"].size();
        LOGF_DEBUG("Executor", "Checking header size: {}", headerSize);
        if (headerSize != 1) {
            std::string errorMsg = std::format("Subquery must return exactly one column, but got {}", headerSize);
            LOGF_ERROR("Executor", "{}", errorMsg);
            // enableLogging = oldLogging;
            throw std::runtime_error(errorMsg);
        }
    } else {
        if (!cells.empty()) {
            const auto& firstRow = cells[0];
            if (firstRow.is_array()) {
                size_t colCount = firstRow.size();
                LOGF_DEBUG("Executor", "Checking first row size: {}", colCount);
                if (colCount != 1) {
                    std::string errorMsg = std::format("Subquery must return exactly one column, but got {}", colCount);
                    LOGF_ERROR("Executor", "{}", errorMsg);
                    // enableLogging = oldLogging;
                    throw std::runtime_error(errorMsg);
                }
            }
        }
    }

    std::vector<Value> values;
    values.reserve(cells.size());

    for (const auto& row : cells) {
        if (!row.is_array() || row.empty()) continue;

        const auto& cell = row[0];
        if (!cell.contains("content")) continue;

        const auto& content = cell["content"];

        if (content.is_null()) {
            continue;
        } else if (content.is_number_integer()) {
            values.push_back(content.get<int64_t>());
        } else if (content.is_number_float()) {
            values.push_back(content.get<double>());
        } else if (content.is_string()) {
            values.push_back(content.get<std::string>());
        } else if (content.is_boolean()) {
            values.push_back(content.get<bool>());
        }
    }

    LOGF_DEBUG("Executor", "Subquery returned {} values", values.size());
    LOGF_DEBUG("Executor", "=== Subquery execution complete ===");

    // enableLogging = oldLogging;
    return values;
}

Value QueryExecutor::evaluateExpression(const ASTNode* expr, const nlohmann::json& row) {
    if (!expr) {
        return std::monostate{};
    }

    switch (expr->type) {
        case ASTNode::Type::LITERAL_EXPR: {
            auto* lit = static_cast<const LiteralExpr*>(expr);
            return lit->value;
        }

        case ASTNode::Type::IDENTIFIER_EXPR: {
            auto* id = static_cast<const IdentifierExpr*>(expr);

            if (row.contains(id->name)) {
                const auto& val = row[id->name];
                if (val.is_number_integer()) {
                    if (val.is_number_unsigned()) {
                        return static_cast<int64_t>(val.get<uint64_t>());
                    } else {
                        return val.get<int64_t>();
                    }
                }
                if (val.is_number_float()) {
                    return val.get<double>();
                }
                if (val.is_string()) {
                    return val.get<std::string>();
                }
                if (val.is_boolean()) {
                    return val.get<bool>();
                }
                if (val.is_null()) {
                    return std::monostate{};
                }
            }
            return std::monostate{};
        }

        default:
            return std::monostate{};
    }
}

bool compareValues(const Value& left, const Value& right, BinaryExpr::Operator op) {
    if (op == BinaryExpr::Operator::LIKE) {
        if (!std::holds_alternative<std::string>(left) || !std::holds_alternative<std::string>(right)) {
            return false;
        }

        const std::string& text = std::get<std::string>(left);
        const std::string& pattern = std::get<std::string>(right);

        return matchLikePattern(text, pattern);
    }

    bool leftIsNumeric = std::holds_alternative<int64_t>(left) || std::holds_alternative<double>(left);
    bool rightIsNumeric = std::holds_alternative<int64_t>(right) || std::holds_alternative<double>(right);

    if (leftIsNumeric && rightIsNumeric) {
        double leftDouble = std::holds_alternative<int64_t>(left) ?
            static_cast<double>(std::get<int64_t>(left)) : std::get<double>(left);
        double rightDouble = std::holds_alternative<int64_t>(right) ?
            static_cast<double>(std::get<int64_t>(right)) : std::get<double>(right);

        switch (op) {
            case BinaryExpr::Operator::EQ: return leftDouble == rightDouble;
            case BinaryExpr::Operator::NE: return leftDouble != rightDouble;
            case BinaryExpr::Operator::LT: return leftDouble < rightDouble;
            case BinaryExpr::Operator::GT: return leftDouble > rightDouble;
            case BinaryExpr::Operator::LE: return leftDouble <= rightDouble;
            case BinaryExpr::Operator::GE: return leftDouble >= rightDouble;
            default: return false;
        }
    }

    switch (op) {
        case BinaryExpr::Operator::EQ: return left == right;
        case BinaryExpr::Operator::NE: return left != right;
        case BinaryExpr::Operator::LT: return left < right;
        case BinaryExpr::Operator::GT: return left > right;
        case BinaryExpr::Operator::LE: return left <= right;
        case BinaryExpr::Operator::GE: return left >= right;
        default: return false;
    }
}

bool QueryExecutor::evaluatePredicate(const ASTNode* expr, const nlohmann::json& row) {
    if (!expr) {
        return true;
    }

    if (expr->type == ASTNode::Type::BINARY_EXPR) {
        auto* binExpr = static_cast<const BinaryExpr*>(expr);

        if (binExpr->op == BinaryExpr::Operator::LIKE) {
            Value leftVal = evaluateExpression(binExpr->left.get(), row);
            Value rightVal = evaluateExpression(binExpr->right.get(), row);

            if (std::holds_alternative<std::string>(leftVal) &&
                std::holds_alternative<std::string>(rightVal)) {
                const std::string& text = std::get<std::string>(leftVal);
                const std::string& pattern = std::get<std::string>(rightVal);
                return matchLikePattern(text, pattern);
            }
            return false;
        }

        if (binExpr->op == BinaryExpr::Operator::IN_OP) {
            auto leftValue = evaluateExpression(binExpr->left.get(), row);

            if (auto subqueryExpr = dynamic_cast<const SubqueryExpr*>(binExpr->right.get())) {
                std::vector<Value> subqueryValues;
                try {
                    subqueryValues = executeSubquery(subqueryExpr);
                } catch (const std::exception& e) {
                    if (enableLogging) {
                        LOGF_ERROR("Executor", "Subquery execution failed: {}", e.what());
                    }
                    throw;
                }

                for (const auto& value : subqueryValues) {
                    bool match = false;

                    if (std::holds_alternative<std::monostate>(leftValue) ||
                        std::holds_alternative<std::monostate>(value)) {
                        continue;
                    }
                    if (leftValue.index() == value.index()) {
                        match = (leftValue == value);
                    }
                    else if ((std::holds_alternative<int64_t>(leftValue) && std::holds_alternative<double>(value)) ||
                             (std::holds_alternative<double>(leftValue) && std::holds_alternative<int64_t>(value))) {
                        double left_as_double, right_as_double;

                        if (std::holds_alternative<int64_t>(leftValue)) {
                            left_as_double = static_cast<double>(std::get<int64_t>(leftValue));
                        } else {
                            left_as_double = std::get<double>(leftValue);
                        }
                        if (std::holds_alternative<int64_t>(value)) {
                            right_as_double = static_cast<double>(std::get<int64_t>(value));
                        } else {
                            right_as_double = std::get<double>(value);
                        }

                        match = (left_as_double == right_as_double);
                    }

                    if (match) {
                        return true;
                    }
                }
                return false;
            }
        }

        if (binExpr->op == BinaryExpr::Operator::AND) {
            bool left = evaluatePredicate(binExpr->left.get(), row);
            if (!left) return false;
            return evaluatePredicate(binExpr->right.get(), row);
        }

        if (binExpr->op == BinaryExpr::Operator::OR) {
            bool left = evaluatePredicate(binExpr->left.get(), row);
            if (left) return true;
            return evaluatePredicate(binExpr->right.get(), row);
        }

        Value leftVal = evaluateExpression(binExpr->left.get(), row);
        Value rightVal = evaluateExpression(binExpr->right.get(), row);

        return compareValues(leftVal, rightVal, binExpr->op);
    }

    return true;
}

std::function<bool(const nlohmann::json&)> QueryExecutor::createPredicate(const ASTNode* whereClause) {
    if (!whereClause) {
        return [](const nlohmann::json&) { return true; };
    }

    auto it = predicateCache.find(whereClause);
    if (it != predicateCache.end()) {
        return it->second;
    }

    auto predicate = [this, whereClause](const nlohmann::json& row) {
        return evaluatePredicate(whereClause, row);
    };

    predicateCache[whereClause] = predicate;

    cleanupCacheIfNeeded();

    return predicate;
}

void QueryExecutor::cleanupCacheIfNeeded() const {
    if (++cacheCleanupCounter >= CACHE_CLEANUP_INTERVAL) {
        if (predicateCache.size() > 100) {
            predicateCache.clear();
        }
        cacheCleanupCounter = 0;
    }
}

void QueryExecutor::appendValueToString(utils::StringBuilder& builder, const Value& value) {
    std::visit([&builder](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::same_as<T, std::monostate>) {
            builder << "NULL";
        } else if constexpr (std::same_as<T, std::string>) {
            builder << std::format("'{}'", arg);
        } else if constexpr (std::same_as<T, bool>) {
            builder << (arg ? "true" : "false");
        } else {
            builder << std::format("{}", arg);
        }
    }, value);
}

std::string QueryExecutor::valueToString(const Value& value) {
    utils::StringBuilder builder(32);
    appendValueToString(builder, value);
    return std::move(builder).str();
}

// Batch operations для OptimizedQueryExecutor
nlohmann::json OptimizedQueryExecutor::executeBatch(const std::vector<ASTNodePtr>& statements) {
    nlohmann::json results = nlohmann::json::array();

    for (const auto& stmt : statements) {
        auto result = execute(stmt);
        results.push_back(result);

        if (result.contains("error")) {
            break;
        }
    }

    return nlohmann::json{
        {"status", "success"},
        {"batch_results", results},
        {"executed_count", results.size()}
    };
}

}