#include "storage_engine.h"
#include "physical/catalog.h"
#include "physical/table.h"
#include <iostream>
#include <memory>
#include <stdexcept>
#include <format>

//================================================================================
// Internal Helper for WHERE clause simulation
//================================================================================
/**
 * @brief Finds the indices of rows that satisfy all conditions in the where_clause.
 * @note This is an internal helper function that simulates a basic Query Executor's
 * "table scan" phase. It performs a full, un-indexed scan of the table.
 * It currently only supports basic equality checks with AND logic.
 *
 * @param table A pointer to the table object to scan.
 * @param where_clause A vector of column-name/value pairs representing the conditions.
 * @return A vector of 64-bit row indices that match the criteria.
 */
static std::vector<uint64_t> findRowIndices(Table* table, const std::vector<std::pair<std::string, Value>>& where_clause) {
    std::vector<uint64_t> result_indices;
    if (!table) return result_indices;

    uint64_t row_count = table->getRowCount();
    if (row_count == 0) return result_indices;

    // For safety, if where_clause is empty, no rows are selected for update/delete.
    if (where_clause.empty()) {
        return result_indices;
    }

    // Perform a full table scan.
    for (uint64_t i = 0; i < row_count; ++i) {
        bool match = true;
        // Check if the current row satisfies ALL conditions (AND logic).
        for (const auto& condition : where_clause) {
            const std::string& col_name = condition.first;
            const Value& required_value = condition.second;
            
            Value current_value = table->readValue(i, col_name);
            
            // This is a simple, direct comparison. A real system would have a
            // sophisticated, type-aware comparison engine.
            if (current_value.data != required_value.data) {
                match = false;
                break; // If one condition fails, we can skip to the next row.
            }
        }
        
        if (match) {
            result_indices.push_back(i);
        }
    }
    
    return result_indices;
}


//================================================================================
// Constructor & Destructor
//================================================================================

StorageEngine::StorageEngine(const std::string& data_path) : data_path_(data_path) {
    try {
        db_catalog_ = std::make_unique<Catalog>(data_path_);
    } catch (const std::exception& e) {
        std::cerr << "Fatal error during StorageEngine initialization: " << e.what() << std::endl;
        throw;
    }
}

StorageEngine::~StorageEngine() = default;


//================================================================================
// DDL - Data Definition Language API
//================================================================================

void StorageEngine::createTable(const std::string& name, const std::vector<ColumnDef>& columns, const Options& options) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    db_catalog_->createTable(name, columns, options);
}

void StorageEngine::dropTable(const std::string& table_name) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    db_catalog_->dropTable(table_name);
}

void StorageEngine::alterRTable(const std::string& old_table_name, const std::string& new_table_name) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    db_catalog_->renameTable(old_table_name, new_table_name);
}

void StorageEngine::alterRColumn(const std::string& table_name, const std::string& old_column_name, const std::string& new_column_name) {
    auto table = getTable(table_name);
    if (!table) {
        throw std::runtime_error(std::format("Table '{}' not found.", table_name));
    }
    table->renameColumn(old_column_name, new_column_name);
}

void StorageEngine::alterTColumn(const std::string& table_name, const std::string& column_name, const std::string& new_column_type) {
    // This function is a placeholder for a future, complex implementation.
    throw std::logic_error("Function not implemented: alterTColumn");
}

void StorageEngine::dropColumn(const std::string& table_name, const std::string& column_name) {
    auto table = getTable(table_name);
    if (!table) {
        throw std::runtime_error(std::format("Table '{}' not found.", table_name));
    }
    table->dropColumn(column_name);
}

//================================================================================
// DML - Data Manipulation Language API
//================================================================================

void StorageEngine::insert(const std::string& table_name, const std::vector<std::string>& columns, const std::vector<std::vector<Value>>& values) {
    auto table = getTable(table_name);
    if (!table) {
        throw std::runtime_error(std::format("Table '{}' not found.", table_name));
    }
    
    if (columns.empty()) {
        throw std::invalid_argument("Explicit column names are required for insert.");
    }
    
    for (const auto& row_values : values) {
        if (columns.size() != row_values.size()) {
            throw std::invalid_argument("Number of columns does not match number of values.");
        }
        std::vector<std::pair<std::string, Value>> named_values;
        named_values.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            named_values.emplace_back(columns[i], row_values[i]);
        }
        table->insertRow(named_values);
    }
}

void StorageEngine::update(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& set_clause, const std::vector<std::pair<std::string, Value>>& where_clause) {
    auto table = getTable(table_name);
    if (!table) {
        throw std::runtime_error(std::format("Table '{}' not found.", table_name));
    }
    
    // Step 1: Find all row indices that satisfy the `where_clause`.
    std::vector<uint64_t> indices_to_update = findRowIndices(table.get(), where_clause);
    
    if (indices_to_update.empty()) {
        return; // Nothing to do if no rows match.
    }

    // Step 2: For each found index, apply all changes from the `set_clause`.
    for (uint64_t index : indices_to_update) {
        for (const auto& set_pair : set_clause) {
            table->updateValue(index, set_pair.first, set_pair.second);
        }
    }
}

void StorageEngine::deleteRows(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& where_clause) {
    auto table = getTable(table_name);
    if (!table) {
        throw std::runtime_error(std::format("Table '{}' not found.", table_name));
    }

    // Step 1: Find all row indices that satisfy the `where_clause`.
    std::vector<uint64_t> indices_to_delete = findRowIndices(table.get(), where_clause);

    if (indices_to_delete.empty()) {
        return;
    }

    // Step 2: Pass the found indices to the Table for physical deletion.
    table->deleteRows(indices_to_delete);
}

//================================================================================
// Accessor Methods
//================================================================================

std::unique_ptr<Table> StorageEngine::getTable(const std::string& table_name) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    // This is the primary accessor for the Query Layer to get direct access
    // to a table's data manipulation primitives (e.g., for SELECT).
    return db_catalog_->getTable(table_name);
}