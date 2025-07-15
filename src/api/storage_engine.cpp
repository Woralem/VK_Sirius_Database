#include "storage_engine.h"
#include "physical/catalog.h"
#include "physical/table.h" // Required for unique_ptr<Table>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <format>

// Constructor and Destructor remain the same...
StorageEngine::StorageEngine(const std::string& data_path) : data_path_(data_path) {
    try {
        db_catalog_ = std::make_unique<Catalog>(data_path_);
        std::cout << "StorageEngine initialized for path: " << data_path_ << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Fatal error during StorageEngine initialization: " << e.what() << std::endl;
        throw;
    }
}

StorageEngine::~StorageEngine() = default;


// --- SQL Operation Implementations ---

void StorageEngine::createTable(const std::string& name, const std::vector<ColumnDef>& columns, const Options& options) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    // Direct delegation to catalog
    db_catalog_->createTable(name, columns, options);
}

void StorageEngine::dropTable(const std::string& table_name) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    // Direct delegation to catalog
    db_catalog_->dropTable(table_name);
}

void StorageEngine::dropColumn(const std::string& table_name, const std::string& column_name) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    
    // Two-step delegation: get table object, then call its method
    auto table = db_catalog_->getTable(table_name);
    if (!table) {
        throw std::runtime_error(std::format("Table '{}' not found.", table_name));
    }
    table->dropColumn(column_name);
}

void StorageEngine::alterRTable(const std::string& old_table_name, const std::string& new_table_name) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");
    // Direct delegation to the new catalog method
    db_catalog_->renameTable(old_table_name, new_table_name);
}

void StorageEngine::alterRColumn(const std::string& table_name, const std::string& old_column_name, const std::string& new_column_name) {
    if (!db_catalog_) throw std::runtime_error("Catalog is not initialized.");

    // Two-step delegation: get table object, then call its new method
    auto table = db_catalog_->getTable(table_name);
    if (!table) {
        throw std::runtime_error(std::format("Table '{}' not found.", table_name));
    }
    table->renameColumn(old_column_name, new_column_name);
}


// --- Stubs for methods not yet implemented ---

void StorageEngine::insert(const std::string& table_name, const std::vector<std::string>& columns, const std::vector<std::vector<Value>>& values) {
    throw std::logic_error("Function not implemented: insert");
}

void StorageEngine::select(const std::string& table_name, const std::vector<std::string>& columns, const std::vector<std::pair<std::string, Value>>& where_clause) {
    throw std::logic_error("Function not implemented: select");
}

void StorageEngine::update(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& set_clause, const std::vector<std::pair<std::string, Value>>& where_clause) {
    throw std::logic_error("Function not implemented: update");
}

void StorageEngine::deleteRows(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& where_clause) {
    throw std::logic_error("Function not implemented: deleteRows");
}

void StorageEngine::alterTColumn(const std::string& table_name, const std::string& column_name, const std::string& new_column_type) {
    throw std::logic_error("Function not implemented: alterTColumn");
}

std::unique_ptr<Table> StorageEngine::getTable(const std::string& table_name) {
    if (!db_catalog_) return nullptr;
    return db_catalog_->getTable(table_name);
}