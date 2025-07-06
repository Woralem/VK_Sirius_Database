#include "storage_engine.h"
// The full definition of Catalog is required here for std::unique_ptr's destructor.
#include "physical/catalog.h"
#include <iostream>
#include <memory>
#include <stdexcept>

// Initializes the storage engine by creating the main database catalog.
StorageEngine::StorageEngine(const std::string& data_path)
    : data_path_(data_path) {
    try {
        // Safely create the Catalog instance using std::make_unique.
        // The Catalog handles all physical file and metadata management.
        db_catalog_ = std::make_unique<Catalog>(data_path_);
        std::cout << "StorageEngine initialized for path: " << data_path_ << std::endl;
    } catch (const std::exception& e) {
        // A failure to create or load the catalog is a fatal error.
        std::cerr << "Fatal error during StorageEngine initialization: " << e.what() << std::endl;
        throw; // Re-throw the exception to halt the application.
    }
}

// The destructor must be defined in the .cpp file where the full definition of `Catalog`
// is known. This is required for `std::unique_ptr` to correctly call the destructor
// of the forward-declared type `Catalog`. Leaving it in the header would cause a
// compilation error.
StorageEngine::~StorageEngine() = default;

// --- SQL Operation Implementations ---

void StorageEngine::createTable(const std::string& name, const std::vector<ColumnDef>& columns, const Options& options) {
    if (!db_catalog_) {
        throw std::runtime_error("Catalog is not initialized.");
    }
    // Delegate the create table operation to the catalog module.
    db_catalog_->createTable(name, columns);
}

// Stubs for other API methods to prevent linker errors.
// These will be implemented in the future.
void StorageEngine::insert(const std::string& table_name, const std::vector<std::string>& columns, const std::vector<std::vector<Value>>& values) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: insert");
}

void StorageEngine::select(const std::string& table_name, const std::vector<std::string>& columns, const std::vector<std::pair<std::string, Value>>& where_clause) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: select");
}

void StorageEngine::update(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& set_clause, const std::vector<std::pair<std::string, Value>>& where_clause) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: update");
}

void StorageEngine::deleteRows(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& where_clause) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: deleteRows");
}

void StorageEngine::alterRTable(const std::string& old_table_name, const std::string& new_table_name) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: alterRTable");
}

void StorageEngine::alterRColumn(const std::string& table_name, const std::string& old_column_name, const std::string& new_column_name) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: alterRColumn");
}

void StorageEngine::alterTColumn(const std::string& table_name, const std::string& column_name, const std::string& new_column_type) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: alterTColumn");
}

void StorageEngine::dropTable(const std::string& table_name) {
    // TODO: Implement later
    // db_catalog_->dropTable(table_name);
    throw std::logic_error("Function not implemented: dropTable");
}

void StorageEngine::dropColumn(const std::string& table_name, const std::string& column_name) {
    // TODO: Implement later
    throw std::logic_error("Function not implemented: dropColumn");
}