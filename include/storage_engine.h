#pragma once

#include <string>
#include <vector>
#include <memory>

#include "types.h"
#include "physical/table.h"
#include <utility>

// Forward declaration for a class used in a pointer/reference.
// This tells the compiler "a class named Catalog exists" without needing its full definition.
class Catalog;

class StorageEngine {
public:
    explicit StorageEngine(const std::string& data_path);
    ~StorageEngine();

    // SQL operations
    void createTable(const std::string& name, const std::vector<ColumnDef>& columns, const Options& options = {});
    void insert(const std::string& table_name, const std::vector<std::string>& columns = {}, const std::vector<std::vector<Value>>& values = {});
    void select(const std::string& table_name, const std::vector<std::string>& columns = {"*"}, 
                const std::vector<std::pair<std::string, Value>>& where_clause = {});
    void update(const std::string& table_name, 
                const std::vector<std::pair<std::string, Value>>& set_clause, 
                const std::vector<std::pair<std::string, Value>>& where_clause);
    void deleteRows(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& where_clause);            
    void alterRTable(const std::string& old_table_name, const std::string& new_table_name);
    void alterRColumn(const std::string& table_name, const std::string& old_column_name, const std::string& new_column_name);
    void alterTColumn(const std::string& table_name, const std::string& column_name, const std::string& new_column_type); 
    void dropTable(const std::string& table_name);
    void dropColumn(const std::string& table_name, const std::string& column_name);


    std::string data_path_;
    std::unique_ptr<Table> getTable(const std::string& table_name);
    
    // The compiler now knows what std::unique_ptr and Catalog are.
    std::unique_ptr<Catalog> db_catalog_;
};