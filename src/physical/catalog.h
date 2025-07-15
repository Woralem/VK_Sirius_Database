#pragma once

#include "physical/file_manager.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <fstream>
#include "physical/table.h"

// Forward declaration to avoid including the full encoding.h header.
// This tells the compiler that these types exist, improving compile times.
struct TableNameKey;
struct KeyHasher;

// Forward declaration
struct ColumnDef;

class Catalog {
public:
    explicit Catalog(const std::string& db_path);
    ~Catalog();

    void createTable(const std::string& table_name, const std::vector<ColumnDef>& columns, const Options& options);
    void dropTable(const std::string& table_name);
    bool getTableLink(const std::string& table_name, uint16_t& link_out) const;
    void renameTable(const std::string& old_name, const std::string& new_name);
    std::unique_ptr<Table> getTable(const std::string& table_name, const Options& options = {});

private:
    void load();
    void setLink(const TableNameKey& key);
    // stringToKey is no longer a member of Catalog, it's a free function.

    std::string db_path_;
    std::string manager_db_path_;
    std::string meta_db_path_;
    FileManager file_manager_;
    uint16_t max_length_name_; // Note: uint16_t is a better type for this than int

    std::fstream manager_file_;
    std::fstream meta_file_;

    // The in-memory cache of the manager.db file.
    // The full definitions of TableNameKey and KeyHasher are needed in catalog.cpp,
    // but not here, thanks to the forward declarations.
    std::unordered_map<TableNameKey, std::pair<uint16_t, std::streampos>, KeyHasher> table_links_;
};