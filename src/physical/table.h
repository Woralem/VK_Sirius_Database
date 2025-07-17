#pragma once

#include "physical/file_manager.h"
#include "physical/column_manager.h" // Include the manager
#include "common/encoding.h"
#include "types.h" // Include for Value and ColumnDef
#include <string>
#include <cstdint>
#include <vector>
#include <filesystem>
#include <optional>
#include <unordered_map>
#include <memory>
#include <mutex>

using ColumnNameKey = TableNameKey;

// Info stored in the .col file for each column, and used for our in-memory schema.
struct ColumnRecord {
    ColumnNameKey key;
    uint16_t link;
    ColumnDef::DataType type;
    std::streampos file_pos; // Position in the .col file for easy updates
};

class Table {
public:
    explicit Table(std::filesystem::path dir_path, std::string_view link_sv, const Options& options);
    ~Table();

    // --- Class-Level Method for File Deletion ---
    // This is static because it operates on files without needing an instance of the class.
    static void dropTableFiles(const std::filesystem::path& base_path);

    // --- Schema (DDL) Operations ---
    void createColumns(const std::vector<ColumnDef>& columns_to_create);
    void dropColumn(const std::string& column_name);
    void renameColumn(const std::string& old_name, const std::string& new_name);

    // --- Data (DML) Operations ---
    void insertRow(const std::vector<std::pair<std::string, Value>>& named_values);
    void deleteRows(const std::vector<uint64_t>& row_indices);
    void updateValue(uint64_t row_index, const std::string& column_name, const Value& new_value);
    void alterColumnType(const std::string& column_name, const std::string& new_type_string);

    // --- Metadata Getters ---
    uint16_t getCleaningFrequency() const;
    uint8_t getMaxColumnNameLength() const;
    uint8_t getMaxStringLength() const;

    // --- Metadata Setters ---
    void setCleaningFrequency(uint16_t freq);
    void setMaxColumnNameLength(uint8_t len);
    void setMaxStringLength(uint8_t len_code);


    uint64_t getRowCount();
    Value readValue(uint64_t row_index, const std::string& column_name);

private:
    // --- Private Helper Methods ---
    void loadSchema(); // Loads .col file into memory
    void writeTableMetadata(const Options& options);
    uint16_t readTableMetadata() const;
    std::vector<uint16_t> getNewColumnLinks(size_t count);
    ColumnManager* getColumnManager(const ColumnNameKey& key); // Lazy-loading factory
    mutable std::mutex table_schema_mutex_;

    // --- Class Members ---
    std::filesystem::path table_dir_path_;    
    std::string file_link_str_; // The hex string for the table link (e.g., "0A1B")
    std::filesystem::path columns_file_path_; 
    std::filesystem::path meta_file_path_;    
    FileManager file_manager_;
    
    // --- In-Memory State ---
    // A cache of column definitions loaded from the .col file.
    // The key is the encoded column name.
    std::unordered_map<ColumnNameKey, ColumnRecord, KeyHasher> column_schema_;

    // A cache of instantiated ColumnManager objects (lazy-loaded).
    // The key is the encoded column name.
    std::unordered_map<ColumnNameKey, std::unique_ptr<ColumnManager>, KeyHasher> column_managers_;
};