#pragma once

#include <string>
#include <cstdint>
#include <vector>
#include <filesystem>
#include <optional>
#include "types.h"
#include "physical/file_manager.h"
#include "common/encoding.h"

struct ColumnDef;
using ColumnNameKey = TableNameKey;

struct ColumnInfo {
    uint16_t link;
    std::streampos file_pos;
};

class Table {
public:
    explicit Table(std::filesystem::path dir_path, std::string_view link, const Options& options);
    ~Table();

    // --- Public Class-Level Method for File Deletion ---
    // This is static because it operates on files without needing an instance of the class.
    static void dropTableFiles(const std::filesystem::path& base_path);

    // --- Public Instance Methods ---
    void createColumns(const std::vector<ColumnDef>& columns_to_create);
    void dropColumn(const std::string& column_name);

    // Getters
    uint16_t getCleaningFrequency() const;
    uint8_t getMaxColumnNameLength() const;
    uint8_t getMaxStringLength() const;

    // Setters
    void setCleaningFrequency(uint16_t freq);
    void setMaxColumnNameLength(uint8_t len);
    void setMaxStringLength(uint8_t len_code);
    void renameColumn(const std::string& old_name, const std::string& new_name);

private:
    // --- Private Helper Methods ---
    std::optional<ColumnInfo> findColumn(const ColumnNameKey& key);
    std::vector<uint16_t> getNewColumnLinks(size_t count);
    void writeTableMetadata(const Options& options);
    uint16_t readTableMetadata() const;

    // --- Class Members ---
    std::filesystem::path table_dir_path_;    
    std::string_view file_link_;              
    std::filesystem::path columns_file_path_; 
    std::filesystem::path meta_file_path_;    
    FileManager file_manager_;
};