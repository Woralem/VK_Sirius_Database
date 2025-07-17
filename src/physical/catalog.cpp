#include "physical/catalog.h"
#include "storage_engine.h"
#include "common/bit_utils.h"
#include "common/encoding.h"
#include "physical/table.h" // Required to instantiate Table objects
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <algorithm>
#include <vector>
#include <filesystem>
#include <string_view>
#include <format>

//================================================================================
// Constructor & Destructor
//================================================================================

// Initializes the catalog by setting up file paths, creating directories/files if
// they don't exist, opening persistent file streams, and loading existing data from disk.
Catalog::Catalog(const std::string& db_path)
    : db_path_(db_path),
      manager_db_path_(db_path + "/manager.db"),
      meta_db_path_(db_path + "/meta.mt"),
      file_manager_(),
      max_length_name_(65535) // Maximum number of tables (2^16)
{
    file_manager_.createDirectory(db_path_);
    file_manager_.createFile(manager_db_path_);
    file_manager_.createFile(meta_db_path_);

    // Open file streams that will be held for the lifetime of the Catalog.
    // This provides fast, persistent access to the core metadata files.
    manager_file_.open(manager_db_path_, std::ios::in | std::ios::out | std::ios::binary);
    meta_file_.open(meta_db_path_, std::ios::in | std::ios::out | std::ios::binary);

    // Set streams to throw exceptions on failure for robust error handling.
    // manager_file_.exceptions(std::ios::failbit | std::ios::badbit);
    // meta_file_.exceptions(std::ios::failbit | std::ios::badbit);

    if (!manager_file_.is_open()) throw std::runtime_error("FATAL: Could not open manager.db stream.");
    if (!meta_file_.is_open()) throw std::runtime_error("FATAL: Could not open meta.mt stream.");
    
    // Populate the in-memory cache with all existing table records.
    load();
}

// Destructor ensures persistent file streams are properly closed, flushing any
// buffered writes to disk.
Catalog::~Catalog() {
     if (manager_file_.is_open()) {
        manager_file_.flush(); // Явно сбрасываем буферы
        manager_file_.close();
    }
    if (meta_file_.is_open()) {
        meta_file_.flush();
        meta_file_.close();
    }
}

//================================================================================
// Public API Methods
//================================================================================

// Creates a new table, which involves validating the name, generating a unique link (ID),
// persisting the name-to-link mapping, and delegating to the Table class to create
// the physical files and column definitions.
void Catalog::createTable(const std::string& table_name, const std::vector<ColumnDef>& columns, const Options& options) {
    std::lock_guard<std::mutex> lock(catalog_mutex_);
    validateTableName(table_name);
    TableNameKey key = stringToKey(table_name);

    if (table_links_.count(key)) {
        throw std::runtime_error(std::format("Table '{}' already exists.", table_name));
    }

    // This performs the logical creation: acquiring a link and writing to manager.db.
    setLink(key);
    uint16_t link = table_links_.at(key).first;

    // --- Physical File Creation ---
    // The link is used to generate a unique, two-level directory structure.
    // For a link 0xHHLL, the path is db_path/HH/LL.*
    uint8_t high_byte = (link >> 8);
    uint8_t low_byte = link & 0xFF;

    char dir_name_buf[2];
    char file_name_buf[2];
    byte_to_hex_lut(high_byte, dir_name_buf);
    byte_to_hex_lut(low_byte, file_name_buf);

    std::string_view dir_name_sv(dir_name_buf, 2);
    std::string_view file_name_sv(file_name_buf, 2);

    std::filesystem::path dir_path = std::filesystem::path(db_path_) / dir_name_sv;
    
    // The parent directory (e.g., 'HH') is only created for the first file in it.
    if (low_byte == 0) {
        file_manager_.createDirectory(dir_path);
    }

    // --- Integration Point ---
    // Instantiate a Table object, which handles its own file creation (.col, .meta, etc.).
    Table new_table(dir_path, file_name_sv, options);
    
    // Use the new Table object to create all the columns.
    if (!columns.empty()) {
        new_table.createColumns(columns);
    }
}

// Drops a table logically (marking it deleted) and physically (deleting its files).
void Catalog::dropTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(catalog_mutex_);
    validateTableName(table_name);
    TableNameKey key = stringToKey(table_name);
    auto it = table_links_.find(key);
    
    if (it == table_links_.end()) {
        throw std::runtime_error(std::format("Table '{}' does not exist.", table_name));
    }

    try {
        std::streampos pos = it->second.second;
        uint16_t link = it->second.first;

        // 1. Logical delete: Mark record in manager.db with a tombstone (all 0xFF).
        manager_file_.seekp(pos);
        std::vector<char> tombstone(sizeof(TableNameKey) + sizeof(uint16_t), 0xFF);
        manager_file_.write(tombstone.data(), tombstone.size());
        manager_file_.flush();

        // 2. Add the link to the global freelist for recycling.
        meta_file_.seekp(0, std::ios::end);
        meta_file_.write(reinterpret_cast<const char*>(&link), sizeof(link));
        meta_file_.flush();

        // 3. Erase from in-memory map.
        table_links_.erase(it);

        // 4. Physical delete: Delegate file deletion to the Table class.
        uint8_t high_byte = (link >> 8), low_byte = link & 0xFF;
        char dir_name_buf[2], file_name_buf[2];
        byte_to_hex_lut(high_byte, dir_name_buf);
        byte_to_hex_lut(low_byte, file_name_buf);
        std::filesystem::path base_path = std::filesystem::path(db_path_) / std::string_view(dir_name_buf, 2) / std::string_view(file_name_buf, 2);
        
        Table::dropTableFiles(base_path);

        std::cout << "Table '" << table_name << "' dropped successfully." << std::endl;
    } catch (const std::ios_base::failure& e) {
        throw std::runtime_error(std::format("Disk I/O error while dropping table '{}': {}", table_name, e.what()));
    }
}

// IN-PLACE, ROBUST, AND SIMPLE renameTable in catalog.cpp
void Catalog::renameTable(const std::string& old_name, const std::string& new_name) {
    std::lock_guard<std::mutex> lock(catalog_mutex_);
    validateTableName(old_name);
    validateTableName(new_name);

    TableNameKey old_key = stringToKey(old_name);
    TableNameKey new_key = stringToKey(new_name);

    auto it = table_links_.find(old_key);
    if (it == table_links_.end()) {
        throw std::runtime_error(std::format("Table '{}' not found.", old_name));
    }
    if (table_links_.count(new_key)) {
        throw std::runtime_error(std::format("Table with name '{}' already exists.", new_name));
    }

    // --- In-Place Update Logic ---
    // 1. Get the data from the old record.
    uint16_t link = it->second.first;
    std::streampos pos = it->second.second;

    // 2. Overwrite the old key with the new key directly in the file.
    try {
        manager_file_.clear(); // Clear any error flags
        manager_file_.seekp(pos);
        // We only overwrite the key portion (12 bytes), leaving the 2-byte link untouched.
        manager_file_.write(reinterpret_cast<const char*>(&new_key), sizeof(new_key));
        manager_file_.flush();
    } catch (const std::ios_base::failure& e) {
        throw std::runtime_error(std::format("Disk I/O error renaming table '{}': {}", old_name, e.what()));
    }

    // 3. Update the in-memory map.
    table_links_.erase(it);
    table_links_[new_key] = {link, pos}; // The position has NOT changed.

    std::cout << "Table '" << old_name << "' renamed to '" << new_name << "'.\n";
}

// Retrieves the link for a given table name from the fast in-memory map.
bool Catalog::getTableLink(const std::string& table_name, uint16_t& link_out) const {
    std::lock_guard<std::mutex> lock(catalog_mutex_);
    if (table_name.length() > 16) return false;

    TableNameKey key = stringToKey(table_name);
    auto it = table_links_.find(key);

    if (it != table_links_.end()) {
        link_out = it->second.first;
        return true;
    }
    return false;
}

//================================================================================
// Private Helper Methods
//================================================================================

// Factory method to create a Table object for an existing table.
// This is the primary way to get a manipulable Table object from its name.
std::unique_ptr<Table> Catalog::getTable(const std::string& table_name, const Options& options) {
    uint16_t link;
    if (!getTableLink(table_name, link)) {
        return nullptr;
    }
    
    uint8_t high_byte = (link >> 8), low_byte = link & 0xFF;
    char dir_name_buf[2], file_name_buf[2];
    byte_to_hex_lut(high_byte, dir_name_buf);
    byte_to_hex_lut(low_byte, file_name_buf);

    std::string_view dir_name_sv(dir_name_buf, 2);
    std::string_view file_name_sv(file_name_buf, 2);

    std::filesystem::path dir_path = std::filesystem::path(db_path_) / dir_name_sv;

    // The options are only used by the Table constructor if its .meta file is new.
    // For an existing table, this parameter has no effect.
    return std::make_unique<Table>(dir_path, file_name_sv, options);
}

// Populates the in-memory `table_links_` map by reading all records
// from the `manager.db` file upon startup.
void Catalog::load() {
    std::ifstream file(manager_db_path_, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open manager.db for loading.");
    }

    const size_t record_size = sizeof(TableNameKey) + sizeof(uint16_t);
    char buffer[record_size];

    while (file.read(buffer, record_size)) {
        std::streampos record_pos = file.tellg() - static_cast<std::streamoff>(record_size);
        
        TableNameKey key;
        memcpy(&key, buffer, sizeof(TableNameKey));
        
        // --- THIS IS THE KEY FIX ---
        // Check if the record is a tombstone (all bits set to 1).
        if (key.part1 == -1ULL && key.part2 == -1U) {
            continue; // Skip this deleted/invalidated record.
        }

        uint16_t link;
        memcpy(&link, buffer + sizeof(TableNameKey), sizeof(uint16_t));
        table_links_[key] = {link, record_pos};
    }
    
    std::cout << "Info: Loaded " << table_links_.size() << " tables from the catalog." << std::endl;
}

// Acquires a unique 16-bit link for a new table and persists the mapping.
void Catalog::setLink(const TableNameKey& key) {
    if (table_links_.size() >= max_length_name_) {
        throw std::runtime_error("Maximum number of tables for the database has been exceeded.");
    }

    uint16_t link;
    meta_file_.clear();
    meta_file_.seekg(0, std::ios::end);
    std::streampos meta_file_size = meta_file_.tellg();

    // The meta.mt file acts as a LIFO freelist for recycled table links.
    if (meta_file_size == 0) {
        // If the freelist is empty, generate a new link sequentially.
        link = static_cast<uint16_t>(table_links_.size());
    } else {
        // If the freelist is not empty, pop the last available link from it.
        meta_file_.seekg(-2, std::ios::end);
        char buffer[2];
        meta_file_.read(buffer, 2);
        
        // Truncate the file to remove the link we just read. This is a bit complex
        // as it requires closing and reopening the stream.
        meta_file_.close();
        std::filesystem::resize_file(meta_db_path_, static_cast<long long>(meta_file_size) - 2);
        meta_file_.open(meta_db_path_, std::ios::in | std::ios::out | std::ios::binary);

        link = (static_cast<uint8_t>(buffer[0]) << 8) | static_cast<uint8_t>(buffer[1]); // Big-endian read
    }

    manager_file_.clear();
    manager_file_.seekp(0, std::ios::end);
    std::streampos new_record_pos = manager_file_.tellg();
    
    // Append the new record (12-byte key + 2-byte link) to the manager file.
    manager_file_.write(reinterpret_cast<const char*>(&key), sizeof(TableNameKey));
    char write_buffer[2] = {static_cast<char>(link >> 8), static_cast<char>(link & 0xFF)}; // Big-endian write
    manager_file_.write(write_buffer, 2);
    manager_file_.flush();
    
    // Update the in-memory map.
    table_links_[key] = {link, new_record_pos};
}