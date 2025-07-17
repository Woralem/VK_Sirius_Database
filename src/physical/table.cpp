#include "physical/table.h"
#include "storage_engine.h" // For ColumnDef
#include "common/encoding.h"
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <format>
#include <cstring>
#include <unordered_set>
#include <algorithm> // for std::sort

// On-disk record for the .col file.
// Using a packed struct ensures it matches the on-disk layout without compiler padding.
struct ColumnFileRecord {
    ColumnNameKey key;
    uint16_t link;
    ColumnDef::DataType type;
} __attribute__((packed));

constexpr size_t COL_RECORD_SIZE = sizeof(ColumnFileRecord);
constexpr size_t METADATA_HEADER_SIZE = 2;


//================================================================================
// Helper functions for alterColumnType
//================================================================================

// Checks if a data type is variable-length based on its type code.
static bool isVariable(ColumnDef::DataType type) {
    return (static_cast<uint8_t>(type) & 0b10000000) != 0;
}

// Determines if a type conversion is valid according to our rules.
static bool isConversionValid(ColumnDef::DataType old_type, ColumnDef::DataType new_type) {
    bool old_is_var = isVariable(old_type);
    bool new_is_var = isVariable(new_type);

    if (old_is_var && !new_is_var) {
        // Disallow converting from variable-length to fixed-length (e.g., VARCHAR -> INTEGER)
        // as this can lead to data loss and is complex to parse.
        return false;
    }
    // All other conversions are permitted:
    // - fixed -> fixed (e.g., INT -> DOUBLE)
    // - fixed -> var   (e.g., INT -> VARCHAR)
    // - var   -> var   (e.g., VARCHAR -> TEXT)
    return true;
}


//================================================================================
// Constructor & Destructor
//================================================================================
Table::Table(std::filesystem::path dir_path, std::string_view link_sv, const Options& options)
    : table_dir_path_(std::move(dir_path)),
      file_link_str_(link_sv),
      file_manager_()
{
    std::filesystem::path file_base_path = table_dir_path_ / file_link_str_;
    columns_file_path_ = file_base_path;
    columns_file_path_.replace_extension(".col");
    meta_file_path_ = file_base_path;
    meta_file_path_.replace_extension(".meta");

    file_manager_.createDirectory(file_base_path);
    file_manager_.createFile(columns_file_path_);
    file_manager_.createFile(meta_file_path_);

    if (std::filesystem::file_size(meta_file_path_) == 0) {
        writeTableMetadata(options);
    }
    
    loadSchema();
}

Table::~Table() {}


//================================================================================
// Private Helper Methods
//================================================================================

void Table::loadSchema() {
    std::ifstream col_file(columns_file_path_, std::ios::binary);
    if (!col_file.is_open()) {
        throw std::runtime_error("Failed to open .col file for schema loading.");
    }
    
    column_schema_.clear();
    ColumnFileRecord disk_record;
    
    while (col_file.good()) {
        std::streampos record_pos = col_file.tellg();
        col_file.read(reinterpret_cast<char*>(&disk_record), COL_RECORD_SIZE);
        if (col_file.gcount() == COL_RECORD_SIZE) {
             column_schema_[disk_record.key] = {
                disk_record.key,
                disk_record.link,
                disk_record.type,
                record_pos
            };
        }
    }
}

ColumnManager* Table::getColumnManager(const ColumnNameKey& key) {
    auto it = column_managers_.find(key);
    if (it != column_managers_.end()) {
        return it->second.get();
    }

    auto schema_it = column_schema_.find(key);
    if (schema_it == column_schema_.end()) {
        return nullptr;
    }

    const auto& record = schema_it->second;
    std::filesystem::path column_data_dir = table_dir_path_ / file_link_str_;
    
    auto new_manager = std::make_unique<ColumnManager>(
        column_data_dir,
        record.link,
        record.type
    );
    
    ColumnManager* manager_ptr = new_manager.get();
    column_managers_[key] = std::move(new_manager);
    
    return manager_ptr;
}

std::vector<uint16_t> Table::getNewColumnLinks(size_t count) {
    if (count == 0) return {};
    std::vector<uint16_t> links(count);
    std::fstream meta_file(meta_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for link management.");
    }

    meta_file.seekg(0, std::ios::end);
    const std::streampos file_end_pos = meta_file.tellg();
    const size_t available_bytes = (static_cast<size_t>(file_end_pos) > METADATA_HEADER_SIZE) ? (static_cast<size_t>(file_end_pos) - METADATA_HEADER_SIZE) : 0;
    const size_t available_links = available_bytes / sizeof(uint16_t);
    const size_t links_to_read = std::min(count, available_links);
    std::streampos freelist_start_pos = file_end_pos;

    if (links_to_read > 0) {
        const size_t bytes_to_read = links_to_read * sizeof(uint16_t);
        freelist_start_pos = file_end_pos - static_cast<std::streamoff>(bytes_to_read);
        meta_file.seekg(freelist_start_pos);
        meta_file.read(reinterpret_cast<char*>(links.data()), bytes_to_read);
    }
    meta_file.close(); 
    if (links_to_read > 0) {
        std::filesystem::resize_file(meta_file_path_, static_cast<uintmax_t>(freelist_start_pos));
    }
    if (links_to_read < count) {
        uintmax_t col_file_size = std::filesystem::file_size(columns_file_path_);
        uint16_t current_record_count = static_cast<uint16_t>(col_file_size / COL_RECORD_SIZE);

        if (static_cast<uintmax_t>(current_record_count) + available_links + (count - links_to_read) > UINT16_MAX) {
            throw std::runtime_error("Cannot create new columns: maximum number of columns (65535) for this table would be exceeded.");
        }

        uint16_t next_link = current_record_count + static_cast<uint16_t>(available_links);
        
        for (size_t i = links_to_read; i < count; ++i) {
            links[i] = next_link++;
        }
    }
    return links;
}

//================================================================================
// Public DML (Data Manipulation Language) Operations
//================================================================================

void Table::insertRow(const std::vector<std::pair<std::string, Value>>& named_values) {
    std::unordered_map<ColumnNameKey, Value, KeyHasher> insert_map;
    const uint8_t max_len = getMaxColumnNameLength();
    for (const auto& pair : named_values) {
        validateTableName(pair.first, max_len);
        insert_map[stringToKey(pair.first)] = pair.second;
    }

    for (const auto& [key, schema_record] : column_schema_) {
        ColumnManager* manager = getColumnManager(key);
        if (!manager) {
            throw std::logic_error(std::format("Schema key for link {} exists but manager could not be created.", schema_record.link));
        }
        
        auto it = insert_map.find(key);
        if (it != insert_map.end()) {
            manager->appendValue(it->second);
        } else {
            Value null_value;
            null_value.data = std::monostate{};
            manager->appendValue(null_value);
        }
    }
}

void Table::deleteRows(const std::vector<uint64_t>& row_indices) {
    if (row_indices.empty()) return;

    std::vector<uint64_t> sorted_indices = row_indices;
    std::sort(sorted_indices.rbegin(), sorted_indices.rend());

    for (const auto& [key, schema_record] : column_schema_) {
        getColumnManager(key);
    }

    for (uint64_t index : sorted_indices) {
        for (const auto& [key, manager_ptr] : column_managers_) {
            manager_ptr->swapAndPop(index);
        }
    }
}

void Table::updateValue(uint64_t row_index, const std::string& column_name, const Value& new_value) {
    ColumnNameKey key = stringToKey(column_name);
    ColumnManager* manager = getColumnManager(key);
    if (!manager) {
        throw std::runtime_error(std::format("Column '{}' not found in table.", column_name));
    }
    manager->updateValue(row_index, new_value);
}

//================================================================================
// Public DDL (Data Definition Language) Operations
//================================================================================

void Table::alterColumnType(const std::string& column_name, const std::string& new_type_string) {
    // 1. Validation and Preparation
    ColumnNameKey key = stringToKey(column_name);
    auto schema_it = column_schema_.find(key);
    if (schema_it == column_schema_.end()) {
        throw std::runtime_error("Column '" + column_name + "' not found in table.");
    }
    
    const ColumnRecord& old_record = schema_it->second;
    ColumnDef::DataType new_type = stringToDataType(new_type_string);
    
    if (old_record.type == new_type) {
        std::cout << "Column '" << column_name << "' already has type '" << new_type_string << "'." << std::endl;
        return;
    }
    
    if (!isConversionValid(old_record.type, new_type)) {
        throw std::runtime_error("Unsupported type conversion for column '" + column_name + "'.");
    }

    // 2. Data Migration
    std::cout << "Migrating data for column '" << column_name << "'..." << std::endl;
    
    auto old_manager = getColumnManager(key);
    uint64_t row_count = old_manager->getRowCount();
    
    std::filesystem::path column_data_dir = table_dir_path_ / file_link_str_;
    
    // Use a block scope to ensure all temporary I/O objects are destroyed before renaming.
    {
        ColumnManager new_temp_manager(column_data_dir, old_record.link, new_type);
        
        auto temp_dt_path = old_manager->getBlockDataPath();
        temp_dt_path.replace_extension(".tmp");
        BlockDataIO temp_block_io(temp_dt_path);

        for (uint64_t i = 0; i < row_count; ++i) {
            Value old_value = old_manager->readValue(i);
            Value new_value;

            // --- THIS IS THE FIX ---
            std::visit([&](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                
                // Case 1: The new type is variable-length (e.g., VARCHAR).
                if (isVariable(new_type)) {
                    if constexpr (std::is_same_v<T, std::monostate>) {
                        new_value.data = std::string("");
                    } else if constexpr (std::is_same_v<T, bool>) {
                        new_value.data = std::string(arg ? "true" : "false");
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        new_value.data = arg;
                    } else { // It's an arithmetic type
                        new_value.data = std::to_string(arg);
                    }
                } 
                // Case 2: The new type is fixed-length.
                else {
                    if constexpr (std::is_same_v<T, std::monostate>) {
                        new_value.data = std::monostate{};
                    } 
                    // Add a check to ensure we don't try to cast a string to a number.
                    else if constexpr (!std::is_same_v<T, std::string>) {
                        switch (new_type) {
                            case ColumnDef::DataType::Double:
                            case ColumnDef::DataType::Float:
                                new_value.data = static_cast<double>(arg);
                                break;
                            case ColumnDef::DataType::Boolean:
                                new_value.data = static_cast<bool>(arg);
                                break;
                            default: // All integer types
                                new_value.data = static_cast<int64_t>(arg);
                                break;
                        }
                    }
                }
            }, old_value.data);
            
            auto new_block_data = new_temp_manager.serializeForBlock(new_value);
            temp_block_io.append(new_block_data);
        }
    }
    
    // ... (rest of the function remains the same) ...
    auto old_dt_path = old_manager->getBlockDataPath();
    auto temp_dt_path = old_dt_path;
    temp_dt_path.replace_extension(".tmp");
    
    column_managers_.erase(key);

    if (isVariable(old_record.type)) {
        auto old_bg_path = old_dt_path;
        old_bg_path.replace_extension(".bg");
        std::filesystem::remove(old_bg_path);

        auto old_sp_path = old_dt_path;
        old_sp_path.replace_extension(".sp");
        std::filesystem::remove(old_sp_path);
    }
    
    std::filesystem::remove(old_dt_path);
    std::filesystem::rename(temp_dt_path, old_dt_path);
    
    ColumnFileRecord new_file_record = {old_record.key, old_record.link, new_type};
    std::fstream col_file(columns_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!col_file.is_open()) {
        throw std::runtime_error("Failed to open .col file to update metadata.");
    }
    col_file.seekp(old_record.file_pos);
    col_file.write(reinterpret_cast<const char*>(&new_file_record), COL_RECORD_SIZE);
    
    column_schema_[key].type = new_type;
    column_managers_.erase(key);

    std::cout << "Column '" << column_name << "' type changed successfully." << std::endl;
}

uint64_t Table::getRowCount() {
    if (column_schema_.empty()) {
        return 0;
    }
    // All columns have the same number of rows. We can get the count from any of them.
    // To do this in a const method without creating a non-const manager,
    // we must create a temporary one. This is acceptable for a utility function like this.
    const auto& first_col_record = column_schema_.begin()->second;
    std::filesystem::path column_data_dir = table_dir_path_ / file_link_str_;
    ColumnManager temp_manager(column_data_dir, first_col_record.link, first_col_record.type);
    return temp_manager.getRowCount();
}

Value Table::readValue(uint64_t row_index, const std::string& column_name) {
    ColumnNameKey key = stringToKey(column_name);
    ColumnManager* manager = getColumnManager(key);
    if (!manager) {
        throw std::runtime_error(std::format("Column '{}' not found in table.", column_name));
    }
    return manager->readValue(row_index);
}

void Table::dropTableFiles(const std::filesystem::path& base_path) {
    std::error_code ec;
    std::filesystem::remove(base_path.native() + L".col", ec);
    std::filesystem::remove(base_path.native() + L".meta", ec);
    std::filesystem::remove_all(base_path, ec);
}

void Table::createColumns(const std::vector<ColumnDef>& columns_to_create) {
    constexpr size_t MAX_COLUMNS_PER_CALL = 4096; // A reasonable hard limit
    if (columns_to_create.size() > MAX_COLUMNS_PER_CALL) {
        throw std::invalid_argument("Cannot create more than " + std::to_string(MAX_COLUMNS_PER_CALL) + " columns in a single call.");
    }

    if (columns_to_create.empty()) return;
    const uint8_t max_len = getMaxColumnNameLength();
    std::vector<ColumnFileRecord> new_records;
    new_records.reserve(columns_to_create.size());

    std::unordered_set<ColumnNameKey, KeyHasher> name_check;
    for (const auto& col_def : columns_to_create) {
        validateTableName(col_def.name, max_len);
        ColumnNameKey key = stringToKey(col_def.name);
        if (!name_check.insert(key).second) {
            throw std::runtime_error(std::format("Duplicate column name '{}' in create list.", col_def.name));
        }
        if (column_schema_.count(key)) {
            throw std::runtime_error(std::format("Column '{}' already exists in table.", col_def.name));
        }
        new_records.push_back({key, 0, col_def.type});
    }

    std::vector<uint16_t> new_links = getNewColumnLinks(columns_to_create.size());
    for(size_t i = 0; i < new_records.size(); ++i) {
        new_records[i].link = new_links[i];
    }
    
    std::fstream col_file(columns_file_path_, std::ios::app | std::ios::out | std::ios::binary);
    if (!col_file.is_open()) {
        throw std::runtime_error("Failed to open .col file for writing.");
    }

    std::streampos current_pos = col_file.tellp();
    for (const auto& record : new_records) {
        col_file.write(reinterpret_cast<const char*>(&record), COL_RECORD_SIZE);
        column_schema_[record.key] = {record.key, record.link, record.type, current_pos};
        current_pos += COL_RECORD_SIZE;
    }
}

void Table::dropColumn(const std::string& column_name) {
    std::lock_guard<std::mutex> lock(table_schema_mutex_);
    const uint8_t max_len = getMaxColumnNameLength();
    validateTableName(column_name, max_len);
    ColumnNameKey key_to_drop = stringToKey(column_name);

    auto schema_it = column_schema_.find(key_to_drop);
    if (schema_it == column_schema_.end()) {
        throw std::runtime_error(std::format("Column '{}' not found in table.", column_name));
    }
    
    ColumnRecord col_record_to_drop = schema_it->second;

    ColumnManager::dropFiles(table_dir_path_ / file_link_str_, col_record_to_drop.link);
    column_schema_.erase(schema_it);
    column_managers_.erase(key_to_drop);

    std::fstream col_file(columns_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    col_file.seekg(0, std::ios::end);
    std::streampos file_end_pos = col_file.tellg();
    
    bool is_last_record = (col_record_to_drop.file_pos == file_end_pos - static_cast<std::streamoff>(COL_RECORD_SIZE));
    
    if (!is_last_record) {
        std::vector<char> last_record_buffer(COL_RECORD_SIZE);
        col_file.seekg(-static_cast<std::streamoff>(COL_RECORD_SIZE), std::ios::end);
        col_file.read(last_record_buffer.data(), COL_RECORD_SIZE);
        col_file.seekp(col_record_to_drop.file_pos);
        col_file.write(last_record_buffer.data(), COL_RECORD_SIZE);
        
        ColumnFileRecord* moved_disk_rec = reinterpret_cast<ColumnFileRecord*>(last_record_buffer.data());
        auto moved_schema_it = column_schema_.find(moved_disk_rec->key);
        if (moved_schema_it != column_schema_.end()) {
            moved_schema_it->second.file_pos = col_record_to_drop.file_pos;
        }
    }
    
    col_file.close();
    std::filesystem::resize_file(columns_file_path_, static_cast<uintmax_t>(file_end_pos) - COL_RECORD_SIZE);

    std::fstream meta_file(meta_file_path_, std::ios::out | std::ios::app | std::ios::binary);
    meta_file.write(reinterpret_cast<const char*>(&col_record_to_drop.link), sizeof(col_record_to_drop.link));
}

void Table::renameColumn(const std::string& old_name, const std::string& new_name) {
    std::lock_guard<std::mutex> lock(table_schema_mutex_);
    const uint8_t max_len = getMaxColumnNameLength();
    validateTableName(old_name, max_len);
    validateTableName(new_name, max_len);
    
    ColumnNameKey old_key = stringToKey(old_name);
    ColumnNameKey new_key = stringToKey(new_name);

    if (column_schema_.count(new_key)) {
        throw std::runtime_error(std::format("Column '{}' already exists.", new_name));
    }
    
    auto it = column_schema_.find(old_key);
    if (it == column_schema_.end()) {
        throw std::runtime_error(std::format("Column to rename '{}' not found.", old_name));
    }
    
    std::fstream col_file(columns_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    col_file.seekp(it->second.file_pos);
    col_file.write(reinterpret_cast<const char*>(&new_key), sizeof(new_key));
    col_file.close();

    auto node = column_schema_.extract(it);
    node.key() = new_key;
    node.mapped().key = new_key;
    column_schema_.insert(std::move(node));

    auto manager_it = column_managers_.find(old_key);
    if (manager_it != column_managers_.end()) {
        auto manager_node = column_managers_.extract(manager_it);
        manager_node.key() = new_key;
        column_managers_.insert(std::move(manager_node));
    }
}

//================================================================================
// Metadata Getters and Setters
//================================================================================

uint16_t Table::readTableMetadata() const {
    std::ifstream meta_file(meta_file_path_, std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for metadata reading.");
    }
    uint16_t header;
    if (!meta_file.read(reinterpret_cast<char*>(&header), sizeof(header))) {
        meta_file.close();
        throw std::runtime_error("Failed to read metadata header from .meta file.");
    }
    meta_file.close();
    return header;
}

void Table::writeTableMetadata(const Options& options) {
    if (options.maxColumnLemgth == 0 || options.maxColumnLemgth > 31) {
        throw std::invalid_argument("maxColumnLemgth must be between 1 and 31.");
    }
    if (options.gcFrequency > 511) {
        throw std::invalid_argument("gcFrequency cannot be greater than 511.");
    }

    std::fstream meta_file(meta_file_path_, std::ios::out | std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for metadata writing.");
    }
    uint16_t header = 0;
    header |= (static_cast<uint16_t>(options.gcFrequency) & 0x1FF) << 7;
    header |= (static_cast<uint16_t>(options.maxColumnLemgth) & 0x1F) << 2;
    header |= (static_cast<uint16_t>(options.maxStringLength) & 0x03);
    meta_file.write(reinterpret_cast<const char*>(&header), sizeof(header));
}

uint16_t Table::getCleaningFrequency() const {
    return readTableMetadata() >> 7;
}

uint8_t Table::getMaxColumnNameLength() const {
    return (readTableMetadata() >> 2) & 0x1F;
}

uint8_t Table::getMaxStringLength() const {
    return readTableMetadata() & 0x03;
}

void Table::setCleaningFrequency(uint16_t freq) {
    uint16_t current_header = readTableMetadata();
    uint16_t new_header = (current_header & ~((uint16_t)0x1FF << 7)) | ((freq & 0x1FF) << 7);
    std::fstream meta_file(meta_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    meta_file.seekp(0);
    meta_file.write(reinterpret_cast<const char*>(&new_header), sizeof(new_header));
}

void Table::setMaxColumnNameLength(uint8_t len) {
    uint16_t current_header = readTableMetadata();
    uint16_t new_header = (current_header & ~((uint16_t)0x1F << 2)) | ((len & 0x1F) << 2);
    std::fstream meta_file(meta_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    meta_file.seekp(0);
    meta_file.write(reinterpret_cast<const char*>(&new_header), sizeof(new_header));
}

void Table::setMaxStringLength(uint8_t len_code) {
    uint16_t current_header = readTableMetadata();
    uint16_t new_header = (current_header & ~((uint16_t)0x03)) | (len_code & 0x03);
    std::fstream meta_file(meta_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    meta_file.seekp(0);
    meta_file.write(reinterpret_cast<const char*>(&new_header), sizeof(new_header));
}