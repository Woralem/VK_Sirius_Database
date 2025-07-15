// --- REORGANIZED and FINAL table.cpp ---

#include "physical/table.h"
#include "storage_engine.h"
#include "common/encoding.h"
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <format>
#include <cstring>
#include <unordered_set>
#include <algorithm> // for std::min

// --- File-Scope Constants ---
// These constants define the on-disk structure for this class's files.
constexpr size_t RECORD_SIZE = sizeof(ColumnNameKey) + sizeof(uint16_t); // 14 bytes
constexpr size_t BUFFERED_READ_RECORDS = 512;
constexpr size_t READ_BUFFER_SIZE = RECORD_SIZE * BUFFERED_READ_RECORDS; // 7168 bytes
constexpr size_t METADATA_HEADER_SIZE = 2; // 2 bytes for table metadata

//================================================================================
// Constructor & Destructor
//================================================================================

Table::Table(std::filesystem::path dir_path, std::string_view link, const Options& options)
    : table_dir_path_(std::move(dir_path)),
      file_link_(link),
      file_manager_()
{
    // The table's data files are located in a subdirectory named after the table's link.
    std::filesystem::path file_base_path = table_dir_path_ / file_link_;
    columns_file_path_ = file_base_path;
    columns_file_path_.replace_extension(".col");
    meta_file_path_ = file_base_path;
    meta_file_path_.replace_extension(".meta");

    // Ensure the subdirectory and the necessary files exist.
    file_manager_.createDirectory(file_base_path);
    file_manager_.createFile(columns_file_path_);
    file_manager_.createFile(meta_file_path_);

    // If the meta file is new, write the initial 2-byte header with table-specific rules.
    if (std::filesystem::file_size(meta_file_path_) == 0) {
        writeTableMetadata(options);
    }
}

Table::~Table() {
    // No resources like open file handles are held, so the destructor is simple.
}

//================================================================================
// Public API Methods
//================================================================================

void Table::renameColumn(const std::string& old_name, const std::string& new_name) {
    const uint8_t max_len = getMaxColumnNameLength();
    validateTableName(old_name, max_len);
    validateTableName(new_name, max_len);

    ColumnNameKey old_key = stringToKey(old_name);
    ColumnNameKey new_key = stringToKey(new_name);

    // Ensure the new name doesn't already exist
    if (findColumn(new_key)) {
        throw std::runtime_error(std::format("Column '{}' already exists.", new_name));
    }

    // Find the old column to get its position
    auto column_info_opt = findColumn(old_key);
    if (!column_info_opt) {
        throw std::runtime_error(std::format("Column to rename '{}' not found.", old_name));
    }

    // Open the file, seek, and overwrite just the key part of the record
    std::fstream col_file(columns_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!col_file.is_open()) {
        throw std::runtime_error("Failed to open .col file for renaming.");
    }
    
    col_file.seekp(column_info_opt->file_pos);
    col_file.write(reinterpret_cast<const char*>(&new_key), sizeof(new_key));

    col_file.flush();
    col_file.close();
    
    std::cout << "Column '" << old_name << "' renamed to '" << new_name << "'.\n";
}

// This static method handles the physical deletion of all files associated with a table.
void Table::dropTableFiles(const std::filesystem::path& base_path) {
    std::error_code ec; // To capture errors without throwing exceptions

    // 1. Delete the .col file
    std::filesystem::path col_path = base_path;
    col_path.replace_extension(".col");
    std::filesystem::remove(col_path, ec);
    // You might want to log ec if it contains an error

    // 2. Delete the .meta file
    std::filesystem::path meta_path = base_path;
    meta_path.replace_extension(".meta");
    std::filesystem::remove(meta_path, ec);
    
    // 3. Recursively delete the entire column data directory
    // This is the most efficient way to remove the directory and all its contents.
    std::filesystem::remove_all(base_path, ec);
}

void Table::createColumns(const std::vector<ColumnDef>& columns_to_create) {
    if (columns_to_create.empty()) {
        return;
    }

    // --- 1. Initial In-Memory Validation ---
    const uint8_t max_len = getMaxColumnNameLength();
    std::vector<ColumnNameKey> keys_to_create;
    keys_to_create.reserve(columns_to_create.size());
    { 
        std::unordered_set<ColumnNameKey, KeyHasher> name_check;
        for (const auto& col_def : columns_to_create) {
            validateTableName(col_def.name, max_len); // Use table-specific max length
            ColumnNameKey key = stringToKey(col_def.name);
            if (!name_check.insert(key).second) {
                throw std::runtime_error(std::format("Duplicate column name '{}' in create list.", col_def.name));
            }
            keys_to_create.push_back(key);
        }
    }

    // --- 2. Open file ONCE and perform single-pass disk validation ---
    std::fstream col_file(columns_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!col_file.is_open()) {
        throw std::runtime_error("Failed to open .col file.");
    }

    { // Validation block
        std::unordered_set<ColumnNameKey, KeyHasher> search_set(keys_to_create.begin(), keys_to_create.end());
        std::vector<char> buffer(READ_BUFFER_SIZE);
        
        while (col_file.read(buffer.data(), buffer.size()) || col_file.gcount() > 0) {
            if (search_set.empty()) break; 
            size_t bytes_read = col_file.gcount();
            for (size_t offset = 0; offset + RECORD_SIZE <= bytes_read; offset += RECORD_SIZE) {
                ColumnNameKey current_key;
                memcpy(&current_key, buffer.data() + offset, sizeof(ColumnNameKey));
                if (search_set.count(current_key)) {
                    col_file.close();
                    throw std::runtime_error("One or more columns already exist in the table.");
                }
            }
        }
    }

    // --- 3. Batch Link Acquisition ---
    std::vector<uint16_t> new_links = getNewColumnLinks(columns_to_create.size());

    // --- 4. Single-Block Batch Write ---
    std::vector<char> write_buffer(columns_to_create.size() * RECORD_SIZE);
    char* current_buffer_pos = write_buffer.data();
    for (size_t i = 0; i < columns_to_create.size(); ++i) {
        memcpy(current_buffer_pos, &keys_to_create[i], sizeof(ColumnNameKey));
        current_buffer_pos += sizeof(ColumnNameKey);
        memcpy(current_buffer_pos, &new_links[i], sizeof(uint16_t));
        current_buffer_pos += sizeof(uint16_t);
    }
    
    col_file.clear(); // Clear EOF flags from reading before seeking
    col_file.seekp(0, std::ios::end);
    col_file.write(write_buffer.data(), write_buffer.size());
    col_file.close();

    // --- 5. TODO: Create Physical Column Files ---
    for (size_t i = 0; i < columns_to_create.size(); ++i) {
        // TODO: Call a future 'ColumnFileManager::createColumnFiles(this->table_dir_path_, new_links[i])'
        std::cout << "Column '" << columns_to_create[i].name << "' created with link " << new_links[i] << ".\n";
    }
}

void Table::dropColumn(const std::string& column_name) {
    validateTableName(column_name, getMaxColumnNameLength());
    ColumnNameKey key_to_drop = stringToKey(column_name);

    auto column_info_opt = findColumn(key_to_drop);
    if (!column_info_opt) {
        throw std::runtime_error(std::format("Column '{}' not found in the table.", column_name));
    }
    ColumnInfo col_info = *column_info_opt;

    std::fstream col_file(columns_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!col_file.is_open()) {
        throw std::runtime_error("Failed to open .col file for writing.");
    }

    // 1. Получить размер файла, чтобы найти последнюю запись.
    col_file.seekg(0, std::ios::end);
    std::streampos file_end_pos = col_file.tellg();
    
    // Если в файле всего одна запись (или он пуст), то col_info.file_pos будет равен 0,
    // а file_end_pos - RECORD_SIZE.
    bool is_last_record = (col_info.file_pos == file_end_pos - static_cast<std::streamoff>(RECORD_SIZE));

    if (!is_last_record) {
        // 2. Если удаляемый элемент - НЕ последний, читаем последний элемент.
        std::vector<char> last_record_buffer(RECORD_SIZE);
        col_file.seekg(-static_cast<std::streamoff>(RECORD_SIZE), std::ios::end);
        col_file.read(last_record_buffer.data(), RECORD_SIZE);
        
        // 3. Перезаписываем удаляемый элемент последним элементом ("swap").
        col_file.seekp(col_info.file_pos);
        col_file.write(last_record_buffer.data(), RECORD_SIZE);
    }
    
    // 4. Закрываем файл перед его усечением. Это ОБЯЗАТЕЛЬНО.
    col_file.close();

    // 5. Усекаем файл, удаляя последнюю запись ("pop").
    // Это работает как для случая, когда мы скопировали последний элемент,
    // так и для случая, когда удаляемый элемент и был последним.
    std::filesystem::resize_file(columns_file_path_, static_cast<uintmax_t>(file_end_pos) - RECORD_SIZE);

    // 6. Добавляем link удаленного элемента в freelist.
    // Логика freelist остается полезной для переиспользования ID.
    std::fstream meta_file(meta_file_path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for writing.");
    }
    meta_file.write(reinterpret_cast<const char*>(&col_info.link), sizeof(col_info.link));
    meta_file.close();

    std::cout << "Column '" << column_name << "' dropped.\n";
}

//================================================================================
// Public Metadata Getters
//================================================================================

uint16_t Table::getCleaningFrequency() const {
    uint16_t header = readTableMetadata();
    return header >> 7; // Extract the top 9 bits
}

uint8_t Table::getMaxColumnNameLength() const {
    uint16_t header = readTableMetadata();
    return (header >> 2) & 0x1F; // Extract the middle 5 bits
}

uint8_t Table::getMaxStringLength() const {
    uint16_t header = readTableMetadata();
    return header & 0x03; // Extract the last 2 bits
}

//================================================================================
// Private Helper Methods
//================================================================================

std::optional<ColumnInfo> Table::findColumn(const ColumnNameKey& key) {
    std::ifstream col_file(columns_file_path_, std::ios::binary);
    if (!col_file.is_open()) {
        throw std::runtime_error("Failed to open .col file for reading.");
    }

    std::vector<char> buffer(READ_BUFFER_SIZE);
    std::streampos current_pos = 0;

    while (col_file.read(buffer.data(), buffer.size()) || col_file.gcount() > 0) {
        size_t bytes_read = col_file.gcount();
        for (size_t offset = 0; offset + RECORD_SIZE <= bytes_read; offset += RECORD_SIZE) {
            ColumnNameKey current_key;
            memcpy(&current_key, buffer.data() + offset, sizeof(ColumnNameKey));
            
            if (current_key.part1 == -1ULL && current_key.part2 == -1U) {
                continue; // Skip tombstone records
            }

            if (current_key == key) {
                uint16_t link;
                memcpy(&link, buffer.data() + offset + sizeof(ColumnNameKey), sizeof(uint16_t));
                return ColumnInfo{link, current_pos + static_cast<std::streamoff>(offset)};
            }
        }
        current_pos += bytes_read;
    }
    return std::nullopt;
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
    const size_t available_bytes = static_cast<size_t>(file_end_pos) > METADATA_HEADER_SIZE
                                       ? static_cast<size_t>(file_end_pos) - METADATA_HEADER_SIZE : 0;
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
        uint16_t current_record_count = static_cast<uint16_t>(col_file_size / RECORD_SIZE);
        uint16_t next_link = current_record_count + static_cast<uint16_t>(available_links);
        
        for (size_t i = links_to_read; i < count; ++i) {
            links[i] = next_link++;
        }
    }

    return links;
}

void Table::writeTableMetadata(const Options& options) {
    std::fstream meta_file(meta_file_path_, std::ios::out | std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for metadata writing.");
    }

    // Pack data into 16 bits: [9: cleaning freq] [5: col name len] [2: str len]
    uint16_t header = 0;
    header |= (static_cast<uint16_t>(options.gcFrequency) & 0x1FF) << 7;
    header |= (static_cast<uint16_t>(options.maxColumnLemgth) & 0x1F) << 2;
    header |= (static_cast<uint16_t>(options.maxStringLength) & 0x03);

    meta_file.write(reinterpret_cast<const char*>(&header), sizeof(header));
}

uint16_t Table::readTableMetadata() const {
    std::ifstream meta_file(meta_file_path_, std::ios::binary);
     if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for metadata reading.");
    }
    uint16_t header;
    if (!meta_file.read(reinterpret_cast<char*>(&header), sizeof(header))) {
        // Close the file even if the read fails.
        meta_file.close();
        throw std::runtime_error("Failed to read metadata header from .meta file.");
    }

    // Explicitly close the file stream after use.
    meta_file.close();
    return header;
}

//================================================================================
// Public Metadata Setters
//================================================================================

void Table::setCleaningFrequency(uint16_t freq) {
    // 1. Read the current 16-bit header.
    uint16_t current_header = readTableMetadata();

    // 2. Modify the header using bitmasks.
    // First, clear the 9 bits for the frequency using a NOT mask.
    uint16_t new_header = current_header & ~((uint16_t)0x1FF << 7);
    // Then, set the new value in the correct position.
    new_header |= (freq & 0x1FF) << 7;

    // 3. Write the modified header back to the file.
    std::fstream meta_file(meta_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for metadata writing.");
    }
    meta_file.seekp(0); // Go to the beginning of the file
    meta_file.write(reinterpret_cast<const char*>(&new_header), sizeof(new_header));
    meta_file.close();
}

void Table::setMaxColumnNameLength(uint8_t len) {
    uint16_t current_header = readTableMetadata();

    // Modify the header: Clear the 5 bits for the length, then set the new value.
    uint16_t new_header = current_header & ~((uint16_t)0x1F << 2);
    new_header |= (len & 0x1F) << 2;

    std::fstream meta_file(meta_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for metadata writing.");
    }
    meta_file.seekp(0);
    meta_file.write(reinterpret_cast<const char*>(&new_header), sizeof(new_header));
    meta_file.close();
}

void Table::setMaxStringLength(uint8_t len_code) {
    uint16_t current_header = readTableMetadata();

    // Modify the header: Clear the 2 bits for the string length, then set the new value.
    uint16_t new_header = current_header & ~((uint16_t)0x03);
    new_header |= (len_code & 0x03);
    
    std::fstream meta_file(meta_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!meta_file.is_open()) {
        throw std::runtime_error("Failed to open .meta file for metadata writing.");
    }
    meta_file.seekp(0);
    meta_file.write(reinterpret_cast<const char*>(&new_header), sizeof(new_header));
    meta_file.close();
}