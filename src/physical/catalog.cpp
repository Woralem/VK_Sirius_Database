#include "physical/catalog.h"
#include "storage_engine.h"
#include "common/bit_utils.h"
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <algorithm>
#include <vector>
#include <filesystem>
#include <string_view>
#include <format>

// --- Static Encoding Data ---

// Provides a 6-bit encoding for a specific set of 64 characters.
// This allows a 16-character table name (16 * 6 = 96 bits) to be packed
// perfectly into a 12-byte (12 * 8 = 96 bits) key.
static const std::unordered_map<char, uint8_t> ENCODING_MAP = {
    {'A', 0}, {'B', 1}, {'C', 2}, {'D', 3}, {'E', 4}, {'F', 5}, {'G', 6}, {'H', 7},
    {'I', 8}, {'J', 9}, {'K', 10}, {'L', 11}, {'M', 12}, {'N', 13}, {'O', 14}, {'P', 15},
    {'Q', 16}, {'R', 17}, {'S', 18}, {'T', 19}, {'U', 20}, {'V', 21}, {'W', 22}, {'X', 23},
    {'Y', 24}, {'Z', 25}, {'a', 26}, {'b', 27}, {'c', 28}, {'d', 29}, {'e', 30}, {'f', 31},
    {'g', 32}, {'h', 33}, {'i', 34}, {'j', 35}, {'k', 36}, {'l', 37}, {'m', 38}, {'n', 39},
    {'o', 40}, {'p', 41}, {'q', 42}, {'r', 43}, {'s', 44}, {'t', 45}, {'u', 46}, {'v', 47},
    {'w', 48}, {'x', 49}, {'y', 50}, {'z', 51}, {'0', 52}, {'1', 53}, {'2', 54}, {'3', 55},
    {'4', 56}, {'5', 57}, {'6', 58}, {'7', 59}, {'8', 60}, {'9', 61}, {'_', 62}, {'-', 63}
};

// --- Private Helper Functions ---

// Validates a table name against the character set and length constraints.
void validateTableName(const std::string& name) {
    if (name.empty()) {
        throw std::invalid_argument("Table name cannot be empty.");
    }
    if (name.length() > 16) {
        throw std::invalid_argument("Table name cannot exceed 16 characters.");
    }
    if (name.back() == '_') {
        throw std::invalid_argument("Table name cannot end with '_'.");
    }
    if (name.find_first_not_of('-') == std::string::npos) {
        throw std::invalid_argument("Table name cannot consist only of '-'.");
    }
    // Ensures every character in the name can be encoded.
    for (char c : name) {
        if (ENCODING_MAP.find(c) == ENCODING_MAP.end()) {
            throw std::invalid_argument("Table name contains invalid character: " + std::string(1, c));
        }
    }
}


// --- Constructor ---

// Initializes the catalog by setting up file paths, creating directories/files if
// they don't exist, opening file streams, and loading existing data from disk.
Catalog::Catalog(const std::string& db_path)
    : db_path_(db_path),
      manager_db_path_(db_path + "/manager.db"),
      meta_db_path_(db_path + "/meta.mt"),
      file_manager_()
{
    file_manager_.createDirectory(db_path_);
    file_manager_.createFile(manager_db_path_);
    file_manager_.createFile(meta_db_path_);

    // Open file streams for managing table metadata and free links.
    manager_file_.open(manager_db_path_, std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
    meta_file_.open(meta_db_path_, std::ios::in | std::ios::out | std::ios::binary);
    manager_file_.exceptions(std::ios::failbit | std::ios::badbit);
    meta_file_.exceptions(std::ios::failbit | std::ios::badbit);

    if (!manager_file_.is_open()) {
        throw std::runtime_error("FATAL: Could not open manager.db stream.");
    }
    if (!meta_file_.is_open()) {
        throw std::runtime_error("FATAL: Could not open meta.mt stream.");
    }
    load();
}

// Destructor ensures file streams are closed, flushing any buffered writes to disk.
Catalog::~Catalog() {
    if (manager_file_.is_open()) {
        manager_file_.close();
    }
    if (meta_file_.is_open()) {
        meta_file_.close();
    }
}

// --- Public Methods ---

// Creates a new table, which involves validating the name, generating a unique
// link (ID), persisting the name-to-link mapping, and creating the physical
// files for the table's data.
void Catalog::createTable(const std::string& table_name, const std::vector<ColumnDef>& columns) {
    validateTableName(table_name);

    // Encode the human-readable name into a compact 12-byte key for internal use.
    TableNameKey key = stringToKey(table_name);

    if (table_links_.count(key)) {
        throw std::runtime_error(std::format("Table {} already exists.", table_name));
    }

    // Get a unique link and persist the new key-link pair.
    setLink(key);

    uint16_t link = table_links_[key].first;

    // The link is used to generate a unique, two-level directory structure.
    // For a link 0xHHLL, the path is db_path/HH/LL.*
    uint8_t high_byte = (link >> 8); // Corresponds to the directory name.
    uint8_t low_byte = link & 0xFF;  // Corresponds to the base file name.

    char dir_name_buf[2];
    char file_name_buf[2];

    byte_to_hex_lut(high_byte, dir_name_buf);
    byte_to_hex_lut(low_byte, file_name_buf);

    std::string_view dir_name_sv(dir_name_buf, 2);
    std::string_view file_name_sv(file_name_buf, 2);

    std::filesystem::path dir_path = std::filesystem::path(db_path_) / dir_name_sv;

    // The directory is only created for the first file in it (e.g., link 0xHH00).
    if (low_byte == 0){
        file_manager_.createDirectory(dir_path);
    }

    std::filesystem::path file_path = dir_path / file_name_sv;

    // Create the physical files for the table.
    file_path.replace_extension(".col"); // Column data file
    file_manager_.createFile(file_path);

    file_path.replace_extension(".meta"); // Table metadata file
    file_manager_.createFile(file_path);
}

// Acquires a unique 16-bit link for a new table and persists the mapping.
void Catalog::setLink(const TableNameKey& key) {
    uint16_t link;

    meta_file_.clear(); // Clear any error flags on the stream.
    meta_file_.seekg(0, std::ios::end);

    // The meta.mt file acts as a freelist for recycled links.
    if (meta_file_.tellg() == 0) {
        // If the freelist is empty, generate a new link sequentially.
        link = static_cast<uint16_t>(table_links_.size());
    } else {
        // If the freelist is not empty, pop the last available link from it.
        meta_file_.seekg(-2, std::ios::end);
        char buffer[2];
        meta_file_.read(buffer, 2);

        // Truncate the file to remove the link we just read.
        auto pos = meta_file_.tellg();
        meta_file_.close();
        std::filesystem::resize_file(meta_db_path_, static_cast<long long>(pos) - 2);
        meta_file_.open(meta_db_path_, std::ios::in | std::ios::out | std::ios::binary);

        link = (static_cast<uint8_t>(buffer[1]) << 8) |
               static_cast<uint8_t>(buffer[0]);
    }

    // Update the in-memory map.
    // Предполагая, что manager_data_ - это ваш объект std::fstream
    manager_file_.seekg(0, std::ios::end);
    std::streampos last_byte_pos = manager_file_.tellg();
    table_links_[key] = {link, last_byte_pos};

    // Append the new record (12-byte key + 2-byte link) to the manager file.
    manager_file_.write(reinterpret_cast<const char*>(&key), sizeof(TableNameKey));
    char write_buffer[2];
    write_buffer[0] = link & 0xFF;          // Little-endian
    write_buffer[1] = (link >> 8) & 0xFF;
    manager_file_.write(write_buffer, 2);

    // Ensure the changes are written to disk.
    manager_file_.flush();
}

// Retrieves the link for a given table name from the in-memory map.
bool Catalog::getTableLink(const std::string& table_name, uint16_t& link_out) const {
    // A quick check to avoid encoding unnecessarily long strings.
    if (table_name.length() > 16) return false;

    TableNameKey key = stringToKey(table_name);
    auto it = table_links_.find(key);

    if (it != table_links_.end()) {
        link_out = it->second.first;
        return true;
    }
    return false;
}


// --- Private Methods ---

// Populates the in-memory `table_links_` map by reading all records
// from the `manager.db` file upon startup.
void Catalog::load() {
    if (!file_manager_.fileExists(manager_db_path_)) {
        return; // Nothing to load if the file doesn't exist.
    }

    std::fstream file(manager_db_path_, std::ios::in | std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open manager.db for loading.");
    }

    // Each on-disk record consists of a 12-byte key and a 2-byte link.
    const size_t record_size = sizeof(TableNameKey) + sizeof(uint16_t);
    char buffer[record_size];

    while (file.read(buffer, record_size)) {
        // Ensure a full record was read before processing.
        if (file.gcount() == record_size) {
            TableNameKey key;
            uint16_t link;
            memcpy(&key, buffer, sizeof(TableNameKey));
            memcpy(&link, buffer + sizeof(TableNameKey), sizeof(uint16_t));
            table_links_[key] = {link, file.tellg() - record_size};
        }
    }

    std::cout << "Info: Loaded " << table_links_.size() << " tables from the catalog." << std::endl;
}

// Encodes a string into a 12-byte key using a custom 6-bit per character scheme.
TableNameKey Catalog::stringToKey(const std::string& s) const {
    // The buffer where the packed bits will be stored.
    std::array<uint8_t, 12> buffer{};

    // Tracks the current bit position (0-95) in the buffer.
    int current_bit_pos = 0;

    for (char c : s) {
        // Look up the 6-bit code for the character.
        uint8_t code = ENCODING_MAP.at(c);

        // Calculate where to start writing the 6 bits.
        int byte_index = current_bit_pos / 8;
        int bit_in_byte = current_bit_pos % 8;

        // Write the 6-bit code. It might span two bytes.
        // `(code << bit_in_byte)` aligns the code with the current bit position.
        buffer[byte_index] |= (code << bit_in_byte);

        // If the 6 bits crossed a byte boundary (e.g., started at bit 3 or later).
        if (bit_in_byte > 2) {
            // Write the remaining part of the code into the next byte.
            // `(code >> (8 - bit_in_byte))` gets the bits that didn't fit.
            buffer[byte_index + 1] |= (code >> (8 - bit_in_byte));
        }

        // Advance the cursor for the next character.
        current_bit_pos += 6;
    }

    // Reinterpret the raw byte buffer as the final TableNameKey.
    return *reinterpret_cast<TableNameKey*>(buffer.data());
}

void Catalog::dropTable(const std::string& table_name) {
    TableNameKey key = stringToKey(table_name);

    auto it = table_links_.find(key);
    
    if (it == table_links_.end()) {
        throw std::runtime_error(std::format("Table {} does not exist.", table_name));
        return;
    }

    try {
        std::streampos pos = it->second.second;
        std::uint16_t link = it->second.first;

        manager_file_.seekp(pos, std::ios::beg); // Переходим в начало файла

        char write_buffer[sizeof(TableNameKey) + sizeof(uint16_t)];
        std::fill(write_buffer, write_buffer + sizeof(write_buffer), 0xFF);
        manager_file_.write(write_buffer, sizeof(write_buffer));

        // Write the link to the meta file for recycling.
        meta_file_.seekp(0, std::ios::end);
        char link_buffer[sizeof(link)];
        link_buffer[0] = link & 0xFF;          // Little-endian
        link_buffer[1] = (link >> 8) & 0xFF;
        meta_file_.write(link_buffer, sizeof(link));

        table_links_.erase(key);
        // TODO: Remove the physical files associated with the table.
        // This would involve deleting the files in the directory structure
        // based on the link, which is not implemented here.

        std::cout << "Table " << table_name << " dropped successfully." << std::endl;
    } catch (const std::ios_base::failure& e) {
        // Если любая из операций seekp/write не удалась, мы попадаем сюда.
        // Кэш table_links_ остается нетронутым. Состояние консистентно.
        // Оборачиваем ошибку ввода-вывода в более понятное сообщение.
        throw std::runtime_error(std::format("Disk I/O error while dropping table '{}': {}", table_name, e.what()));
    }
}