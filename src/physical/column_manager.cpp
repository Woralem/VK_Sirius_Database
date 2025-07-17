#include "physical/column_manager.h"
#include "common/bit_utils.h"
#include "physical/file_manager.h"
#include <stdexcept>
#include <cstring>
#include <variant>
#include <iostream>
#include <limits>

// Helper function to convert a uint16_t link to a 4-char hex string (e.g., 258 -> "0102").
// This is used to create unique file names for each column.
std::string linkToHex(uint16_t link) {
    char hex_buf[4];
    byte_to_hex_lut(static_cast<uint8_t>(link >> 8), &hex_buf[0]); // High byte
    byte_to_hex_lut(static_cast<uint8_t>(link & 0xFF), &hex_buf[2]); // Low byte
    return std::string(hex_buf, 4);
}

// --- Constructor & Destructor ---

ColumnManager::ColumnManager(const std::filesystem::path& table_data_dir, uint16_t column_link, ColumnDef::DataType type)
    : data_type_(type),
      freelist_is_dirty_(false)
{
    // The base path for all files related to this column (e.g., "db/0A/01/0001").
    std::filesystem::path file_base_path = table_data_dir / linkToHex(column_link);

    // Ensure the parent directory (e.g., "db/0A/01") exists.
    FileManager fm;
    fm.createDirectory(file_base_path.parent_path());

    // Initialize the main data file I/O handler (.dt file).
    std::filesystem::path dt_path = file_base_path;
    dt_path.replace_extension(".dt");
    block_io_ = std::make_unique<BlockDataIO>(dt_path);

    // If the data type is variable-length, we also need heap storage.
    if (isVariableLength()) {
        // Initialize the heap file I/O handler (.bg file).
        std::filesystem::path bg_path = file_base_path;
        bg_path.replace_extension(".bg");
        heap_io_ = std::make_unique<HeapIO>(bg_path);

        // Store the path to the space manager file (.sp file) for later use.
        sp_path_ = file_base_path;
        sp_path_.replace_extension(".sp");
    }
}

ColumnManager::~ColumnManager() {
    // On destruction, ensure any changes to the freelist are saved to disk.
    persistFreelistIfDirty();
}

// --- Serialization and Deserialization ---

// Converts a Value object into its raw 8-byte representation for the .dt file.
std::array<char, 8> ColumnManager::serializeForBlock(const Value& value) {
    std::array<char, 8> block_data{};

    if (isVariableLength()) {
        // For variable-length types, the .dt file stores an 8-byte offset into the .bg heap file.
        // We use the maximum uint64_t value as a sentinel to represent a NULL or empty string.
        // This avoids ambiguity with offset 0, which is a valid position.
        uint64_t offset = std::numeric_limits<uint64_t>::max(); 

        if (const auto* str_ptr = std::get_if<std::string>(&value.data)) {
            const std::string& str = *str_ptr;
            if (!str.empty()) {
                // The on-heap payload is a 2-byte length prefix followed by the string data.
                uint16_t len = str.length();
                uint16_t payload_size = sizeof(len) + len;
                std::vector<char> payload(payload_size);
                memcpy(payload.data(), &len, sizeof(len));
                memcpy(payload.data() + sizeof(len), str.data(), len);

                // Before writing, try to reclaim space from the freelist.
                ensureFreelistIsLoaded();
                auto claimed_chunk = SpaceManager::claim(freelist_cache_.value(), payload_size);
                
                if (claimed_chunk.has_value()) {
                    // A suitable free chunk was found. Write the data there.
                    offset = claimed_chunk->offset;
                    heap_io_->writeAt(offset, payload);
                    freelist_is_dirty_ = true;
                    // If the claimed chunk was larger than needed, return the remainder to the freelist.
                    if (claimed_chunk->length > payload_size) {
                        SpaceManager::add(freelist_cache_.value(), offset + payload_size, claimed_chunk->length - payload_size);
                    }
                } else {
                    // No suitable space was found; append the data to the end of the heap.
                    offset = heap_io_->append(payload);
                }
            }
        }
        // Write the final offset (or the NULL sentinel) into the 8-byte block data.
        memcpy(block_data.data(), &offset, sizeof(offset));
    } else {
        // For fixed-size types, the value itself is stored in the .dt file.
        std::visit([&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                // NULL is represented by all bits set to 1 (0xFF).
                memset(block_data.data(), 0xFF, sizeof(block_data));
            } else if constexpr (std::is_arithmetic_v<T>) {
                // Copy the raw bytes of the arithmetic value.
                memcpy(block_data.data(), &arg, sizeof(T));
            }
        }, value.data);
    }
    return block_data;
}

// Converts an 8-byte raw block from the .dt file back into a Value object.
Value ColumnManager::deserializeFromBlock(const std::array<char, 8>& block_data) {
    Value result;

    if (isVariableLength()) {
        // Read the 8-byte offset from the block data.
        uint64_t offset = *reinterpret_cast<const uint64_t*>(block_data.data());
        
        // Check for the NULL sentinel value.
        if (offset == std::numeric_limits<uint64_t>::max()) {
            result.data = std::string(""); // Represent NULL as an empty string.
        } else {
            // The offset is valid; read the data from the heap file.
            // First, read the 2-byte length prefix.
            std::vector<char> len_bytes = heap_io_->read(offset, sizeof(uint16_t));
            uint16_t len = *reinterpret_cast<const uint16_t*>(len_bytes.data());

            if (len > 0) {
                // Then, read the actual string data.
                std::vector<char> str_bytes = heap_io_->read(offset + sizeof(uint16_t), len);
                result.data = std::string(str_bytes.data(), len);
            } else {
                result.data = std::string("");
            }
        }
    } else {
        // For fixed-size types, check for the NULL marker (all 0xFF bytes).
        bool is_null = true;
        for(char c : block_data) {
            if (static_cast<unsigned char>(c) != 0xFF) { is_null = false; break; }
        }
        
        if (is_null) {
            result.data = std::monostate{}; // This is a NULL value.
        } else {
            // Reinterpret the raw bytes based on the column's data type.
            switch (data_type_) {
                case ColumnDef::DataType::Integer: case ColumnDef::DataType::BigInt: case ColumnDef::DataType::SmallInt: case ColumnDef::DataType::TinyInt:
                    result.data = *reinterpret_cast<const int64_t*>(block_data.data());
                    break;
                case ColumnDef::DataType::Double: case ColumnDef::DataType::Float:
                    result.data = *reinterpret_cast<const double*>(block_data.data());
                    break;
                case ColumnDef::DataType::Boolean:
                    result.data = *reinterpret_cast<const bool*>(block_data.data());
                    break;
                default:
                    // Should not happen for supported fixed-size types.
                    result.data = std::monostate{};
            }
        }
    }
    return result;
}

// --- Public API Methods ---

void ColumnManager::appendValue(const Value& value) {
    auto block_data = serializeForBlock(value);
    block_io_->append(block_data);
}

Value ColumnManager::readValue(uint64_t row_index) {
    auto block_data = block_io_->readAt(row_index);
    return deserializeFromBlock(block_data);
}

void ColumnManager::updateValue(uint64_t row_index, const Value& new_value) {
    if (isVariableLength()) {
        // For VarChar types, we must free the old data before writing the new data.
        auto old_block_data = block_io_->readAt(row_index);
        uint64_t old_offset = *reinterpret_cast<const uint64_t*>(old_block_data.data());
        
        // Only free the heap space if it was pointing to valid data (i.e., not the NULL sentinel).
        if (old_offset != std::numeric_limits<uint64_t>::max()) {
            // To know the size of the old data, we must read its length prefix from the heap.
            std::vector<char> len_bytes = heap_io_->read(old_offset, sizeof(uint16_t));
            uint16_t old_len = *reinterpret_cast<const uint16_t*>(len_bytes.data());
            
            // Add the newly freed chunk (offset and total size) to our freelist cache.
            ensureFreelistIsLoaded();
            SpaceManager::add(freelist_cache_.value(), old_offset, sizeof(old_len) + old_len);
            freelist_is_dirty_ = true;
        }
    }
    
    // Now, serialize the new value. This process is the same for both inserts and updates.
    // The serializeForBlock function will automatically try to use the freelist,
    // which we may have just added a new chunk to.
    auto new_block_data = serializeForBlock(new_value);
    
    // Finally, write the new 8-byte block data (containing either a new offset or a fixed-size value)
    // to the specified row in the .dt file.
    block_io_->writeAt(row_index, new_block_data);
}

void ColumnManager::swapAndPop(uint64_t row_index) {
    uint64_t row_count = getRowCount();

    if (row_index >= row_count) {
        throw std::out_of_range("Cannot swap-and-pop: row index is out of bounds.");
    }
    
    uint64_t last_row_index = row_count - 1;

    // If the row being deleted is a variable-length type, we must free its associated heap data.
    if (isVariableLength()) {
        auto deleted_block_data = block_io_->readAt(row_index);
        uint64_t deleted_offset;
        memcpy(&deleted_offset, deleted_block_data.data(), sizeof(deleted_offset));

        // As before, only free if it's not a NULL sentinel.
        if (deleted_offset != std::numeric_limits<uint64_t>::max()) {
            // Read length prefix to determine total size.
            std::vector<char> len_bytes = heap_io_->read(deleted_offset, sizeof(uint16_t));
            uint16_t len;
            memcpy(&len, len_bytes.data(), sizeof(len));
            
            // Add the chunk to the freelist.
            ensureFreelistIsLoaded();
            SpaceManager::add(freelist_cache_.value(), deleted_offset, sizeof(len) + len);
            freelist_is_dirty_ = true;
        }
    }
    
    // If the row to be deleted is not the last row, we overwrite it
    // with the data from the last row.
    if (row_index != last_row_index) {
        auto last_row_data = block_io_->readLast();
        block_io_->writeAt(row_index, last_row_data);
    }
    
    // Finally, truncate the file to remove the last row (which has now either
    // been moved or was the one we wanted to delete anyway).
    if (row_count > 0) {
        block_io_->truncate();
    }
}

uint64_t ColumnManager::getRowCount() const {
    return block_io_->getRowCount();
}

// Static method to delete all physical files for a given column.
void ColumnManager::dropFiles(const std::filesystem::path& table_data_dir, uint16_t column_link) {
    std::filesystem::path file_base_path = table_data_dir / linkToHex(column_link);
    std::error_code ec; // Use error_code to prevent exceptions if files don't exist.

    auto dt_path = file_base_path;
    std::filesystem::remove(dt_path.replace_extension(".dt"), ec);
    
    auto bg_path = file_base_path;
    std::filesystem::remove(bg_path.replace_extension(".bg"), ec);

    auto sp_path = file_base_path;
    std::filesystem::remove(sp_path.replace_extension(".sp"), ec);
}

// --- Private Helper Methods ---

// Lazy-loads the freelist from disk into the cache.
void ColumnManager::ensureFreelistIsLoaded() {
    // This is only relevant for variable-length types that have a heap.
    if (!heap_io_) return;

    // If the cache is empty (has no value), it needs to be loaded.
    if (!freelist_cache_.has_value()) {
        freelist_cache_ = std::vector<FreeSpaceRecord>();
        // Only try to load from the .sp file if it actually exists.
        if (std::filesystem::exists(sp_path_)) {
            SpaceManager::load(sp_path_, freelist_cache_.value());
        }
        // After loading, the cache is in sync with the disk, so it's not dirty.
        freelist_is_dirty_ = false;
    }
}

// Writes the freelist cache to disk if it has been modified.
void ColumnManager::persistFreelistIfDirty() {
    if (freelist_cache_.has_value() && freelist_is_dirty_) {
        SpaceManager::persist(sp_path_, freelist_cache_.value());
        freelist_is_dirty_ = false; // The cache is now clean.
    }
}

// Checks if the column type is variable-length by inspecting its type code's most significant bit.
bool ColumnManager::isVariableLength() const {
    return (static_cast<uint8_t>(data_type_) & 0b10000000) != 0;
}