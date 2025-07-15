#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <functional> // For std::hash

// --- 1. The key structure for the table name ---
// Represents the 12-byte (96-bit) name as two integer types for efficient hashing and comparison.
struct TableNameKey {
    uint64_t part1; // First 8 bytes (64 bits)
    uint32_t part2; // Remaining 4 bytes (32 bits)

    bool operator==(const TableNameKey& other) const {
        return part1 == other.part1 && part2 == other.part2;
    }
};

// --- 2. A custom hasher for TableNameKey ---
// Required for using TableNameKey in std::unordered_map.
struct KeyHasher {
    std::size_t operator()(const TableNameKey& k) const {
        const std::size_t h1 = std::hash<uint64_t>{}(k.part1);
        const std::size_t h2 = std::hash<uint32_t>{}(k.part2);
        // Combine hashes using a bit shift and XOR to minimize collisions.
        return h1 ^ (h2 << 1);
    }
};


// --- 3. Encoding/Decoding Logic and LUTs ---

// Provides a 6-bit encoding for a specific set of 64 characters.
constexpr std::array<std::pair<char, uint8_t>, 64> ENCODING_MAP = {{
    {'_', 0}, {'B', 1}, {'C', 2}, {'D', 3}, {'E', 4}, {'F', 5}, {'G', 6}, {'H', 7},
    {'I', 8}, {'J', 9}, {'K', 10}, {'L', 11}, {'M', 12}, {'N', 13}, {'O', 14}, {'P', 15},
    {'Q', 16}, {'R', 17}, {'S', 18}, {'T', 19}, {'U', 20}, {'V', 21}, {'W', 22}, {'X', 23},
    {'Y', 24}, {'Z', 25}, {'a', 26}, {'b', 27}, {'c', 28}, {'d', 29}, {'e', 30}, {'f', 31},
    {'g', 32}, {'h', 33}, {'i', 34}, {'j', 35}, {'k', 36}, {'l', 37}, {'m', 38}, {'n', 39},
    {'o', 40}, {'p', 41}, {'q', 42}, {'r', 43}, {'s', 44}, {'t', 45}, {'u', 46}, {'v', 47},
    {'w', 48}, {'x', 49}, {'y', 50}, {'z', 51}, {'0', 52}, {'1', 53}, {'2', 54}, {'3', 55},
    {'4', 56}, {'5', 57}, {'6', 58}, {'7', 59}, {'8', 60}, {'9', 61}, {'A', 62}, {'-', 63}
}};

// Creates an encoding LUT (char -> 6-bit code).
constexpr auto createEncodingLut() {
    std::array<uint8_t, 128> lut{};
    lut.fill(255); // 255 indicates an invalid character.
    for (const auto& pair : ENCODING_MAP) {
        lut[static_cast<size_t>(pair.first)] = pair.second;
    }
    return lut;
}

// Creates a decoding LUT (6-bit code -> char).
constexpr auto createDecodingLut() {
    std::array<char, 64> lut{};
    for (const auto& pair : ENCODING_MAP) {
        lut[pair.second] = pair.first;
    }
    return lut;
}

// Global, compile-time generated LUTs.
// constexpr makes them implicitly inline, safe to define in a header.
constexpr auto ENCODING_LUT = createEncodingLut();
constexpr auto DECODING_LUT = createDecodingLut();


// --- 4. Public Helper Functions ---

// Overload the function for validation with a dynamic length
inline void validateTableName(const std::string& name, uint8_t max_length) {
    if (name.empty()) throw std::invalid_argument("Table/Column name cannot be empty.");
    // Use the passed max_length for the check
    if (name.length() > max_length) {
        throw std::invalid_argument("Table/Column name exceeds maximum length of " + std::to_string(max_length) + " characters.");
    }
    if (name.back() == '_') throw std::invalid_argument("Table/Column name cannot end with '_'.");
    if (name.find_first_not_of('-') == std::string::npos) throw std::invalid_argument("Table/Column name cannot consist only of '-'.");
    
    for (char c : name) {
        if (static_cast<unsigned char>(c) >= 128 || ENCODING_LUT[static_cast<size_t>(c)] == 255) {
            throw std::invalid_argument("Table/Column name contains invalid character: " + std::string(1, c));
        }
    }
}

// Keep the original for backwards compatibility or for the fixed-size Catalog
inline void validateTableName(const std::string& name) {
    validateTableName(name, 16); // Default to 16
}

// Encodes a string into a 12-byte key.
inline TableNameKey stringToKey(const std::string& s) {
    std::array<uint8_t, 12> buffer{};
    int bit_pos = 0;
    for (char c : s) {
        uint8_t code = ENCODING_LUT[static_cast<size_t>(c)];
        int byte_idx = bit_pos / 8;
        int bit_in_byte = bit_pos % 8;
        buffer[byte_idx] |= (code << bit_in_byte);
        if (bit_in_byte > 2) {
            buffer[byte_idx + 1] |= (code >> (8 - bit_in_byte));
        }
        bit_pos += 6;
    }
    return *reinterpret_cast<TableNameKey*>(buffer.data());
}

// Decodes a 12-byte key back into a string.
inline std::string keyToString(const TableNameKey& key) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(&key);
    std::string result;
    result.reserve(16);

    for (int bit_pos = 0; bit_pos < 96; bit_pos += 6) { // 16 chars * 6 bits = 96 bits
        int byte_idx = bit_pos / 8;
        int bit_in_byte = bit_pos % 8;

        uint16_t temp_window; // Use a 16-bit integer to safely read across byte boundary

        if (byte_idx >= 11) {
            // We are at the last byte. Only read that single byte into our window.
            temp_window = bytes[byte_idx];
        } else {
            // Safely copy 2 bytes (16 bits) into our window.
            // The compiler will often optimize this to a single register load.
            memcpy(&temp_window, &bytes[byte_idx], sizeof(uint16_t));
        }

        // Shift the window to align our 6 bits to the right, then mask them.
        // The mask 0x3F (binary 00111111) isolates the 6 bits.
        uint8_t code = (temp_window >> bit_in_byte) & 0x3F;

        // Decode the 6-bit code back to a character and append it.
        result += DECODING_LUT[code];
    }
    
    // After decoding, the string may be padded with trailing '_' characters
    // because stringToKey fills the unused part of the key with zero-bits.
    // We need to trim this padding.
    size_t last_char_pos = result.find_last_not_of('_');

    if (last_char_pos == std::string::npos) {
        throw std::runtime_error("Error in decoding key, it's empty");
    }

    // Resize the string to remove the trailing padding characters.
    result.resize(last_char_pos + 1);

    return result;
}