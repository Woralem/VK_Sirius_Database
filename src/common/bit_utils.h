#pragma once

#include <cstdint>
#include <string>

/**
 * @file bit_utils.h
 * @brief Contains low-level, high-performance utility functions for bit and byte manipulation.
 */

/// A lookup table for fast conversion of a byte's high or low nibble to a hexadecimal character.
constexpr char HEX_CHARS[] = "0123456789ABCDEF";

/**
 * @brief Converts a single byte to its two-character hexadecimal representation using a lookup table.
 * @param byte The byte to convert (e.g., 255).
 * @param out A pointer to a character buffer of at least size 2. The result (e.g., "FF") will be written here.
 */
inline void byte_to_hex_lut(uint8_t byte, char* out) {
    // Use the lookup table for a branch-free, fast conversion.
    out[0] = HEX_CHARS[byte >> 4];   // Get the high 4 bits.
    out[1] = HEX_CHARS[byte & 0x0F]; // Get the low 4 bits.
}