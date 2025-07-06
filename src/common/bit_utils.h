#pragma once
#include <cstdint>
#include <string>

// Глобальная или статическая константа для таблицы поиска
constexpr char HEX_CHARS[] = "0123456789ABCDEF";

// Определение функции теперь прямо в заголовочном файле
inline void byte_to_hex_lut(uint8_t byte, char* out) {
    out[0] = HEX_CHARS[byte >> 4];      // Старшие 4 бита
    out[1] = HEX_CHARS[byte & 0x0F];    // Младшие 4 бита
}