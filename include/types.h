// --- FINAL, CORRECTED types.h ---
#pragma once

#include <string>
#include <vector>
#include <array>
#include <cstdint>
#include <utility>

// --- Data Structures ---
// These are common data transfer structures used across the engine.

struct ColumnDef {
    std::string name;
    // DataType is forward-declared below, so we use it here.
    enum class DataType : std::uint8_t; 
    DataType type;
    bool primeryKey = false;
    bool notNull = false;
};

struct Options {
    std::vector<std::string> additionalTypes = {};
    int maxColumnLemgth = 16;
    std::vector<std::string> additionalChars = {};
    int maxStringLength = 16; // This likely needs to be an enum or code
    int gcFrequency = 7; // Frequency of garbage collection in days
};

struct Value { /*...*/ }; // Can use std::variant


// --- DataType Enum Definition ---

// Defines all supported data types, each with a unique byte code.
// The `std::uint8_t` base ensures a compact, single-byte representation.
enum class ColumnDef::DataType : std::uint8_t {
    // A sentinel value used for invalid or unrecognized type codes.
    Unknown   = 255,

    // --- Fixed-Size Types (MSB = 0) ---
    Null      = 0b00000000,
    TinyInt   = 0b00000001,
    SmallInt  = 0b00000010,
    Integer   = 0b00000011,
    BigInt    = 0b00000100,
    UTinyInt  = 0b00000101,
    USmallInt = 0b00000110,
    UInteger  = 0b00000111,
    UBigInt   = 0b00001000,
    Float     = 0b00001001,
    Double    = 0b00001010,
    Date      = 0b00001011,
    Time      = 0b00001100,
    Timestamp = 0b00001101,
    Boolean   = 0b00001110,

    // --- Variable-Size Types (MSB = 1) ---
    Decimal   = 0b10000000,
    VarChar   = 0b10000001,
    Text      = 0b10000010,
    VarBinary = 0b10000011,
    Blob      = 0b10000100,
    Uuid      = 0b10000101,
    Array     = 0b10000110,
    Json      = 0b10000111,
    JsonB     = 0b10001000,
    PhoneNumber = 0b10001001,
    EmailAddress = 0b10001010,

    // Added from your original file
    Address = 0b10001011,
    Telegram = 0b10001100
};


// --- Compile-time Lookup Table (LUT) ---
// This allows for O(1) conversion from a byte code to a DataType.
// Since these are `constexpr`, they are implicitly `inline` and safe for a header.

constexpr std::array<ColumnDef::DataType, 256> CreateTypeLut() {
    std::array<ColumnDef::DataType, 256> lut{};
    lut.fill(ColumnDef::DataType::Unknown);

    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Null)] = ColumnDef::DataType::Null;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::TinyInt)] = ColumnDef::DataType::TinyInt;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::SmallInt)] = ColumnDef::DataType::SmallInt;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Integer)] = ColumnDef::DataType::Integer;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::BigInt)] = ColumnDef::DataType::BigInt;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::UTinyInt)] = ColumnDef::DataType::UTinyInt;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::USmallInt)] = ColumnDef::DataType::USmallInt;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::UInteger)] = ColumnDef::DataType::UInteger;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::UBigInt)] = ColumnDef::DataType::UBigInt;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Float)] = ColumnDef::DataType::Float;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Double)] = ColumnDef::DataType::Double;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Date)] = ColumnDef::DataType::Date;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Time)] = ColumnDef::DataType::Time;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Timestamp)] = ColumnDef::DataType::Timestamp;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Boolean)] = ColumnDef::DataType::Boolean;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Decimal)] = ColumnDef::DataType::Decimal;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::VarChar)] = ColumnDef::DataType::VarChar;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Text)] = ColumnDef::DataType::Text;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::VarBinary)] = ColumnDef::DataType::VarBinary;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Blob)] = ColumnDef::DataType::Blob;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Uuid)] = ColumnDef::DataType::Uuid;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Array)] = ColumnDef::DataType::Array;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Json)] = ColumnDef::DataType::Json;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::JsonB)] = ColumnDef::DataType::JsonB;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::PhoneNumber)] = ColumnDef::DataType::PhoneNumber;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::EmailAddress)] = ColumnDef::DataType::EmailAddress;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Address)] = ColumnDef::DataType::Address;
    lut[static_cast<std::uint8_t>(ColumnDef::DataType::Telegram)] = ColumnDef::DataType::Telegram;

    return lut;
}

constexpr auto TYPE_LUT = CreateTypeLut();