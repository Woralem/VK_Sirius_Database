#pragma once

#include <string>
#include <vector>
#include <array>
#include <variant>
#include <cstdint>
#include <string_view>
#include <stdexcept>
#include <utility>
#include <map>

/**
 * @file types.h
 * @brief Defines fundamental data structures and type definitions used throughout the storage engine.
 *
 * This file contains the core data transfer objects (DTOs) like `Value`, `ColumnDef`,
 * and `Options`, as well as the central `DataType` enum and its related conversion utilities.
 */


// --- Value Definition ---

/**
 * @using ValueType
 * @brief A type-safe union to hold any possible data value.
 *
 * `std::variant` is used to represent a value, which can be a number, string, boolean,
 * or a special state representing SQL NULL (`std::monostate`).
 */
using ValueType = std::variant<
    std::monostate, // For NULL values
    int64_t,        // For TinyInt, SmallInt, Integer, BigInt
    double,         // For Float, Double
    bool,           // For Boolean
    std::string     // For VarChar, Text, etc.
>;

/// A wrapper struct for a `ValueType`, used for passing data.
struct Value {
    ValueType data;
};

/// Defines the properties of a column, used for table creation and schema management.
struct ColumnDef {
    std::string name;
    /// The data type of the column.
    enum class DataType : std::uint8_t; 
    DataType type;
    bool primeryKey = false;
    bool notNull = false;
};

/// A structure for passing optional configuration when creating a table.
struct Options {
    std::vector<std::string> additionalTypes = {};
    // Use unsigned types to prevent negative inputs from wrapping around.
    uint8_t maxColumnLemgth = 16; 
    std::vector<std::string> additionalChars = {};
    uint8_t maxStringLength = 0; // Use a code: 0=16, 1=32, 2=64, 3=255
    uint16_t gcFrequency = 7; 
};


// --- DataType Enum Definition ---

/**
 * @enum ColumnDef::DataType
 * @brief Defines all supported data types, each with a unique byte code.
 *
 * The enum uses `std::uint8_t` as its underlying type for a compact, single-byte
 * on-disk representation. A convention is used where the most significant bit (MSB)
 * indicates the storage type:
 * - MSB = 0: Fixed-size type (e.g., INTEGER, DOUBLE).
 * - MSB = 1: Variable-size type (e.g., VARCHAR, TEXT), which requires heap storage.
 */
enum class ColumnDef::DataType : std::uint8_t {
    /// A sentinel value used for invalid or unrecognized type codes.
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
    Address   = 0b10001011,
    Telegram  = 0b10001100
};


// --- Compile-time Lookup Table (LUT) ---
// This allows for O(1) conversion from a byte code to a DataType.

/// Generates a lookup table at compile time to map a raw byte to a DataType.
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

/// A global, compile-time generated lookup table for byte-to-DataType conversion.
constexpr auto TYPE_LUT = CreateTypeLut();

/**
 * @brief Converts a user-provided type string into the internal DataType enum.
 * @details This function is used to parse DDL commands like `CREATE TABLE` and `ALTER TABLE`.
 * It uses a static map for efficient, case-sensitive lookups.
 * @param type_str A string_view of the type name (e.g., "INTEGER", "VARCHAR").
 * @return The corresponding ColumnDef::DataType enum value.
 * @throws std::invalid_argument if the type string is not recognized.
 */
inline ColumnDef::DataType stringToDataType(std::string_view type_str) {
    // A static map provides an efficient, one-time-initialized lookup.
    // Using string_view for the key avoids string copies during lookup.
    static const std::map<std::string_view, ColumnDef::DataType> type_map = {
        {"NULL", ColumnDef::DataType::Null},
        {"TINYINT", ColumnDef::DataType::TinyInt},
        {"SMALLINT", ColumnDef::DataType::SmallInt},
        {"INTEGER", ColumnDef::DataType::Integer},
        {"BIGINT", ColumnDef::DataType::BigInt},
        {"UTINYINT", ColumnDef::DataType::UTinyInt},
        {"USMALLINT", ColumnDef::DataType::USmallInt},
        {"UINTEGER", ColumnDef::DataType::UInteger},
        {"UBIGINT", ColumnDef::DataType::UBigInt},
        {"FLOAT", ColumnDef::DataType::Float},
        {"DOUBLE", ColumnDef::DataType::Double},
        {"DATE", ColumnDef::DataType::Date},
        {"TIME", ColumnDef::DataType::Time},
        {"TIMESTAMP", ColumnDef::DataType::Timestamp},
        {"BOOLEAN", ColumnDef::DataType::Boolean},
        {"DECIMAL", ColumnDef::DataType::Decimal},
        {"VARCHAR", ColumnDef::DataType::VarChar},
        {"TEXT", ColumnDef::DataType::Text},
        {"VARBINARY", ColumnDef::DataType::VarBinary},
        {"BLOB", ColumnDef::DataType::Blob},
        {"UUID", ColumnDef::DataType::Uuid},
        {"ARRAY", ColumnDef::DataType::Array},
        {"JSON", ColumnDef::DataType::Json},
        {"JSONB", ColumnDef::DataType::JsonB},
        {"PHONENUMBER", ColumnDef::DataType::PhoneNumber},
        {"EMAILADDRESS", ColumnDef::DataType::EmailAddress},
        {"ADDRESS", ColumnDef::DataType::Address},
        {"TELEGRAM", ColumnDef::DataType::Telegram}
    };
    
    auto it = type_map.find(type_str);
    if (it != type_map.end()) {
        return it->second;
    }
    // Provide a clear error message if the type is not found.
    throw std::invalid_argument(std::string("Unknown data type '") + std::string(type_str) + "'");
}