#include <iostream>
#include <array>
#include <cstdint>
#include <string>

// --- HOW TO ADD A NEW DATA TYPE ---
//
// 1.  Define the type in the `DataType` enum below.
//     - Choose a unique byte code (a value from 0 to 254).
//     - Follow the convention for the most significant bit (MSB):
//       - `0b0xxxxxxx` for fixed-size types (e.g., Integer, Date).
//       - `0b1xxxxxxx` for variable-size types (e.g., VarChar, Blob).
//
// 2.  Register the new type in the `CreateTypeLut` factory function.
//     - Add a new line to map your type's byte code to its enum value:
//       `lut[static_cast<uint8_t>(DataType::YourNewType)] = DataType::YourNewType;`
//
// The `TYPE_LUT` is generated at compile time, so no further changes are needed.
// ------------------------------------


// Defines all supported data types, each with a unique byte code.
// The `std::uint8_t` base ensures a compact, single-byte representation.
// The most significant bit (MSB) distinguishes between fixed-size (MSB=0)
// and variable-size (MSB=1) types.
enum class DataType : std::uint8_t {
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
    PhoneNumber = 0b10001001, // Custom type
    EmailAddress = 0b10001010, // Custom type
    Address = 0b10001011,      // Custom type
    Telegram = 0b10001100      // Custom type
};

// A `constexpr` factory function that builds a lookup table (LUT) at compile time.
// This allows for O(1) conversion from a byte code to a `DataType`.
constexpr std::array<DataType, 256> CreateTypeLut() {
    std::array<DataType, 256> lut{};

    // Initialize all 256 possible byte values to `Unknown` by default.
    // This safely handles any byte code that isn't an explicitly defined type.
    lut.fill(DataType::Unknown);

    // Map the byte code of each defined type to its corresponding enum value.
    // The array index is the byte code, providing direct, constant-time lookup.
    lut[static_cast<std::uint8_t>(DataType::Null)] = DataType::Null;
    lut[static_cast<std::uint8_t>(DataType::TinyInt)] = DataType::TinyInt;
    lut[static_cast<std::uint8_t>(DataType::SmallInt)] = DataType::SmallInt;
    lut[static_cast<std::uint8_t>(DataType::Integer)] = DataType::Integer;
    lut[static_cast<std::uint8_t>(DataType::BigInt)] = DataType::BigInt;
    lut[static_cast<std::uint8_t>(DataType::UTinyInt)] = DataType::UTinyInt;
    lut[static_cast<std::uint8_t>(DataType::USmallInt)] = DataType::USmallInt;
    lut[static_cast<std::uint8_t>(DataType::UInteger)] = DataType::UInteger;
    lut[static_cast<std::uint8_t>(DataType::UBigInt)] = DataType::UBigInt;
    lut[static_cast<std::uint8_t>(DataType::Float)] = DataType::Float;
    lut[static_cast<std::uint8_t>(DataType::Double)] = DataType::Double;
    lut[static_cast<std::uint8_t>(DataType::Date)] = DataType::Date;
    lut[static_cast<std::uint8_t>(DataType::Time)] = DataType::Time;
    lut[static_cast<std::uint8_t>(DataType::Timestamp)] = DataType::Timestamp;
    lut[static_cast<std::uint8_t>(DataType::Boolean)] = DataType::Boolean;
    lut[static_cast<std::uint8_t>(DataType::Decimal)] = DataType::Decimal;
    lut[static_cast<std::uint8_t>(DataType::VarChar)] = DataType::VarChar;
    lut[static_cast<std::uint8_t>(DataType::Text)] = DataType::Text;
    lut[static_cast<std::uint8_t>(DataType::VarBinary)] = DataType::VarBinary;
    lut[static_cast<std::uint8_t>(DataType::Blob)] = DataType::Blob;
    lut[static_cast<std::uint8_t>(DataType::Uuid)] = DataType::Uuid;
    lut[static_cast<std::uint8_t>(DataType::Array)] = DataType::Array;
    lut[static_cast<std::uint8_t>(DataType::Json)] = DataType::Json;
    lut[static_cast<std::uint8_t>(DataType::JsonB)] = DataType::JsonB;
    lut[static_cast<std::uint8_t>(DataType::PhoneNumber)] = DataType::PhoneNumber;
    lut[static_cast<std::uint8_t>(DataType::EmailAddress)] = DataType::EmailAddress;
    lut[static_cast<std::uint8_t>(DataType::Address)] = DataType::Address;
    lut[static_cast<std::uint8_t>(DataType::Telegram)] = DataType::Telegram;

    return lut;
}

// The global, compile-time generated lookup table.
// Provides an efficient, safe way to map a byte to a `DataType`.
// Usage: `DataType type = TYPE_LUT[byte_code];`
constexpr auto TYPE_LUT = CreateTypeLut();