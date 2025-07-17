#pragma once
#include "config.h"
#include <string>
#include <string_view>
#include <format>
#include <ranges>
#include <concepts>
#include <type_traits>
#include <optional>

namespace utils {

template<typename... Args>
[[nodiscard]] constexpr std::string smart_format(std::string_view fmt, Args&&... args) {
    return std::vformat(fmt, std::make_format_args(args...));
}

template<std::ranges::range Range, typename Predicate>
[[nodiscard]] constexpr bool smart_all_of(Range&& range, Predicate pred) {
    return std::ranges::all_of(range, pred);
}

template<std::ranges::range Range, typename Predicate>
[[nodiscard]] constexpr bool smart_any_of(Range&& range, Predicate pred) {
    return std::ranges::any_of(range, pred);
}

template<std::ranges::range Range, typename Predicate>
[[nodiscard]] constexpr auto smart_find_if(Range&& range, Predicate pred) {
    return std::ranges::find_if(range, pred);
}

template<std::ranges::range Range, typename UnaryOperation>
constexpr void smart_transform_inplace(Range&& range, UnaryOperation op) {
    std::ranges::transform(range, std::ranges::begin(range), op);
}

template<typename Container, typename Predicate>
constexpr void smart_erase_if(Container& container, Predicate pred) {
    std::erase_if(container, pred);
}

template<typename Container, typename Value>
constexpr void smart_erase(Container& container, const Value& value) {
    std::erase(container, value);
}

// UTF-8 validation functions
[[nodiscard]] inline bool isValidUtf8(std::string_view str) {
    size_t i = 0;
    while (i < str.length()) {
        unsigned char c = static_cast<unsigned char>(str[i]);

        if (c <= 0x7F) {
            // ASCII character
            i++;
        } else if ((c & 0xE0) == 0xC0) {
            // 2-byte sequence
            if (i + 1 >= str.length()) return false;
            if ((static_cast<unsigned char>(str[i + 1]) & 0xC0) != 0x80) return false;
            // Check for overlong encoding
            if ((c & 0x1E) == 0) return false;
            i += 2;
        } else if ((c & 0xF0) == 0xE0) {
            // 3-byte sequence
            if (i + 2 >= str.length()) return false;
            if ((static_cast<unsigned char>(str[i + 1]) & 0xC0) != 0x80) return false;
            if ((static_cast<unsigned char>(str[i + 2]) & 0xC0) != 0x80) return false;
            // Check for overlong encoding
            if (c == 0xE0 && (static_cast<unsigned char>(str[i + 1]) & 0x20) == 0) return false;
            i += 3;
        } else if ((c & 0xF8) == 0xF0) {
            // 4-byte sequence
            if (i + 3 >= str.length()) return false;
            if ((static_cast<unsigned char>(str[i + 1]) & 0xC0) != 0x80) return false;
            if ((static_cast<unsigned char>(str[i + 2]) & 0xC0) != 0x80) return false;
            if ((static_cast<unsigned char>(str[i + 3]) & 0xC0) != 0x80) return false;
            // Check for overlong encoding
            if (c == 0xF0 && (static_cast<unsigned char>(str[i + 1]) & 0x30) == 0) return false;
            // Check for values > U+10FFFF
            if (c > 0xF4 || (c == 0xF4 && static_cast<unsigned char>(str[i + 1]) > 0x8F)) return false;
            i += 4;
        } else {
            // Invalid UTF-8 start byte
            return false;
        }
    }
    return true;
}

[[nodiscard]] inline std::optional<size_t> findInvalidUtf8Byte(std::string_view str) {
    size_t i = 0;
    while (i < str.length()) {
        unsigned char c = static_cast<unsigned char>(str[i]);

        if (c <= 0x7F) {
            i++;
        } else if ((c & 0xE0) == 0xC0) {
            if (i + 1 >= str.length() || (static_cast<unsigned char>(str[i + 1]) & 0xC0) != 0x80) {
                return i;
            }
            if ((c & 0x1E) == 0) return i;
            i += 2;
        } else if ((c & 0xF0) == 0xE0) {
            if (i + 2 >= str.length() ||
                (static_cast<unsigned char>(str[i + 1]) & 0xC0) != 0x80 ||
                (static_cast<unsigned char>(str[i + 2]) & 0xC0) != 0x80) {
                return i;
            }
            if (c == 0xE0 && (static_cast<unsigned char>(str[i + 1]) & 0x20) == 0) return i;
            i += 3;
        } else if ((c & 0xF8) == 0xF0) {
            if (i + 3 >= str.length() ||
                (static_cast<unsigned char>(str[i + 1]) & 0xC0) != 0x80 ||
                (static_cast<unsigned char>(str[i + 2]) & 0xC0) != 0x80 ||
                (static_cast<unsigned char>(str[i + 3]) & 0xC0) != 0x80) {
                return i;
            }
            if (c == 0xF0 && (static_cast<unsigned char>(str[i + 1]) & 0x30) == 0) return i;
            if (c > 0xF4 || (c == 0xF4 && static_cast<unsigned char>(str[i + 1]) > 0x8F)) return i;
            i += 4;
        } else {
            return i;
        }
    }
    return std::nullopt;
}

[[nodiscard]] inline std::string sanitizeUtf8(std::string_view str) {
    std::string result;
    result.reserve(str.length());

    size_t i = 0;
    while (i < str.length()) {
        unsigned char c = static_cast<unsigned char>(str[i]);

        if (c <= 0x7F) {
            result.push_back(str[i]);
            i++;
        } else if ((c & 0xE0) == 0xC0) {
            if (i + 1 < str.length() && (static_cast<unsigned char>(str[i + 1]) & 0xC0) == 0x80 && (c & 0x1E) != 0) {
                result.push_back(str[i]);
                result.push_back(str[i + 1]);
                i += 2;
            } else {
                result.push_back('?');
                i++;
            }
        } else if ((c & 0xF0) == 0xE0) {
            if (i + 2 < str.length() &&
                (static_cast<unsigned char>(str[i + 1]) & 0xC0) == 0x80 &&
                (static_cast<unsigned char>(str[i + 2]) & 0xC0) == 0x80 &&
                !(c == 0xE0 && (static_cast<unsigned char>(str[i + 1]) & 0x20) == 0)) {
                result.push_back(str[i]);
                result.push_back(str[i + 1]);
                result.push_back(str[i + 2]);
                i += 3;
            } else {
                result.push_back('?');
                i++;
            }
        } else if ((c & 0xF8) == 0xF0) {
            if (i + 3 < str.length() &&
                (static_cast<unsigned char>(str[i + 1]) & 0xC0) == 0x80 &&
                (static_cast<unsigned char>(str[i + 2]) & 0xC0) == 0x80 &&
                (static_cast<unsigned char>(str[i + 3]) & 0xC0) == 0x80 &&
                !(c == 0xF0 && (static_cast<unsigned char>(str[i + 1]) & 0x30) == 0) &&
                (c < 0xF4 || (c == 0xF4 && static_cast<unsigned char>(str[i + 1]) <= 0x8F))) {
                result.push_back(str[i]);
                result.push_back(str[i + 1]);
                result.push_back(str[i + 2]);
                result.push_back(str[i + 3]);
                i += 4;
            } else {
                result.push_back('?');
                i++;
            }
        } else {
            result.push_back('?');
            i++;
        }
    }
    return result;
}

class StringBuilder {
private:
    std::string buffer;

public:
    explicit constexpr StringBuilder(size_t reserve_size = 256) {
        buffer.reserve(reserve_size);
    }

    template<typename T>
    constexpr StringBuilder& operator<<(T&& value) {
        if constexpr (std::same_as<std::decay_t<T>, std::string_view>) {
            buffer.append(value);
        } else if constexpr (std::same_as<std::decay_t<T>, std::string>) {
            buffer.append(value);
        } else if constexpr (std::same_as<std::decay_t<T>, const char*>) {
            buffer.append(value);
        } else if constexpr (std::integral<std::decay_t<T>>) {
            buffer.append(std::format("{}", value));
        } else if constexpr (std::floating_point<std::decay_t<T>>) {
            buffer.append(std::format("{}", value));
        } else {
            buffer.append(std::format("{}", value));
        }
        return *this;
    }

    constexpr StringBuilder& append(std::string_view str) {
        buffer.append(str);
        return *this;
    }

    constexpr StringBuilder& append(char c) {
        buffer.push_back(c);
        return *this;
    }

    constexpr StringBuilder& append(const std::string& str) {
        buffer.append(str);
        return *this;
    }

    constexpr StringBuilder& append(size_t count, char c) {
        buffer.append(count, c);
        return *this;
    }

    [[nodiscard]] constexpr std::string str() && {
        return std::move(buffer);
    }

    [[nodiscard]] constexpr const std::string& str() const& {
        return buffer;
    }

    [[nodiscard]] constexpr size_t size() const noexcept {
        return buffer.size();
    }

    constexpr void clear() noexcept {
        buffer.clear();
    }

    constexpr void reserve(size_t size) {
        buffer.reserve(size);
    }
};

template<typename Container, typename Key>
concept AssociativeContainer = requires(Container c, Key k) {
    c.find(k);
    c.end();
};

template<typename Container, typename Key> requires AssociativeContainer<Container, Key>
[[nodiscard]] constexpr bool contains(const Container& container, const Key& key) {
    if constexpr (requires { container.contains(key); }) {
        return container.contains(key);
    } else {
        return container.find(key) != container.end();
    }
}

[[nodiscard]] constexpr std::string_view trim_left(std::string_view str) {
    auto pos = std::ranges::find_if_not(str, [](char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    });
    return std::string_view{pos, str.end()};
}

[[nodiscard]] constexpr std::string_view trim_right(std::string_view str) {
    auto pos = std::ranges::find_if_not(str | std::views::reverse, [](char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }).base();
    return std::string_view{str.begin(), pos};
}

[[nodiscard]] constexpr std::string_view trim(std::string_view str) {
    return trim_right(trim_left(str));
}

} // namespace utils