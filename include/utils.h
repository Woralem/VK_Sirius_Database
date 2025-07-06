#pragma once
#include "config.h"
#include <string>
#include <string_view>
#include <format>
#include <ranges>
#include <concepts>
#include <type_traits>

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

template<AssociativeContainer Container, typename Key>
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