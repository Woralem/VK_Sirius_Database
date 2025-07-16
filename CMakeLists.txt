cmake_minimum_required(VERSION 3.20)
project(VK_Database_API_Layer CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

file(GLOB_RECURSE SOURCES "src/*.cpp")
file(GLOB_RECURSE HEADERS "include/*.h")
add_executable(VK_Database_API_Layer
        ${SOURCES}
        ${HEADERS}
        src/sessionManager.cpp
        include/userManager.h
        src/userManager.cpp
)

# Настройка выходных каталогов
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# Зависимости для CPR
if(UNIX AND NOT APPLE)
    find_package(CURL REQUIRED)
    find_package(Threads REQUIRED)
    set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
    set(CMAKE_INSTALL_RPATH "\$ORIGIN")
endif()

# Загрузка зависимостей
FetchContent_Declare(
        asio
        GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
        GIT_TAG asio-1-28-0
)
FetchContent_MakeAvailable(asio)
set(ASIO_INCLUDE_DIR ${asio_SOURCE_DIR}/asio/include)
set(asio_SOURCE_DIR ${asio_SOURCE_DIR} CACHE PATH "" FORCE)

# Добавьте эту строку для Crow
set(CROW_FORCE_USE_EXTERNAL_ASIO ON CACHE BOOL "" FORCE)
FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.2
)
FetchContent_Declare(
        crow
        GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
        GIT_TAG master
)
FetchContent_Declare(
        cpr
        GIT_REPOSITORY https://github.com/libcpr/cpr.git
        GIT_TAG 1.10.5
)
FetchContent_Declare(
        Sodium
        GIT_REPOSITORY https://github.com/robinlinden/libsodium-cmake.git
        GIT_TAG e5b985ad0dd235d8c4307ea3a385b45e76c74c6a
)

# Настройка параметров зависимостей
set(ASIO_STANDALONE ON CACHE BOOL "" FORCE)
set(CROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(SODIUM_DISABLE_TESTS ON)

FetchContent_MakeAvailable(asio nlohmann_json crow cpr Sodium)

# Настройка include директорий
target_include_directories(VK_Database_API_Layer
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${crow_SOURCE_DIR}/include
        ${nlohmann_json_SOURCE_DIR}/include
        ${asio_SOURCE_DIR}/asio/include
)

# Настройка линковки
target_link_libraries(VK_Database_API_Layer
        PRIVATE
        nlohmann_json::nlohmann_json
        Crow::Crow
        cpr::cpr
        sodium
)

# Платформо-специфичные настройки
if(WIN32)
    target_link_libraries(VK_Database_API_Layer PRIVATE ws2_32 wsock32)
    target_compile_definitions(VK_Database_API_Layer PRIVATE _WIN32_WINNT=0x0601)
else()
    target_link_libraries(VK_Database_API_Layer PRIVATE Threads::Threads CURL::libcurl)
endif()

if(CMAKE_EXPORT_COMPILE_COMMANDS)
    set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES
            ${CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES})
endif()