cmake_minimum_required(VERSION 3.20)

# win + r --> powershell --> cd C:\ --> git clone https://github.com/Microsoft/vcpkg.git --> cd vcpkg
# .\bootstrap-vcpkg.bat --> .\vcpkg install nlohmann-json:x64-windows --> .\vcpkg install cpr:x64-windows
# .\vcpkg install libsodium:x64-windows --> .\vcpkg install asio:x64-windows
set(CMAKE_TOOLCHAIN_FILE "C:/Users/user/vcpkg/scripts/buildsystems/vcpkg.cmake") # сюда свой путь просто укажи вместо C:/Users/user/vcpkg/ скорее всего у тебя просто будет C:/vcpkg/scripts/buildsystems/vcpkg.cmake

project(VK_Database_API_Layer CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB_RECURSE SOURCES "src/*.cpp")
file(GLOB_RECURSE HEADERS "include/*.h")

add_executable(VK_Database_API_Layer
        ${SOURCES}
        ${HEADERS}
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

find_package(nlohmann_json CONFIG REQUIRED)
find_package(cpr CONFIG REQUIRED)
find_package(unofficial-sodium CONFIG REQUIRED)
find_package(asio CONFIG REQUIRED)

include(FetchContent)
set(CROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_DOCS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
        crow
        GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
        GIT_TAG master
)
FetchContent_MakeAvailable(crow)

target_include_directories(VK_Database_API_Layer
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${crow_SOURCE_DIR}/include
)

target_link_libraries(VK_Database_API_Layer
        PRIVATE
        nlohmann_json::nlohmann_json
        cpr::cpr
        unofficial-sodium::sodium
        asio::asio
        Crow::Crow
)

if(WIN32)
    target_link_libraries(VK_Database_API_Layer PRIVATE ws2_32 wsock32)
    target_compile_definitions(VK_Database_API_Layer PRIVATE
            _WIN32_WINNT=0x0601
            ASIO_STANDALONE
    )
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
