cmake_minimum_required(VERSION 3.20)
project(VK_Database_API_Layer CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

file(GLOB_RECURSE SOURCES "src/*.cpp")
file(GLOB_RECURSE HEADERS "include/*.cpp")
add_executable(VK_Database_API_Layer
        ${SOURCES}
        ${HEADERS}
)
if(WIN32) # Install dlls in the same directory as the executable on Windows
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
endif()
FetchContent_Declare(
        asio
        GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
        GIT_TAG asio-1-28-0
)
FetchContent_MakeAvailable(asio)
set(ASIO_INCLUDE_DIR
        ${asio_SOURCE_DIR}/asio/include CACHE INTERNAL ""
)
set(ASIO_STANDALONE ON CACHE BOOL "" FORCE)


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
set(CROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_DOCS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
        cpr
        GIT_REPOSITORY https://github.com/libcpr/cpr.git
        GIT_TAG 1.10.5
)
FetchContent_MakeAvailable(nlohmann_json crow cpr)
target_link_libraries(VK_Database_API_Layer
        PRIVATE
        nlohmann_json::nlohmann_json
        Crow::Crow
        cpr::cpr
        ws2_32 wsock32
)




target_include_directories(VK_Database_API_Layer
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${crow_SOURCE_DIR}/include
        ${nlohmann_json_SOURCE_DIR}/include
        ${ASIO_INCLUDE_DIR}
)


if(WIN32)
    target_link_libraries(VK_Database_API_Layer PRIVATE ws2_32 wsock32)
    target_compile_definitions(VK_Database_API_Layer PRIVATE _WIN32_WINNT=0x0601)
endif()
if(CMAKE_EXPORT_COMPILE_COMMANDS)
    set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES
            ${CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES})
endif()
