cmake_minimum_required(VERSION 3.20)

project(maxcompute_odbc_driver VERSION 1.0.0 LANGUAGES CXX)

# Set C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# macOS 26+ may need explicit C++ standard library include path
if(APPLE)
    # Get SDK path
    execute_process(
        COMMAND xcrun --show-sdk-path
        OUTPUT_VARIABLE MACOS_SDK_PATH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(MACOS_SDK_PATH AND EXISTS "${MACOS_SDK_PATH}/usr/include/c++/v1")
        include_directories(SYSTEM "${MACOS_SDK_PATH}/usr/include/c++/v1")
        message(STATUS "Added C++ standard library include path: ${MACOS_SDK_PATH}/usr/include/c++/v1")
    endif()
endif()

# Disable Brotli support
add_definitions(-DCPPHTTPLIB_BROTLI_SUPPORT=0)
add_definitions(-DCPPHTTPLIB_USE_BROTLI_IF_AVAILABLE=0)

# Prevent Windows headers from defining min/max macros and reduce bloat
add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN)

# Set output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if(MSVC)
  add_compile_options(/source-charset:utf-8 /execution-charset:utf-8)
endif()

# use vcpkg
if(NOT WIN32)
    find_package(ODBC REQUIRED)
    find_package(Iconv REQUIRED)
endif()

find_package(OpenSSL CONFIG REQUIRED)
find_package(ZLIB REQUIRED)
find_package(date CONFIG REQUIRED)
# cpp-base64 is header-only, handled via include directories
find_path(CPP_BASE64_INCLUDE_DIRS "cpp-base64/base64.cpp")
find_package(httplib CONFIG REQUIRED)
find_package(pugixml CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(tl-expected CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(crc32c CONFIG REQUIRED)

# Find libltdl for dynamic loading support
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LIBLTDL QUIET IMPORTED_TARGET libltdl)
    if(LIBLTDL_FOUND)
        message(STATUS "Found libltdl via pkg-config")
    else()
        message(STATUS "libltdl not found via pkg-config, will try vcpkg")
    endif()
endif()

# Add include directories
include_directories(include)

# Add subdirectories
add_subdirectory(src)

# Enable testing
enable_testing()
add_subdirectory(test)

# Add code formatting targets
find_program(CLANG_FORMAT "clang-format")
if(CLANG_FORMAT)
    # Find all source files to format
    file(GLOB_RECURSE FORMAT_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/test/*.h"
    )

    # Create format target
    add_custom_target(format
        COMMAND ${CLANG_FORMAT} -i --style=file ${FORMAT_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Formatting source code with clang-format"
    )

    # Create check-format target (dry run, no modifications)
    add_custom_target(check-format
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_format.sh
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Checking code formatting"
    )
endif()