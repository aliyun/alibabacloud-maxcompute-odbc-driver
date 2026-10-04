# test/CMakeLists.txt
#
# Build modes are decided by the unit-test gate (cmake/McoTesting.cmake, called
# from the top-level CMakeLists.txt):
#
#   BUILD_TESTING=OFF -> this directory configures nothing. The gate already
#                        printed the build-only marker.
#   BUILD_TESTING=ON  -> Google Test is guaranteed to exist (otherwise configure
#                        already failed) and every suite listed below has to be
#                        built and registered, otherwise configure fails instead
#                        of quietly producing a zero-test "successful" build.

if(NOT MCO_BUILD_TESTS)
    return()
endif()

# 定义 Apple 专属框架（只在 Apple 平台有效）
set(APPLE_FRAMEWORKS "")
if(APPLE)
    set(APPLE_FRAMEWORKS
            "-framework CoreFoundation"
            "-framework CFNetwork"
            "-framework Security"
            "-framework SystemConfiguration"
    )
    find_library(LIBLTDL_LIBRARIES NAMES ltdl PATHS /opt/homebrew/lib /usr/local/lib)
    if(LIBLTDL_LIBRARIES)
        message(STATUS "Found libltdl: ${LIBLTDL_LIBRARIES}")
    else()
        message(WARNING "libltdl not found, you may need to install it via 'brew install libtool'")
    endif()
endif()


set(ODBC_FRAMEWORKS "")
if(NOT WIN32)
    set(ODBC_FRAMEWORKS ODBC::ODBC Iconv::Iconv)
endif()

# 添加测试主函数
add_library(test_main test_main.cpp)
target_link_libraries(test_main PUBLIC GTest::gtest ${APPLE_FRAMEWORKS})

# 添加配置模块测试
add_executable(config_test unit/config_test.cpp)
target_link_libraries(config_test PRIVATE config common test_main)
add_test(NAME config_test COMMAND config_test)

# 添加日志模块测试
add_executable(logging_test unit/logging_test.cpp)
target_link_libraries(logging_test PRIVATE common test_main)
add_test(NAME logging_test COMMAND logging_test)

# 添加模型测试
add_executable(models_test unit/models_test.cpp)
target_link_libraries(models_test PRIVATE maxcompute_client config common test_main)
add_test(NAME models_test COMMAND models_test)

# 添加设置解析器测试
add_executable(setting_parser_test unit/setting_parser_test.cpp)
target_link_libraries(setting_parser_test PRIVATE maxcompute_client config common test_main)
add_test(NAME setting_parser_test COMMAND setting_parser_test)

# 编码 helper 测试: 直接编译 encoding.cpp 进入测试二进制, 避免对
# SHARED 目标 (odbc_api) 的符号导出依赖.
add_executable(encoding_test
    unit/encoding_test.cpp
    ${CMAKE_SOURCE_DIR}/src/odbc_api/encoding.cpp)
target_link_libraries(encoding_test PRIVATE common test_main ${ODBC_FRAMEWORKS})
add_test(NAME encoding_test COMMAND encoding_test)

# SQLGetData 分段读取契约测试: 直接编译 column_fetch.cpp 进测试二进制,
# 只依赖 sql.h 的类型, 不需要驱动 so 的符号导出, 也不需要网络连接.
add_executable(column_fetch_test
    unit/column_fetch_test.cpp
    ${CMAKE_SOURCE_DIR}/src/odbc_api/column_fetch.cpp)
target_link_libraries(column_fetch_test PRIVATE test_main ${ODBC_FRAMEWORKS})
add_test(NAME column_fetch_test COMMAND column_fetch_test)

# Handle-level contract: catches cursor transitions that pure segment tests cannot.
add_executable(statement_fetch_test
    unit/statement_fetch_test.cpp
    ${CMAKE_SOURCE_DIR}/src/odbc_api/handles.cpp
    ${CMAKE_SOURCE_DIR}/src/odbc_api/conversions.cpp
    ${CMAKE_SOURCE_DIR}/src/odbc_api/encoding.cpp
    ${CMAKE_SOURCE_DIR}/src/odbc_api/column_fetch.cpp)
target_link_libraries(statement_fetch_test PRIVATE maxcompute_client config common test_main ${ODBC_FRAMEWORKS})
add_test(NAME statement_fetch_test COMMAND statement_fetch_test)

# Suites that a testing build must produce. Removing or renaming one is a
# conscious change and belongs in the matching PR, not in a skipped test run.
set(MCO_EXPECTED_UNIT_TESTS
    config_test
    logging_test
    models_test
    setting_parser_test
    encoding_test
    column_fetch_test
    statement_fetch_test
)
foreach(mco_expected_unit_test IN LISTS MCO_EXPECTED_UNIT_TESTS)
    if(NOT TARGET ${mco_expected_unit_test})
        message(FATAL_ERROR
            "MCO-BUILD-GATE: BUILD_TESTING=ON but test target "
            "'${mco_expected_unit_test}' is missing from ${CMAKE_CURRENT_LIST_DIR}. "
            "Add the suite back, or move it out of MCO_EXPECTED_UNIT_TESTS in the same "
            "change that removes it.")
    endif()
endforeach()



# Second half of the gate: fails when no add_test() above survived to here.
mco_testing_end()
