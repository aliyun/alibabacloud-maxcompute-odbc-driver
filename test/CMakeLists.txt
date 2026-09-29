# test/CMakeLists.txt

# 启用测试
enable_testing()

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

# 查找 Google Test
find_package(GTest QUIET)

if(GTest_FOUND)
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

    message(STATUS "Google Test found. Building all tests.")
else()
    message(STATUS "Google Test not found. Skipping tests.")
endif()