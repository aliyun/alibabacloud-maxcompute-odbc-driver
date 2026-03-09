#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/common/utils.h"
#include "maxcompute_odbc/odbc_api/entry_points.h"
#include "maxcompute_odbc/odbc_api/handle_registry.h"
#include "maxcompute_odbc/odbc_api/handles.h"
#include <type_traits>  // For std::is_same_v

// Import string conversion utilities from maxcompute_odbc namespace
using maxcompute_odbc::OdbcStringToStdString;
using maxcompute_odbc::OdbcWStringToStdString;
using maxcompute_odbc::StdStringToOdbcWString;

// Template implementation for SQLGetInfo functions (C++ linkage only)
template <typename CharType>
SQLRETURN GetInfoImpl(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                      SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                      SQLSMALLINT *StringLengthPtr) {
  static_assert(
      std::is_same_v<CharType, char> || std::is_same_v<CharType, wchar_t>,
      "CharType must be either char or wchar_t");

  try {
    if constexpr (std::is_same_v<CharType, char>) {
      MCO_LOG_DEBUG("SQLGetInfo called with InfoType: {}", InfoType);
    } else {
      MCO_LOG_DEBUG("SQLGetInfoW called with InfoType: {}", InfoType);
    }

    // 获取连接句柄
    auto *pConn = maxcompute_odbc::HandleRegistry::instance()
                      .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
    if (!pConn) {
      if constexpr (std::is_same_v<CharType, char>) {
        MCO_LOG_ERROR("SQLGetInfo: Invalid connection handle received");
      } else {
        MCO_LOG_ERROR("SQLGetInfoW: Invalid connection handle received");
      }
      return SQL_INVALID_HANDLE;
    }

    // 确保InfoValuePtr有效
    if (!InfoValuePtr && StringLengthPtr) {
      if constexpr (std::is_same_v<CharType, char>) {
        MCO_LOG_ERROR(
            "SQLGetInfo: InfoValuePtr is null but StringLengthPtr is provided");
      } else {
        MCO_LOG_ERROR(
            "SQLGetInfoW: InfoValuePtr is null but StringLengthPtr is "
            "provided");
      }
      pConn->addDiagRecord({0, "HY009", "Invalid use of null pointer"});
      return SQL_ERROR;
    }

    // 根据InfoType处理不同类型的信息
    SQLRETURN result = SQL_SUCCESS;
    SQLSMALLINT info_length = 0;

    // Create appropriate string literals based on CharType
    auto get_string_literal = [](const char *ansi_str) -> auto {
      if constexpr (std::is_same_v<CharType, char>) {
        return ansi_str;
      } else {
        if (strcmp(ansi_str, "maxcompute_odbc.dll") == 0)
          return L"maxcompute_odbc.dll";
        else if (strcmp(ansi_str, "MaxCompute") == 0)
          return L"MaxCompute";
        else if (strcmp(ansi_str, "1.0.0") == 0)
          return L"1.0.0";
        else if (strcmp(ansi_str, "1.0") == 0)
          return L"1.0";
        else if (strcmp(ansi_str, "03.80") == 0)
          return L"03.80";
        else if (strcmp(ansi_str, "`") == 0)
          return L"`";
        else if (strcmp(ansi_str, "No Project Connected") == 0)
          return L"No Project Connected";
        else
          return L"";
      }
    };

    switch (InfoType) {
      case SQL_DRIVER_NAME: {
        // 驱动程序名称
        const CharType *driver_name = get_string_literal("maxcompute_odbc.dll");
        size_t len = std::char_traits<CharType>::length(driver_name);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, driver_name, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = driver_name[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_DRIVER_VER: {
        // 驱动程序版本 (这里用示例版本号)
        const CharType *driver_ver = get_string_literal("1.0.0");
        size_t len = std::char_traits<CharType>::length(driver_ver);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, driver_ver, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = driver_ver[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_DATA_SOURCE_NAME: {
        // 数据源名称
        const CharType *dsn_name =
            get_string_literal("MaxCompute");  // 使用一个默认的显示名
        size_t len = std::char_traits<CharType>::length(dsn_name);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, dsn_name, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = dsn_name[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_DATABASE_NAME: {
        // 当前数据库/项目名称
        auto &config = pConn->getConfigForUpdate();
        std::string proj = config.project;
        if (proj.empty()) {
          proj = "No Project Connected";
        }

        if constexpr (std::is_same_v<CharType, char>) {
          // ANSI string handling
          const char *db_name = proj.c_str();
          size_t len = strlen(db_name);
          if (InfoValuePtr && BufferLength > 0) {
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, db_name, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
          info_length = static_cast<SQLSMALLINT>(len);
        } else {
          // Wide string handling - use proper UTF-8 to UTF-16 conversion
          SQLSMALLINT actual_length = 0;
          if (InfoValuePtr && BufferLength > 0) {
            StdStringToOdbcWString(proj, static_cast<SQLWCHAR *>(InfoValuePtr),
                                   BufferLength, &actual_length);
            // Check for truncation
            StdStringToOdbcWString(proj, nullptr, 0, &actual_length);
            if (static_cast<size_t>(actual_length) >
                static_cast<size_t>(BufferLength)) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            StdStringToOdbcWString(proj, nullptr, 0, &actual_length);
          }
          info_length = actual_length;
        }
        break;
      }
      case SQL_DBMS_NAME: {
        // 数据库管理系统名称
        const CharType *dbms_name = get_string_literal("MaxCompute");
        size_t len = std::char_traits<CharType>::length(dbms_name);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, dbms_name, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = dbms_name[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_DBMS_VER: {
        // 数据库管理系统版本
        const CharType *dbms_ver = get_string_literal("1.0");
        size_t len = std::char_traits<CharType>::length(dbms_ver);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, dbms_ver, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = dbms_ver[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_DRIVER_ODBC_VER: {
        // ODBC版本 (返回3.80)
        const CharType *odbc_ver = get_string_literal("03.80");
        size_t len = std::char_traits<CharType>::length(odbc_ver);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, odbc_ver, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = odbc_ver[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_ODBC_VER: {
        // ODBC规范版本
        const CharType *odbc_ver = get_string_literal("03.80");
        size_t len = std::char_traits<CharType>::length(odbc_ver);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, odbc_ver, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = odbc_ver[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_IDENTIFIER_QUOTE_CHAR: {
        // 标识符引用字符
        const CharType *quote_char = get_string_literal("`");
        size_t len = std::char_traits<CharType>::length(quote_char);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, quote_char, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = quote_char[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_PROCEDURES: {
        // 是否支持存储过程 (我们这里返回不支持)
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_FALSE;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_EXPRESSIONS_IN_ORDERBY: {
        // 是否支持在ORDER BY中使用表达式
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_TRUE;  // 假设支持
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_MAX_TABLE_NAME_LEN: {
        // 最大表名称长度
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = 30;  // 假设长度
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_ACTIVE_CONNECTIONS: {
        // 最大活动连接数
        if (InfoValuePtr) {
          *(SQLSMALLINT *)InfoValuePtr =
              0;  // 对 MaxCompute 没有实际意义，返回0
        }
        info_length = sizeof(SQLSMALLINT);
        break;
      }
      case SQL_ACTIVE_STATEMENTS: {
        // 最大活动语句数
        if (InfoValuePtr) {
          *(SQLSMALLINT *)InfoValuePtr =
              0;  // 对 MaxCompute 没有实际意义，返回0
        }
        info_length = sizeof(SQLSMALLINT);
        break;
      }
      case SQL_CONVERT_FUNCTIONS: {
        // 转换函数支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_FN_CVT_CAST;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_STRING_FUNCTIONS: {
        // 字符串函数支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr =
              SQL_FN_STR_ASCII | SQL_FN_STR_CONCAT | SQL_FN_STR_LENGTH |
              SQL_FN_STR_SUBSTRING | SQL_FN_STR_LTRIM | SQL_FN_STR_RTRIM;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_KEYWORDS: {
        // 关键字列表
        const CharType *keywords = get_string_literal(
            "SELECT,INSERT,UPDATE,DELETE,CREATE,DROP,ALTER,INTO,WHERE,ORDER,BY,"
            "GROUP,HAVING,UNION,ALL,DISTINCT,LIMIT,OFFSET,AS,AND,OR,NOT,IN,"
            "EXISTS,BETWEEN,LIKE,IS,NULL,TRUE,FALSE,COUNT,SUM,AVG,MIN,MAX");
        size_t len = std::char_traits<CharType>::length(keywords);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, keywords, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = keywords[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_ODBC_INTERFACE_CONFORMANCE: {
        // ODBC接口符合性级别
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_OIC_CORE;  // Core level
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_SQL_CONFORMANCE: {
        // SQL符合性级别
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_SC_SQL92_ENTRY;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_ASYNC_MODE: {
        // 异步模式支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr =
              SQL_AM_STATEMENT;  // 只支持语句级别异步
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_ASYNC_DBC_FUNCTIONS: {
        // 异步连接函数支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_ASYNC_DBC_NOT_CAPABLE;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_BATCH_SUPPORT: {
        // 批处理支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = 0;  // MaxCompute不支持传统的批处理
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS: {
        // 最大并发异步语句数
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = 16;  // 适配MaxCompute服务端限制
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_ORDER_BY_COLUMNS_IN_SELECT: {
        // ORDER BY列必须在SELECT中的要求
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_FALSE;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_CATALOG_NAME: {
        // 是否支持目录
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr =
              SQL_FALSE;  // MaxCompute使用project类似概念
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_CATALOG_TERM: {
        // 目录术语
        const CharType *catalog_term = get_string_literal("project");
        size_t len = std::char_traits<CharType>::length(catalog_term);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, catalog_term, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = catalog_term[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_CONVERT_GUID: {
        // GUID转换支持
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_FALSE;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_IDENTIFIER_CASE: {
        // 标识符大小写敏感性
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr =
              SQL_IC_SENSITIVE;  // MaxCompute区分大小写
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_MAX_SCHEMA_NAME_LEN: {
        // 最大模式(schema)名称长度
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = 128;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_MAX_CATALOG_NAME_LEN: {
        // 最大目录(catalog)名称长度
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = 128;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_OUTER_JOINS: {
        // 是否支持外连接
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr =
              SQL_OJ_INNER | SQL_OJ_LEFT | SQL_OJ_RIGHT;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_GETDATA_EXTENSIONS: {
        // GetData扩展
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_NULL_COLLATION: {
        // NULL排序行为
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_NC_START;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_COLUMN_ALIAS: {
        // 列别名支持
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_TRUE;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_GROUP_BY: {
        // GROUP BY支持
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_GB_GROUP_BY_EQUALS_SELECT;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_UNION: {
        // UNION支持
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_U_UNION | SQL_U_UNION_ALL;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_SQL92_PREDICATES: {
        // SQL-92 谓词支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_SP_BETWEEN | SQL_SP_COMPARISON |
                                         SQL_SP_EXISTS | SQL_SP_IN |
                                         SQL_SP_ISNULL | SQL_SP_LIKE;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_SQL92_VALUE_EXPRESSIONS: {
        // SQL-92值表达式支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr =
              SQL_SVE_CASE | SQL_SVE_CAST | SQL_SVE_COALESCE | SQL_SVE_NULLIF;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_SQL92_STRING_FUNCTIONS: {
        // SQL-92字符串函数支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_SSF_CONVERT | SQL_SSF_LOWER |
                                         SQL_SSF_UPPER | SQL_SSF_SUBSTRING;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS: {
        // SQL-92数值函数支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr =
              SQL_SNVF_BIT_LENGTH | SQL_SNVF_CHAR_LENGTH | SQL_SNVF_EXTRACT;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_SQL92_DATETIME_FUNCTIONS: {
        // SQL-92日期时间函数支持
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_SDF_CURRENT_DATE |
                                         SQL_SDF_CURRENT_TIME |
                                         SQL_SDF_CURRENT_TIMESTAMP;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_TXN_ISOLATION_OPTION: {
        // 事务隔离选项 - MaxCompute不支持事务
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = 0;
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_DEFAULT_TXN_ISOLATION: {
        // 默认事务隔离级别 - MaxCompute不支持事务
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = SQL_TXN_READ_COMMITTED;  // 默认值
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_DATA_SOURCE_READ_ONLY: {
        // 数据源是否只读
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_FALSE;  // MaxCompute支持读写操作
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_CURSOR_COMMIT_BEHAVIOR: {
        // 提交后的光标行为 - MaxCompute不支持事务
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_CB_PRESERVE;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_CURSOR_ROLLBACK_BEHAVIOR: {
        // 回滚后的光标行为 - MaxCompute不支持事务
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_CB_PRESERVE;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_CONCAT_NULL_BEHAVIOR: {
        // NULL连接行为
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = SQL_CB_NULL;
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_MAX_ROW_SIZE: {
        // 最大行大小
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = 0;  // 无限制
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      case SQL_MAX_TABLES_IN_SELECT: {
        // SELECT语句中的最大表数
        if (InfoValuePtr) {
          *(SQLUSMALLINT *)InfoValuePtr = 16;  // MaxCompute支持连接多个表
        }
        info_length = sizeof(SQLUSMALLINT);
        break;
      }
      case SQL_PROCEDURE_TERM: {
        // 存储过程术语 (MaxCompute不支持存储过程)
        const CharType *proc_term = get_string_literal("");
        size_t len = std::char_traits<CharType>::length(proc_term);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, proc_term, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = proc_term[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_SCHEMA_TERM: {
        // 模式术语
        const CharType *schema_term = get_string_literal("schema");
        size_t len = std::char_traits<CharType>::length(schema_term);
        if (InfoValuePtr && BufferLength > 0) {
          if constexpr (std::is_same_v<CharType, char>) {
            // ANSI string handling
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), len);
            memcpy(InfoValuePtr, schema_term, copy_len);
            static_cast<char *>(InfoValuePtr)[copy_len] =
                '\0';  // null terminate
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          } else {
            // Wide string handling
            size_t max_wchars = BufferLength / sizeof(SQLWCHAR) -
                                1;  // leave space for null terminator
            size_t copy_len = std::min(max_wchars, len);
            for (size_t i = 0; i < copy_len; ++i) {
              static_cast<SQLWCHAR *>(InfoValuePtr)[i] = schema_term[i];
            }
            static_cast<SQLWCHAR *>(InfoValuePtr)[copy_len] = L'\0';
            if (copy_len < len) {
              result = SQL_SUCCESS_WITH_INFO;
              pConn->addDiagRecord(
                  {0, "01004", "String data, right truncated."});
            }
          }
        }
        info_length = static_cast<SQLSMALLINT>(len * sizeof(CharType));
        break;
      }
      case SQL_CATALOG_USAGE: {
        // 目录用法
        if (InfoValuePtr) {
          *(SQLUINTEGER *)InfoValuePtr = 0;  // 不支持标准目录功能
        }
        info_length = sizeof(SQLUINTEGER);
        break;
      }
      default:
        MCO_LOG_DEBUG("SQLGetInfo: Unsupported InfoType: {}", InfoType);
        return SQL_SUCCESS;
    }

    // 设置字符串长度
    if (StringLengthPtr) {
      *StringLengthPtr = info_length;
    }

    if constexpr (std::is_same_v<CharType, char>) {
      MCO_LOG_DEBUG("SQLGetInfo returning: {}", result == SQL_SUCCESS
                                                    ? "SQL_SUCCESS"
                                                    : "SQL_SUCCESS_WITH_INFO");
    } else {
      MCO_LOG_DEBUG("SQLGetInfoW returning: {}", result == SQL_SUCCESS
                                                     ? "SQL_SUCCESS"
                                                     : "SQL_SUCCESS_WITH_INFO");
    }
    return result;
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLGetInfo function failed with exception: {}", e.what());
    if (ConnectionHandle != SQL_NULL_HANDLE) {
      auto *pConn = maxcompute_odbc::HandleRegistry::instance()
                        .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
      if (pConn) {
        pConn->addDiagRecord(
            {0, "HY000", std::string("General error: ") + e.what()});
      }
    }
    return SQL_ERROR;
  }
}

// Forward declaration of the common implementation function template (before
// extern "C")
template <typename CharType>
SQLRETURN GetInfoImpl(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                      SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                      SQLSMALLINT *StringLengthPtr);

extern "C" {

// SQLAllocEnv is an old ODBC 2.x function for allocating environment handles
// It's equivalent to SQLAllocHandle(SQL_HANDLE_ENV, ...)
SQLRETURN SQL_API SQLAllocEnv(SQLHENV *EnvironmentHandle) {
  MCO_LOG_DEBUG("SQLAllocEnv called with environment handle ptr: {}",
                reinterpret_cast<void *>(EnvironmentHandle));

  if (!EnvironmentHandle) {
    return SQL_INVALID_HANDLE;
  }
  *EnvironmentHandle = SQL_NULL_HANDLE;

  try {
    // Allocate an environment handle using the same logic as SQLAllocHandle
    auto env = std::make_unique<maxcompute_odbc::EnvHandle>();
    *EnvironmentHandle =
        maxcompute_odbc::HandleRegistry::instance().allocate(std::move(env));
    return SQL_SUCCESS;
  } catch (...) {
    return SQL_ERROR;
  }
}

// SQLAllocConnect is an old ODBC 2.x function for allocating connection handles
SQLRETURN SQL_API SQLAllocConnect(SQLHENV EnvironmentHandle,
                                  SQLHDBC *ConnectionHandle) {
  MCO_LOG_DEBUG(
      "SQLAllocConnect called with env handle: {}, conn handle ptr: {}",
      reinterpret_cast<void *>(EnvironmentHandle),
      reinterpret_cast<void *>(ConnectionHandle));

  if (!ConnectionHandle) {
    return SQL_INVALID_HANDLE;
  }
  *ConnectionHandle = SQL_NULL_HANDLE;

  try {
    auto *env = maxcompute_odbc::HandleRegistry::instance()
                    .get<maxcompute_odbc::EnvHandle>(EnvironmentHandle);
    if (!env) return SQL_INVALID_HANDLE;

    auto conn = std::make_unique<maxcompute_odbc::ConnHandle>(env);
    *ConnectionHandle =
        maxcompute_odbc::HandleRegistry::instance().allocate(std::move(conn));
    return SQL_SUCCESS;
  } catch (...) {
    return SQL_ERROR;
  }
}

// SQLAllocStmt is an old ODBC 2.x function for allocating statement handles
SQLRETURN SQL_API SQLAllocStmt(SQLHDBC ConnectionHandle,
                               SQLHSTMT *StatementHandle) {
  MCO_LOG_DEBUG("SQLAllocStmt called with conn handle: {}, stmt handle ptr: {}",
                reinterpret_cast<void *>(ConnectionHandle),
                reinterpret_cast<void *>(StatementHandle));

  if (!StatementHandle) {
    return SQL_INVALID_HANDLE;
  }
  *StatementHandle = SQL_NULL_HANDLE;

  try {
    auto *conn = maxcompute_odbc::HandleRegistry::instance()
                     .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
    if (!conn) return SQL_INVALID_HANDLE;

    auto stmt = std::make_unique<maxcompute_odbc::StmtHandle>(conn);
    *StatementHandle =
        maxcompute_odbc::HandleRegistry::instance().allocate(std::move(stmt));
    return SQL_SUCCESS;
  } catch (...) {
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle,
                                 SQLHANDLE *OutputHandle) {
  MCO_LOG_DEBUG(
      "SQLAllocHandle called with handle type: {}, input handle: {}, output "
      "handle ptr: {}",
      HandleType, reinterpret_cast<void *>(InputHandle),
      reinterpret_cast<void *>(OutputHandle));

  if (!OutputHandle) {
    return SQL_INVALID_HANDLE;
  }
  *OutputHandle = SQL_NULL_HANDLE;

  try {
    if (HandleType == SQL_HANDLE_ENV) {
      if (InputHandle != SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;
      auto env = std::make_unique<maxcompute_odbc::EnvHandle>();
      *OutputHandle =
          maxcompute_odbc::HandleRegistry::instance().allocate(std::move(env));

      return SQL_SUCCESS;
    }
    if (HandleType == SQL_HANDLE_DBC) {
      if (!InputHandle) return SQL_INVALID_HANDLE;
      auto *env = maxcompute_odbc::HandleRegistry::instance()
                      .get<maxcompute_odbc::EnvHandle>(InputHandle);
      if (!env) return SQL_INVALID_HANDLE;
      auto conn = std::make_unique<maxcompute_odbc::ConnHandle>(env);
      *OutputHandle =
          maxcompute_odbc::HandleRegistry::instance().allocate(std::move(conn));
      return SQL_SUCCESS;
    }
    if (HandleType == SQL_HANDLE_STMT) {
      if (!InputHandle) return SQL_INVALID_HANDLE;
      auto *conn = maxcompute_odbc::HandleRegistry::instance()
                       .get<maxcompute_odbc::ConnHandle>(InputHandle);
      if (!conn) return SQL_INVALID_HANDLE;
      auto stmt = std::make_unique<maxcompute_odbc::StmtHandle>(conn);
      *OutputHandle =
          maxcompute_odbc::HandleRegistry::instance().allocate(std::move(stmt));
      return SQL_SUCCESS;
    }
    return SQL_ERROR;
  } catch (...) {
    return SQL_ERROR;
  }
}

// --- 释放句柄 ---
// SQLFreeEnv is an old ODBC 2.x function, alias for
// SQLFreeHandle(SQL_HANDLE_ENV, ...)
SQLRETURN SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle) {
  if (EnvironmentHandle == SQL_NULL_HANDLE) {
    return SQL_INVALID_HANDLE;
  }

  try {
    maxcompute_odbc::HandleRegistry::instance().free(EnvironmentHandle);
    return SQL_SUCCESS;
  } catch (...) {
    return SQL_ERROR;
  }
}

// SQLFreeConnect is an old ODBC 2.x function, alias for
// SQLFreeHandle(SQL_HANDLE_DBC, ...)
SQLRETURN SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle) {
  if (ConnectionHandle == SQL_NULL_HANDLE) {
    return SQL_INVALID_HANDLE;
  }

  try {
    maxcompute_odbc::HandleRegistry::instance().free(ConnectionHandle);
    return SQL_SUCCESS;
  } catch (...) {
    return SQL_ERROR;
  }
}

// SQLFreeStmt is an old ODBC 2.x function, alias for
// SQLFreeHandle(SQL_HANDLE_STMT, ...)
SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option) {
  if (StatementHandle == SQL_NULL_HANDLE) {
    return SQL_INVALID_HANDLE;
  }

  // Note: Option parameter (SQL_CLOSE, SQL_DROP, SQL_UNBIND, SQL_RESET_PARAMS)
  // is used to control what to do with the statement
  try {
    if (Option == SQL_DROP) {
      // SQL_DROP: Free the statement handle
      maxcompute_odbc::HandleRegistry::instance().free(StatementHandle);
    }
    // Other options (SQL_CLOSE, SQL_UNBIND, SQL_RESET_PARAMS) are ignored
    // as we don't maintain cursor state or bound parameters in this simple
    // implementation
    return SQL_SUCCESS;
  } catch (...) {
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle) {
  if (Handle == SQL_NULL_HANDLE) {
    return SQL_INVALID_HANDLE;
  }

  try {
    if (HandleType == SQL_HANDLE_ENV || HandleType == SQL_HANDLE_DBC ||
        HandleType == SQL_HANDLE_STMT) {
      // 直接从注册表移除，自动析构
      maxcompute_odbc::HandleRegistry::instance().free(Handle);
      return SQL_SUCCESS;
    }
    return SQL_ERROR;
  } catch (...) {
    return SQL_ERROR;
  }
}

// --- 连接 ---
// Note: String conversion utilities are now in maxcompute_odbc namespace
// in utils.h: OdbcStringToStdString, OdbcWStringToStdString,
// StdStringToOdbcWString

// --- 连接 ---
// SQLConnect is an old ODBC 2.x function for establishing a connection
SQLRETURN SQL_API SQLConnect(SQLHDBC ConnectionHandle, SQLCHAR *ServerName,
                             SQLSMALLINT NameLength1, SQLCHAR *UserName,
                             SQLSMALLINT NameLength2, SQLCHAR *Authentication,
                             SQLSMALLINT NameLength3) {
  auto *conn = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
  if (!conn) return SQL_INVALID_HANDLE;
  return conn->connect(OdbcStringToStdString(ServerName, NameLength1),
                       OdbcStringToStdString(UserName, NameLength2),
                       OdbcStringToStdString(Authentication, NameLength3));
}

// SQLDisconnect is an old ODBC 2.x function for disconnecting from a data
// source
SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle) {
  MCO_LOG_DEBUG("SQLDisconnect called with connection handle: {}",
                reinterpret_cast<void *>(ConnectionHandle));

  auto *conn = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
  if (!conn) {
    MCO_LOG_ERROR("SQLDisconnect: Invalid connection handle received");
    return SQL_INVALID_HANDLE;
  }

  // Check if there's an active SDK connection
  if (!conn->getSDK()) {
    MCO_LOG_DEBUG(
        "SQLDisconnect: Connection already closed, returning SQL_SUCCESS");
    return SQL_SUCCESS;
  }

  // Before disconnecting, ensure we report a compatible transaction state
  // to prevent "无效的事务状态" error from Microsoft ODBC Driver Manager
  try {
    // Report that we're in autocommit mode (which is always true for
    // MaxCompute) This satisfies ODBC Driver Manager's transaction state
    // validation
    MCO_LOG_DEBUG(
        "SQLDisconnect: Verifying transaction state before disconnect");

    // Call the actual disconnect method
    SQLRETURN result = conn->disconnect();

    if (result == SQL_SUCCESS) {
      MCO_LOG_DEBUG(
          "SQLDisconnect: Transaction state compatible, disconnection "
          "successful");
    } else {
      MCO_LOG_ERROR("SQLDisconnect: Disconnection failed with error");
    }

    return result;
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLDisconnect failed with exception: {}", e.what());
    conn->addDiagRecord(
        {0, "HY000", std::string("General error: ") + e.what()});
    return SQL_ERROR;
  }
}

// --- 执行 ---
// SQLExecDirect is an old ODBC 2.x function for executing a statement without
// preparation
SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
                                SQLCHAR *StatementText, SQLINTEGER TextLength) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  std::string sqlText = OdbcStringToStdString(StatementText, TextLength);
  MCO_LOG_INFO("SQLExecDirect: Executing query: '{}'", sqlText);

  return stmt->executeDirect(sqlText);
}
// SQLExecDirectW is the wide character version of SQLExecDirect
SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT StatementHandle,
                                 SQLWCHAR *StatementText,
                                 SQLINTEGER TextLength) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  // Handle empty query - return success immediately
  if (TextLength == 0 || (TextLength == SQL_NTS && (StatementText == nullptr ||
                                                    StatementText[0] == 0))) {
    return SQL_SUCCESS;
  }

  return stmt->executeDirect(OdbcWStringToStdString(StatementText, TextLength));
}
}

// SQLPrepare is an old ODBC 2.x function for preparing a SQL statement
SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR *StatementText,
                             SQLINTEGER TextLength) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  std::string sqlText = OdbcStringToStdString(StatementText, TextLength);
  MCO_LOG_INFO("SQLPrepare: Preparing query: '{}'", sqlText);

  return stmt->prepare(sqlText);
}

// SQLPrepareW is the wide character version of SQLPrepare
SQLRETURN SQL_API SQLPrepareW(SQLHSTMT StatementHandle, SQLWCHAR *StatementText,
                              SQLINTEGER TextLength) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  std::string sqlText = OdbcWStringToStdString(StatementText, TextLength);
  MCO_LOG_INFO("SQLPrepareW: Preparing query: '{}'", sqlText);

  return stmt->prepare(sqlText);
}

// SQLExecute is an old ODBC 2.x function for executing a prepared statement
SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  MCO_LOG_INFO("SQLExecute: Executing prepared statement");
  return stmt->execute();
}

// --- 获取结果 ---
// SQLFetch is an old ODBC 2.x function for fetching the next row of a result
// set
SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->fetch();
}

// --- 绑定列 ---
// SQLBindCol is an old ODBC 2.x function for binding a column in the result set
// to a variable
SQLRETURN SQL_API SQLBindCol(SQLHSTMT StatementHandle,
                             SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                             SQLPOINTER TargetValue, SQLLEN BufferLength,
                             SQLLEN *StrLen_or_Ind) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->bindCol(ColumnNumber, TargetType, TargetValue, BufferLength,
                       StrLen_or_Ind);
}

// --- 获取元数据 ---
// SQLNumResultCols is an old ODBC 2.x function for getting the number of
// columns in the result set
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT StatementHandle,
                                   SQLSMALLINT *ColumnCount) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->getNumResultCols(ColumnCount);
}

// SQLDescribeCol is an old ODBC 2.x function for describing a column in the
// result set
SQLRETURN SQL_API SQLDescribeCol(
    SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
    SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr,
    SQLSMALLINT *DataTypePtr, SQLULEN *ColumnSizePtr,
    SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->describeCol(ColumnNumber, ColumnName, BufferLength,
                           NameLengthPtr, DataTypePtr, ColumnSizePtr,
                           DecimalDigitsPtr, NullablePtr);
}

// SQLDescribeColW is the wide character version of SQLDescribeCol
SQLRETURN SQL_API SQLDescribeColW(
    SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLWCHAR *ColumnName,
    SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr,
    SQLSMALLINT *DataTypePtr, SQLULEN *ColumnSizePtr,
    SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  // First, get column info with a temporary buffer
  std::vector<char> temp_buf(256);
  SQLSMALLINT name_len = 0;
  SQLRETURN ret = stmt->describeCol(
      ColumnNumber, reinterpret_cast<SQLCHAR *>(temp_buf.data()),
      static_cast<SQLSMALLINT>(temp_buf.size()), &name_len, DataTypePtr,
      ColumnSizePtr, DecimalDigitsPtr, NullablePtr);

  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    return ret;
  }

  // Convert column name to UTF-16 if buffer is provided
  if (ColumnName && BufferLength > 0) {
    std::string col_name(temp_buf.data(), name_len);
    size_t max_chars = static_cast<size_t>(BufferLength) / sizeof(SQLWCHAR) - 1;
    uint16_t *dest = reinterpret_cast<uint16_t *>(ColumnName);
    size_t out_pos = 0;
    const char *src = col_name.c_str();
    size_t src_len = col_name.length();

    for (size_t i = 0; i < src_len && out_pos < max_chars; ++i) {
      unsigned char c = static_cast<unsigned char>(src[i]);
      uint32_t codepoint;

      if (c < 0x80) {
        codepoint = c;
      } else if ((c & 0xE0) == 0xC0) {
        if (i + 1 >= src_len) break;
        codepoint =
            ((c & 0x1F) << 6) | (static_cast<unsigned char>(src[i + 1]) & 0x3F);
        i += 1;
      } else if ((c & 0xF0) == 0xE0) {
        if (i + 2 >= src_len) break;
        codepoint = ((c & 0x0F) << 12) |
                    ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 6) |
                    (static_cast<unsigned char>(src[i + 2]) & 0x3F);
        i += 2;
      } else if ((c & 0xF8) == 0xF0) {
        if (i + 3 >= src_len || out_pos + 1 >= max_chars) break;
        codepoint = ((c & 0x07) << 18) |
                    ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 12) |
                    ((static_cast<unsigned char>(src[i + 2]) & 0x3F) << 6) |
                    (static_cast<unsigned char>(src[i + 3]) & 0x3F);
        i += 3;
        codepoint -= 0x10000;
        dest[out_pos++] = static_cast<SQLWCHAR>(0xD800 | (codepoint >> 10));
        dest[out_pos++] = static_cast<SQLWCHAR>(0xDC00 | (codepoint & 0x3FF));
        continue;
      } else {
        continue;
      }
      dest[out_pos++] = static_cast<SQLWCHAR>(codepoint);
    }
    dest[out_pos] = 0;

    if (NameLengthPtr) {
      // 返回字符数，而不是字节数
      *NameLengthPtr = static_cast<SQLSMALLINT>(out_pos);
    }
  } else if (NameLengthPtr) {
    // 返回字符数，而不是字节数
    *NameLengthPtr = name_len;
  }

  return ret;
}

// SQLRowCount is an old ODBC 2.x function for getting the number of rows in
// the result set
SQLRETURN SQL_API SQLRowCount(SQLHSTMT StatementHandle, SQLLEN *RowCount) {
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->getRowCount(RowCount);
}

// SQLColAttribute is an old ODBC 2.x function for getting information about a
// column attribute
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT StatementHandle,
                                  SQLUSMALLINT ColumnNumber,
                                  SQLUSMALLINT FieldIdentifier,
                                  SQLPOINTER CharacterAttributePtr,
                                  SQLSMALLINT BufferLength,
                                  SQLSMALLINT *StringLengthPtr,
                                  SQLLEN *NumericAttributePtr) {
  MCO_LOG_DEBUG(
      "SQLColAttribute (ANSI) called with statement handle: {}, column: {}, "
      "field: {}",
      reinterpret_cast<void *>(StatementHandle), ColumnNumber, FieldIdentifier);

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->getColAttribute(ColumnNumber, FieldIdentifier,
                               CharacterAttributePtr, BufferLength,
                               StringLengthPtr, NumericAttributePtr, false);
}

// SQLColAttributeW is the wide character version of SQLColAttribute
SQLRETURN SQL_API SQLColAttributeW(SQLHSTMT StatementHandle,
                                   SQLUSMALLINT ColumnNumber,
                                   SQLUSMALLINT FieldIdentifier,
                                   SQLPOINTER CharacterAttributePtr,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT *StringLengthPtr,
                                   SQLLEN *NumericAttributePtr) {
  MCO_LOG_DEBUG(
      "SQLColAttributeW called with statement handle: {}, column: {}, field: "
      "{}",
      reinterpret_cast<void *>(StatementHandle), ColumnNumber, FieldIdentifier);

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->getColAttribute(ColumnNumber, FieldIdentifier,
                               CharacterAttributePtr, BufferLength,
                               StringLengthPtr, NumericAttributePtr, true);
}

// SQLGetData is an old ODBC 2.x function for retrieving the value of a column
// in the current row
SQLRETURN SQL_API SQLGetData(SQLHSTMT StatementHandle,
                             SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                             SQLPOINTER TargetValue, SQLLEN BufferLength,
                             SQLLEN *StrLen_or_Ind) {
  MCO_LOG_DEBUG(
      "SQLGetData called with statement handle: {}, column: {}, target type: "
      "{}",
      reinterpret_cast<void *>(StatementHandle), ColumnNumber, TargetType);

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;
  return stmt->getData(ColumnNumber, TargetType, TargetValue, BufferLength,
                       StrLen_or_Ind);
}

// SQLDriverConnect is an old ODBC 2.x function for establishing a connection
SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC hDbc, SQLHWND hwndParent, SQLCHAR *szConnStrIn,
    SQLSMALLINT cbConnStrIn, SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax,
    SQLSMALLINT *pcbConnStrOut, SQLUSMALLINT fDriverCompletion) {
  MCO_LOG_DEBUG(
      "SQLDriverConnect called with connection handle: {}, connection string "
      "length: {}, completion mode: "
      "{}",
      reinterpret_cast<void *>(hDbc), cbConnStrIn, fDriverCompletion);

  // 1. 将 ODBC 句柄转换为我们内部的 C++ 对象指针
  auto *pConn = maxcompute_odbc::HandleRegistry::instance()
                    .get<maxcompute_odbc::ConnHandle>(hDbc);
  if (!pConn) {
    MCO_LOG_ERROR("Invalid connection handle in SQLDriverConnect");
    return SQL_INVALID_HANDLE;
  }

  // 2. 解析输入的连接字符串 (sensitive, so use DEBUG level only when safe)
  std::string connStrIn(
      reinterpret_cast<const char *>(szConnStrIn),
      cbConnStrIn == SQL_NTS
          ? strlen(reinterpret_cast<const char *>(szConnStrIn))
          : cbConnStrIn);

  try {
    auto &config = pConn->getConfigForUpdate();
    maxcompute_odbc::ConnectionStringParser::updateConfig(connStrIn, config);

    // Add fallback to environment variables if UID/PWD are not specified in
    // connection string
    if (!config.accessKeyId.has_value() || config.accessKeyId->empty()) {
      const char *accessKeyIdFromEnv =
          std::getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
      if (accessKeyIdFromEnv) {
        config.accessKeyId = std::string(accessKeyIdFromEnv);
      }
    }
    if (!config.accessKeySecret.has_value() ||
        config.accessKeySecret->empty()) {
      const char *accessKeySecretFromEnv =
          std::getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
      if (accessKeySecretFromEnv) {
        config.accessKeySecret = std::string(accessKeySecretFromEnv);
      }
    }

    // Add fallback to environment variables if SERVER/PROJECT are not specified
    // in connection string
    if (config.endpoint.empty()) {
      const char *endpointFromEnv = std::getenv("ALIBABA_CLOUD_ENDPOINT");
      if (endpointFromEnv) {
        config.endpoint = std::string(endpointFromEnv);
      }
    }
    if (config.project.empty()) {
      const char *projectFromEnv = std::getenv("ALIBABA_CLOUD_PROJECT");
      if (projectFromEnv) {
        config.project = std::string(projectFromEnv);
      }
    }

    // Add fallback to environment variables for proxy settings if not specified
    // in connection string
    bool isHttps = config.endpoint.find("https://") == 0;
    if (!config.httpProxy.has_value() && !config.httpsProxy.has_value()) {
      auto proxyEnv = maxcompute_odbc::getProxyFromEnvironment(isHttps);
      std::string proxyUrl = std::get<0>(proxyEnv);
      std::string proxyUser = std::get<1>(proxyEnv);
      std::string proxyPassword = std::get<2>(proxyEnv);

      if (!proxyUrl.empty()) {
        if (isHttps) {
          config.httpsProxy = proxyUrl;
        } else {
          config.httpProxy = proxyUrl;
        }

        // Set proxy credentials if found
        if (!proxyUser.empty() && !config.proxyUser.has_value()) {
          config.proxyUser = proxyUser;
        }
        if (!proxyPassword.empty() && !config.proxyPassword.has_value()) {
          config.proxyPassword = proxyPassword;
        }

        MCO_LOG_INFO("Using proxy from environment: {}",
                     isHttps ? "HTTPS_PROXY" : "HTTP_PROXY");
      }
    }

    // 4. 处理 fDriverCompletion (简化版：只支持 NOPROMPT)
    // 生产级的驱动需要在这里处理弹出对话框的逻辑
    if (fDriverCompletion != SQL_DRIVER_NOPROMPT) {
      MCO_LOG_WARNING(
          "SQLDriverConnect: Only SQL_DRIVER_NOPROMPT is supported in this "
          "version. Driver completion mode requested: {}",
          fDriverCompletion);
      // 也可以选择在这里弹出UI，如果实现了的话
    }

    // 5. 验证必要参数是否存在
    MCO_LOG_DEBUG(
        "Validating connection parameters: endpoint='{}', project='{}', "
        "credentials valid={}",
        config.endpoint, config.project, config.hasValidCredentials());
    if (config.endpoint.empty() || config.project.empty() ||
        !config.hasValidCredentials()) {
      std::string missing_params =
          "Connection string missing required attributes: ";
      bool needs_comma = false;
      if (config.endpoint.empty()) {
        missing_params += "SERVER";
        needs_comma = true;
      }
      if (config.project.empty()) {
        if (needs_comma) missing_params += ", ";
        missing_params += "PROJECT";
        needs_comma = true;
      }
      if (!config.hasValidCredentials()) {
        if (needs_comma) missing_params += ", ";
        missing_params += "UID/PWD";
      }

      MCO_LOG_ERROR("{}", missing_params);
      throw std::runtime_error(missing_params);
    }

    // Log configuration details for debugging (excluding sensitive information)
    if (config.accessKeyId) {
      MCO_LOG_DEBUG(
          "Access key ID: '{}****'",
          config.accessKeyId.value().substr(
              0, std::min<size_t>(4, config.accessKeyId.value().size())));
    }

    // 6. 【核心】创建 SDK 实例并建立连接
    MCO_LOG_DEBUG("Creating MaxComputeClient with provided configuration");
    pConn->setSDK(std::make_unique<maxcompute_odbc::MaxComputeClient>(config));
    MCO_LOG_INFO("Successfully connected to MaxCompute project '{}' at '{}'.",
                 config.project, config.endpoint);

    // 7. 构建并写回输出连接字符串 (不包含密码)
    if (szConnStrOut && cbConnStrOutMax > 0) {
      MCO_LOG_DEBUG("Building output connection string");
      std::string outStr =
          "DRIVER={MaxCompute ODBC Driver};SERVER=" + config.endpoint +
          ";PROJECT=" + config.project + ";UID=" + config.accessKeyId.value() +
          ";";

      size_t len_to_copy =
          outStr.length() < static_cast<size_t>(cbConnStrOutMax - 1)
              ? outStr.length()
              : static_cast<size_t>(cbConnStrOutMax - 1);
      memcpy(szConnStrOut, outStr.c_str(), len_to_copy);
      szConnStrOut[len_to_copy] = '\0';

      if (pcbConnStrOut) {
        *pcbConnStrOut = static_cast<SQLSMALLINT>(outStr.length());
        if (outStr.length() > len_to_copy) {
          // 如果缓冲区不够大，需要返回 SQL_SUCCESS_WITH_INFO
          MCO_LOG_WARNING(
              "Connection string buffer too small, connection string "
              "truncated.");
          pConn->addDiagRecord({0, "01004", "String data, right truncated."});
          return SQL_SUCCESS_WITH_INFO;
        }
      }
    }

    MCO_LOG_DEBUG("SQLDriverConnect completed successfully");
    return SQL_SUCCESS;

  } catch (const std::exception &e) {
    std::string error_msg = std::string("SQLDriverConnect failed: ") + e.what();

    // Add comprehensive diagnostics including connection details
    pConn->addDiagRecord(
        {0, "08001",
         error_msg});  // 08001: Client unable to establish connection

    MCO_LOG_ERROR("SQLDriverConnect failed: {}", e.what());
    MCO_LOG_ERROR("Connection attempt diagnostics:");
    MCO_LOG_ERROR("  Input DSN length: {}", connStrIn.length());
    MCO_LOG_ERROR("  Driver completion mode: {}", fDriverCompletion);
    MCO_LOG_ERROR("  Buffer sizes: input={}, output_buffer_max={}", cbConnStrIn,
                  cbConnStrOutMax);

    // Log additional details if config object is valid
    try {
      auto &config = pConn->getConfigForUpdate();
      MCO_LOG_ERROR("  Configuration state during error:");
      MCO_LOG_ERROR("    Endpoint: '{}'", config.endpoint);
      MCO_LOG_ERROR("    Project: '{}'", config.project);
      MCO_LOG_ERROR("    Access key ID present: {}",
                    config.accessKeyId.has_value());
      MCO_LOG_ERROR("    Access key secret present: {}",
                    config.accessKeySecret.has_value());
    } catch (...) {
      MCO_LOG_ERROR(
          "  Unable to access connection configuration for error diagnostics");
    }

    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLDriverConnectA(
    SQLHDBC hDbc, SQLHWND hwndParent, SQLCHAR *szConnStrIn,
    SQLSMALLINT cbConnStrIn, SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax,
    SQLSMALLINT *pcbConnStrOut, SQLUSMALLINT fDriverCompletion) {
  MCO_LOG_DEBUG(
      "SQLDriverConnectA called with connection handle: {}, connection string "
      "length: {}, completion "
      "mode: "
      "{}",
      reinterpret_cast<void *>(hDbc), cbConnStrIn, fDriverCompletion);

  // 1. 将 ODBC 句柄转换为我们内部的 C++ 对象指针
  auto *pConn = maxcompute_odbc::HandleRegistry::instance()
                    .get<maxcompute_odbc::ConnHandle>(hDbc);
  if (!pConn) {
    MCO_LOG_ERROR("Invalid connection handle in SQLDriverConnectA");
    return SQL_INVALID_HANDLE;
  }

  // 2. 解析输入的连接字符串 (sensitive, so use DEBUG level only when safe)
  std::string connStrIn = OdbcStringToStdString(szConnStrIn, cbConnStrIn);

  MCO_LOG_DEBUG("Input connection string: [REDACTED for security purposes]");

  try {
    // 3. 将解析的参数填充到 ConnHandle 的内部配置 m_config 中
    // (假设 Config 结构体有这些成员)
    auto &config =
        pConn->getConfigForUpdate();  // 假设有这么一个获取配置引用的方法

    maxcompute_odbc::ConnectionStringParser::updateConfig(connStrIn, config);

    // Add fallback to environment variables if UID/PWD are not specified in
    // connection string
    if (!config.accessKeyId.has_value() || config.accessKeyId->empty()) {
      const char *accessKeyIdFromEnv =
          std::getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
      if (accessKeyIdFromEnv) {
        config.accessKeyId = std::string(accessKeyIdFromEnv);
      }
    }
    if (!config.accessKeySecret.has_value() ||
        config.accessKeySecret->empty()) {
      const char *accessKeySecretFromEnv =
          std::getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
      if (accessKeySecretFromEnv) {
        config.accessKeySecret = std::string(accessKeySecretFromEnv);
      }
    }

    // Add fallback to environment variables if SERVER/PROJECT are not specified
    // in connection string
    if (config.endpoint.empty()) {
      const char *endpointFromEnv = std::getenv("ALIBABA_CLOUD_ENDPOINT");
      if (endpointFromEnv) {
        config.endpoint = std::string(endpointFromEnv);
      }
    }
    if (config.project.empty()) {
      const char *projectFromEnv = std::getenv("ALIBABA_CLOUD_PROJECT");
      if (projectFromEnv) {
        config.project = std::string(projectFromEnv);
      }
    }

    // Add fallback to environment variables for proxy settings if not specified
    // in connection string
    bool isHttps = config.endpoint.find("https://") == 0;
    if (!config.httpProxy.has_value() && !config.httpsProxy.has_value()) {
      auto proxyEnv = maxcompute_odbc::getProxyFromEnvironment(isHttps);
      std::string proxyUrl = std::get<0>(proxyEnv);
      std::string proxyUser = std::get<1>(proxyEnv);
      std::string proxyPassword = std::get<2>(proxyEnv);

      if (!proxyUrl.empty()) {
        if (isHttps) {
          config.httpsProxy = proxyUrl;
        } else {
          config.httpProxy = proxyUrl;
        }

        // Set proxy credentials if found
        if (!proxyUser.empty() && !config.proxyUser.has_value()) {
          config.proxyUser = proxyUser;
        }
        if (!proxyPassword.empty() && !config.proxyPassword.has_value()) {
          config.proxyPassword = proxyPassword;
        }

        MCO_LOG_DEBUG("Using proxy from environment: {}",
                      isHttps ? "HTTPS_PROXY" : "HTTP_PROXY");
      }
    }

    // 4. 处理 fDriverCompletion (简化版：只支持 NOPROMPT)
    // 生产级的驱动需要在这里处理弹出对话框的逻辑
    if (fDriverCompletion != SQL_DRIVER_NOPROMPT) {
      MCO_LOG_WARNING(
          "SQLDriverConnectA: Only SQL_DRIVER_NOPROMPT is supported in this "
          "version. Driver completion mode requested: {}",
          fDriverCompletion);
      // 也可以选择在这里弹出UI，如果实现了的话
    }

    // 5. 验证必要参数是否存在
    MCO_LOG_DEBUG(
        "Validating connection parameters: endpoint='{}', project='{}', "
        "credentials valid={}",
        config.endpoint, config.project, config.hasValidCredentials());
    if (config.endpoint.empty() || config.project.empty() ||
        !config.hasValidCredentials()) {
      std::string missing_params =
          "Connection string missing required attributes: ";
      bool needs_comma = false;
      if (config.endpoint.empty()) {
        missing_params += "SERVER";
        needs_comma = true;
      }
      if (config.project.empty()) {
        if (needs_comma) missing_params += ", ";
        missing_params += "PROJECT";
        needs_comma = true;
      }
      if (!config.hasValidCredentials()) {
        if (needs_comma) missing_params += ", ";
        missing_params += "UID/PWD";
      }

      MCO_LOG_ERROR("{}", missing_params);
      throw std::runtime_error(missing_params);
    }

    // Log configuration details for debugging (excluding sensitive information)
    MCO_LOG_INFO(
        "Configuration validation successful, attempting to connect to "
        "project: '{}' at: '{}', ",
        config.project, config.endpoint);
    if (config.accessKeyId) {
      MCO_LOG_DEBUG(
          "Access key ID: '{}****'",
          config.accessKeyId.value().substr(
              0, std::min<size_t>(4, config.accessKeyId.value().size())));
    }

    // 6. 【核心】创建 SDK 实例并建立连接
    MCO_LOG_DEBUG(
        "Creating MaxComputeClient with provided configuration (ANSI)");
    pConn->setSDK(std::make_unique<maxcompute_odbc::MaxComputeClient>(config));
    MCO_LOG_INFO("Successfully connected to MaxCompute project '{}' at '{}'.",
                 config.project, config.endpoint);

    // 7. 构建并写回输出连接字符串 (不包含密码)
    if (szConnStrOut && cbConnStrOutMax > 0) {
      MCO_LOG_DEBUG("Building output connection string (ANSI)");
      std::string outStr =
          "DRIVER={MaxCompute ODBC Driver};SERVER=" + config.endpoint +
          ";PROJECT=" + config.project + ";UID=" + config.accessKeyId.value() +
          ";";

      size_t len_to_copy =
          outStr.length() < static_cast<size_t>(cbConnStrOutMax - 1)
              ? outStr.length()
              : static_cast<size_t>(cbConnStrOutMax - 1);
      memcpy(szConnStrOut, outStr.c_str(), len_to_copy);
      szConnStrOut[len_to_copy] = '\0';

      if (pcbConnStrOut) {
        *pcbConnStrOut = static_cast<SQLSMALLINT>(outStr.length());
        if (outStr.length() > len_to_copy) {
          // 如果缓冲区不够大，需要返回 SQL_SUCCESS_WITH_INFO
          MCO_LOG_WARNING(
              "Connection string buffer too small, connection string "
              "truncated in SQLDriverConnectA.");
          pConn->addDiagRecord({0, "01004", "String data, right truncated."});
          return SQL_SUCCESS_WITH_INFO;
        }
      }
    }

    MCO_LOG_DEBUG("SQLDriverConnectA completed successfully");
    return SQL_SUCCESS;

  } catch (const std::exception &e) {
    std::string error_msg =
        std::string("SQLDriverConnectA failed: ") + e.what();

    // Add comprehensive diagnostics including connection details
    pConn->addDiagRecord(
        {0, "08001",
         error_msg});  // 08001: Client unable to establish connection

    MCO_LOG_ERROR("SQLDriverConnectA failed: {}", e.what());
    MCO_LOG_ERROR("Connection attempt diagnostics:");
    MCO_LOG_ERROR("  Input DSN length: {}", connStrIn.length());
    MCO_LOG_ERROR("  Driver completion mode: {}", fDriverCompletion);
    MCO_LOG_ERROR("  Buffer sizes: input={}, output_buffer_max={}", cbConnStrIn,
                  cbConnStrOutMax);

    // Log additional details if config object is valid
    try {
      auto &config = pConn->getConfigForUpdate();
      MCO_LOG_ERROR("  Configuration state during error:");
      MCO_LOG_ERROR("    Endpoint: '{}'", config.endpoint);
      MCO_LOG_ERROR("    Project: '{}'", config.project);
      MCO_LOG_ERROR("    Access key ID present: {}",
                    config.accessKeyId.has_value());
      MCO_LOG_ERROR("    Access key secret present: {}",
                    config.accessKeySecret.has_value());
    } catch (...) {
      MCO_LOG_ERROR(
          "  Unable to access connection configuration for error diagnostics");
    }

    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC hDbc, SQLHWND hwndParent,
                                    SQLWCHAR *szConnStrIn,
                                    SQLSMALLINT cbConnStrIn,
                                    SQLWCHAR *szConnStrOut,
                                    SQLSMALLINT cbConnStrOutMax,
                                    SQLSMALLINT *pcbConnStrOut,
                                    SQLUSMALLINT fDriverCompletion) {
  MCO_LOG_DEBUG(
      "SQLDriverConnectW called with connection handle: {}, connection string "
      "length: {}, completion "
      "mode: "
      "{}",
      reinterpret_cast<void *>(hDbc), cbConnStrIn, fDriverCompletion);

  // 1. 将 ODBC 句柄转换为我们内部的 C++ 对象指针
  auto *pConn = maxcompute_odbc::HandleRegistry::instance()
                    .get<maxcompute_odbc::ConnHandle>(hDbc);
  if (!pConn) {
    MCO_LOG_ERROR("Invalid connection handle in SQLDriverConnectW");
    return SQL_INVALID_HANDLE;
  }

  // 2. 解析输入的连接字符串 (sensitive, so use DEBUG level only when safe)
  MCO_LOG_DEBUG("SQLDriverConnectW: cbConnStrIn={}, szConnStrIn={}",
                cbConnStrIn, szConnStrIn ? "non-null" : "null");
  std::string connStrIn = OdbcWStringToStdString(szConnStrIn, cbConnStrIn);
  MCO_LOG_DEBUG("Parsed connection string ({} chars): {}", connStrIn.length(),
                connStrIn);
  auto &config =
      pConn->getConfigForUpdate();  // 假设有这么一个获取配置引用的方法

  try {
    MCO_LOG_INFO("Parsing driver connect string parameters (Wide)");
    maxcompute_odbc::ConnectionStringParser::updateConfig(connStrIn, config);

    // Add fallback to environment variables if UID/PWD are not specified in
    // connection string
    if (!config.accessKeyId.has_value() || config.accessKeyId->empty()) {
      const char *accessKeyIdFromEnv =
          std::getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
      if (accessKeyIdFromEnv) {
        config.accessKeyId = std::string(accessKeyIdFromEnv);
      }
    }
    if (!config.accessKeySecret.has_value() ||
        config.accessKeySecret->empty()) {
      const char *accessKeySecretFromEnv =
          std::getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
      if (accessKeySecretFromEnv) {
        config.accessKeySecret = std::string(accessKeySecretFromEnv);
      }
    }

    // Add fallback to environment variables if SERVER/PROJECT are not specified
    // in connection string
    if (config.endpoint.empty()) {
      const char *endpointFromEnv = std::getenv("ALIBABA_CLOUD_ENDPOINT");
      if (endpointFromEnv) {
        config.endpoint = std::string(endpointFromEnv);
      }
    }
    if (config.project.empty()) {
      const char *projectFromEnv = std::getenv("ALIBABA_CLOUD_PROJECT");
      if (projectFromEnv) {
        config.project = std::string(projectFromEnv);
      }
    }

    // Add fallback to environment variables for proxy settings if not specified
    // in connection string
    bool isHttps = config.endpoint.find("https://") == 0;
    if (!config.httpProxy.has_value() && !config.httpsProxy.has_value()) {
      auto proxyEnv = maxcompute_odbc::getProxyFromEnvironment(isHttps);
      std::string proxyUrl = std::get<0>(proxyEnv);
      std::string proxyUser = std::get<1>(proxyEnv);
      std::string proxyPassword = std::get<2>(proxyEnv);

      if (!proxyUrl.empty()) {
        if (isHttps) {
          config.httpsProxy = proxyUrl;
        } else {
          config.httpProxy = proxyUrl;
        }

        // Set proxy credentials if found
        if (!proxyUser.empty() && !config.proxyUser.has_value()) {
          config.proxyUser = proxyUser;
        }
        if (!proxyPassword.empty() && !config.proxyPassword.has_value()) {
          config.proxyPassword = proxyPassword;
        }

        MCO_LOG_INFO("Using proxy from environment: {}",
                     isHttps ? "HTTPS_PROXY" : "HTTP_PROXY");
      }
    }

    // 4. 处理 fDriverCompletion (简化版：只支持 NOPROMPT)
    // 生产级的驱动需要在这里处理弹出对话框的逻辑
    if (fDriverCompletion != SQL_DRIVER_NOPROMPT) {
      MCO_LOG_WARNING(
          "SQLDriverConnectW: Only SQL_DRIVER_NOPROMPT is supported in this "
          "version. Driver completion mode requested: {}",
          fDriverCompletion);
      // 也可以选择在这里弹出UI，如果实现了的话
    }

    // 5. 验证必要参数是否存在
    MCO_LOG_DEBUG(
        "Validating connection parameters: endpoint='{}', project='{}', "
        "credentials valid={}",
        config.endpoint, config.project, config.hasValidCredentials());
    if (config.endpoint.empty() || config.project.empty() ||
        !config.hasValidCredentials()) {
      std::string missing_params =
          "Connection string missing required attributes: ";
      bool needs_comma = false;
      if (config.endpoint.empty()) {
        missing_params += "SERVER";
        needs_comma = true;
      }
      if (config.project.empty()) {
        if (needs_comma) missing_params += ", ";
        missing_params += "PROJECT";
        needs_comma = true;
      }
      if (!config.hasValidCredentials()) {
        if (needs_comma) missing_params += ", ";
        missing_params += "UID/PWD";
      }

      MCO_LOG_ERROR("{}", missing_params);
      throw std::runtime_error(missing_params);
    }

    // Log configuration details for debugging (excluding sensitive information)
    MCO_LOG_INFO(
        "Configuration validation successful, attempting to connect to "
        "project: '{}' at: '{}', ",
        config.project, config.endpoint);
    if (config.accessKeyId) {
      MCO_LOG_DEBUG(
          "Access key ID: '{}****'",
          config.accessKeyId.value().substr(
              0, std::min<size_t>(4, config.accessKeyId.value().size())));
    }

    // 6. 【核心】创建 SDK 实例并建立连接
    MCO_LOG_DEBUG(
        "Creating MaxComputeClient with provided configuration (Wide)");
    pConn->setSDK(std::make_unique<maxcompute_odbc::MaxComputeClient>(config));
    MCO_LOG_INFO("Successfully connected to MaxCompute project '{}' at '{}'.",
                 config.project, config.endpoint);

    // 7. 构建并写回输出连接字符串 (不包含密码)
    if (szConnStrOut && cbConnStrOutMax > 0) {
      MCO_LOG_DEBUG("Building output connection string (Wide)");
      std::string outStr =
          "DRIVER={MaxCompute ODBC Driver};SERVER=" + config.endpoint +
          ";PROJECT=" + config.project + ";UID=" + config.accessKeyId.value() +
          ";";

      // Convert the output string to wide character format
      StdStringToOdbcWString(outStr, szConnStrOut, cbConnStrOutMax,
                             pcbConnStrOut);

      // Check if the output was truncated
      if (pcbConnStrOut && outStr.length() > (cbConnStrOutMax - 1)) {
        // 如果缓冲区不够大，需要返回 SQL_SUCCESS_WITH_INFO
        MCO_LOG_WARNING(
            "Connection string buffer too small, connection string "
            "truncated in SQLDriverConnectW.");
        pConn->addDiagRecord({0, "01004", "String data, right truncated."});
        return SQL_SUCCESS_WITH_INFO;
      }
    }

    MCO_LOG_DEBUG("SQLDriverConnectW completed successfully");
    return SQL_SUCCESS;

  } catch (const std::exception &e) {
    std::string error_msg =
        std::string("SQLDriverConnectW failed: ") + e.what();

    // Add comprehensive diagnostics including connection details
    pConn->addDiagRecord(
        {0, "08001",
         error_msg});  // 08001: Client unable to establish connection

    MCO_LOG_ERROR("SQLDriverConnectW failed: {}", e.what());
    MCO_LOG_ERROR("Connection attempt diagnostics:");
    MCO_LOG_ERROR("  Input DSN length: {}", connStrIn.length());
    MCO_LOG_ERROR("  Driver completion mode: {}", fDriverCompletion);
    MCO_LOG_ERROR("  Buffer sizes: input={}, output_buffer_max={}", cbConnStrIn,
                  cbConnStrOutMax);

    // Log additional details if config object is valid
    try {
      auto &config = pConn->getConfigForUpdate();
      MCO_LOG_ERROR("  Configuration state during error:");
      MCO_LOG_ERROR("    Endpoint: '{}'", config.endpoint);
      MCO_LOG_ERROR("    Project: '{}'", config.project);
      MCO_LOG_ERROR("    Access key ID present: {}",
                    config.accessKeyId.has_value());
      MCO_LOG_ERROR("    Access key secret present: {}",
                    config.accessKeySecret.has_value());
    } catch (...) {
      MCO_LOG_ERROR(
          "  Unable to access connection configuration for error diagnostics");
    }

    return SQL_ERROR;
  }
}

// SQLColumns is an old ODBC 2.x function for getting column information
SQLRETURN SQL_API SQLColumns(SQLHSTMT StatementHandle, SQLCHAR *CatalogName,
                             SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                             SQLSMALLINT NameLength2, SQLCHAR *TableName,
                             SQLSMALLINT NameLength3, SQLCHAR *ColumnName,
                             SQLSMALLINT NameLength4) {
  MCO_LOG_DEBUG("SQLColumns called with statement handle: {}",
                reinterpret_cast<void *>(StatementHandle));

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  std::string catalog = OdbcStringToStdString(CatalogName, NameLength1);
  std::string schema = OdbcStringToStdString(SchemaName, NameLength2);
  std::string table = OdbcStringToStdString(TableName, NameLength3);
  std::string column = OdbcStringToStdString(ColumnName, NameLength4);

  return stmt->columns(catalog, schema, table, column);
}

// SQLColumnsW is the wide character version of SQLColumns
SQLRETURN SQL_API SQLColumnsW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName,
                              SQLSMALLINT NameLength1, SQLWCHAR *SchemaName,
                              SQLSMALLINT NameLength2, SQLWCHAR *TableName,
                              SQLSMALLINT NameLength3, SQLWCHAR *ColumnName,
                              SQLSMALLINT NameLength4) {
  MCO_LOG_DEBUG("SQLColumnsW called with statement handle: {}",
                reinterpret_cast<void *>(StatementHandle));

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  std::string catalog = OdbcWStringToStdString(CatalogName, NameLength1);
  std::string schema = OdbcWStringToStdString(SchemaName, NameLength2);
  std::string table = OdbcWStringToStdString(TableName, NameLength3);
  std::string column = OdbcWStringToStdString(ColumnName, NameLength4);

  MCO_LOG_DEBUG(
      "Processing SQLColumnsW request - Catalog: '{}', Schema: '{}', Table: "
      "'{}', Column: '{}'",
      catalog, schema, table, column);

  return stmt->columns(catalog, schema, table, column);
}

// ANSI version of SQLTables
SQLRETURN SQL_API SQLTables(SQLHSTMT StatementHandle, SQLCHAR *CatalogName,
                            SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                            SQLSMALLINT NameLength2, SQLCHAR *TableName,
                            SQLSMALLINT NameLength3, SQLCHAR *TableType,
                            SQLSMALLINT NameLength4) {
  MCO_LOG_DEBUG("SQLTables called with statement handle: {}",
                reinterpret_cast<void *>(StatementHandle));

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  std::string catalog = OdbcStringToStdString(CatalogName, NameLength1);
  std::string schema = OdbcStringToStdString(SchemaName, NameLength2);
  std::string table = OdbcStringToStdString(TableName, NameLength3);
  std::string table_type = OdbcStringToStdString(TableType, NameLength4);

  // For MaxCompute, schema and catalog parameters are typically ignored in
  // favor of the connection project This is consistent with typical MaxCompute
  // behavior
  MCO_LOG_DEBUG(
      "Processing SQLTables request - Catalog: '{}', Schema: '{}', Table: "
      "'{}', TableType: '{}'",
      catalog, schema, table, table_type);

  return stmt->tables(catalog, schema, table, table_type);
}

// Unicode version of SQLTables
SQLRETURN SQL_API SQLTablesW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName,
                             SQLSMALLINT NameLength1, SQLWCHAR *SchemaName,
                             SQLSMALLINT NameLength2, SQLWCHAR *TableName,
                             SQLSMALLINT NameLength3, SQLWCHAR *TableType,
                             SQLSMALLINT NameLength4) {
  MCO_LOG_DEBUG("SQLTablesW called with statement handle: {}",
                reinterpret_cast<void *>(StatementHandle));

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) return SQL_INVALID_HANDLE;

  std::string catalog = OdbcWStringToStdString(CatalogName, NameLength1);
  std::string schema = OdbcWStringToStdString(SchemaName, NameLength2);
  std::string table = OdbcWStringToStdString(TableName, NameLength3);
  std::string table_type = OdbcWStringToStdString(TableType, NameLength4);

  // For MaxCompute, schema and catalog parameters are typically ignored in
  // favor of the connection project This is consistent with typical MaxCompute
  // behavior
  MCO_LOG_DEBUG(
      "Processing SQLTablesW request - Catalog: '{}', Schema: '{}', Table: "
      "'{}', TableType: '{}'",
      catalog, schema, table, table_type);

  return stmt->tables(catalog, schema, table, table_type);
}

// --- Diagnostic Functions ---

// SQLGetDiagRec for retrieving ANSI diagnostic information
SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                SQLSMALLINT BufferLength,
                                SQLSMALLINT *TextLengthPtr) {
  MCO_LOG_DEBUG("SQLGetDiagRec called with HandleType: {}, RecNumber: {}",
                HandleType, RecNumber);

  try {
    // Check for valid RecNumber (1-based index)
    if (RecNumber < 1) {
      MCO_LOG_ERROR("Invalid RecNumber in SQLGetDiagRec: {}", RecNumber);
      return SQL_ERROR;
    }

    // First, we need to retrieve the appropriate handle based on the handle
    // type
    maxcompute_odbc::OdbcHandle *baseHandle = nullptr;

    switch (HandleType) {
      case SQL_HANDLE_ENV:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::EnvHandle>(Handle);
        break;
      case SQL_HANDLE_DBC:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::ConnHandle>(Handle);
        break;
      case SQL_HANDLE_STMT:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::StmtHandle>(Handle);
        break;
      case SQL_HANDLE_DESC:
        // Descriptor handles - not yet implemented
        MCO_LOG_INFO("SQL_HANDLE_DESC not currently supported in GetDiag");
        return SQL_ERROR;
      default:
        MCO_LOG_ERROR("Unknown HandleType in SQLGetDiagRec: {}", HandleType);
        return SQL_ERROR;
    }

    if (!baseHandle) {
      MCO_LOG_ERROR("Invalid handle in SQLGetDiagRec, HandleType: {}",
                    HandleType);
      return SQL_INVALID_HANDLE;
    }

    // Get diagnostic record from handle
    const std::vector<maxcompute_odbc::DiagRecord> &diagRecords =
        baseHandle->getDiagRecords();

    // Check if requested record number exists (RecNumber is 1-based)
    if (RecNumber > static_cast<SQLSMALLINT>(diagRecords.size())) {
      MCO_LOG_DEBUG("No diagnostic record at RecNumber: {}", RecNumber);
      return SQL_NO_DATA;
    }

    // Get the record at RecNumber-1 (0-based index in vector)
    const maxcompute_odbc::DiagRecord &record = diagRecords[RecNumber - 1];

    // Write SQLSTATE if provided
    if (Sqlstate) {
      // Copy SQLSTATE ensuring it's exactly 5 characters and null-terminated
      std::string sqlstate_str = record.sql_state;
      sqlstate_str.resize(5, ' ');  // pad to exactly 5 chars
      sqlstate_str += '\0';         // null terminate
      size_t copy_len = std::min(static_cast<size_t>(5), sqlstate_str.length());
      memcpy(Sqlstate, sqlstate_str.c_str(), copy_len);
      if (copy_len < 5) {
        memset(Sqlstate + copy_len, ' ', 5 - copy_len);
      }
      Sqlstate[5] = '\0';
    }

    // Write native error if provided
    if (NativeError) {
      *NativeError = record.native_error_code;
    }

    // Prepare message string if provided
    if (MessageText) {
      std::string message = record.message;
      SQLSMALLINT actual_length = static_cast<SQLSMALLINT>(message.length());

      if (BufferLength > 0) {
        // Copy message to buffer limited by BufferLength-1 to allow for null
        // terminator
        size_t max_copy = BufferLength - 1;
        size_t copy_len =
            std::min(static_cast<size_t>(max_copy), message.length());
        memcpy(MessageText, message.c_str(), copy_len);
        MessageText[copy_len] = '\0';  // null terminate
        if (copy_len < message.length()) {
          actual_length = static_cast<SQLSMALLINT>(copy_len);
        }
      }

      // Report the actual length if requested
      if (TextLengthPtr) {
        *TextLengthPtr = actual_length;
      }

      // If the message was truncated due to buffer size, return
      // SQL_SUCCESS_WITH_INFO
      if (message.length() > static_cast<size_t>(BufferLength - 1) &&
          BufferLength > 0) {
        MCO_LOG_WARNING(
            "SQLGetDiagRec: Buffer too small for message, truncated");
        return SQL_SUCCESS_WITH_INFO;
      }
    } else if (TextLengthPtr) {
      // If message buffer not provided but length is, report required length
      std::string message = record.message;
      *TextLengthPtr = static_cast<SQLSMALLINT>(message.length());
    }

    MCO_LOG_DEBUG("SQLGetDiagRec successful, RecNumber: {}, SQLSTATE: {}",
                  RecNumber, record.sql_state);
    return SQL_SUCCESS;
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLGetDiagRec failed with exception: {}", e.what());
    return SQL_ERROR;
  } catch (...) {
    MCO_LOG_ERROR("SQLGetDiagRec failed with unknown exception");
    return SQL_ERROR;
  }
}

// SQLGetDiagRecW for retrieving wide character diagnostic information
SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                 SQLSMALLINT RecNumber, SQLWCHAR *Sqlstate,
                                 SQLINTEGER *NativeError, SQLWCHAR *MessageText,
                                 SQLSMALLINT BufferLength,
                                 SQLSMALLINT *TextLengthPtr) {
  MCO_LOG_DEBUG("SQLGetDiagRecW called with HandleType: {}, RecNumber: {}",
                HandleType, RecNumber);

  try {
    // Check for valid RecNumber (1-based index)
    if (RecNumber < 1) {
      MCO_LOG_ERROR("Invalid RecNumber in SQLGetDiagRecW: {}", RecNumber);
      return SQL_ERROR;
    }

    // First, we need to retrieve the appropriate handle based on the handle
    // type
    maxcompute_odbc::OdbcHandle *baseHandle = nullptr;

    switch (HandleType) {
      case SQL_HANDLE_ENV:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::EnvHandle>(Handle);
        break;
      case SQL_HANDLE_DBC:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::ConnHandle>(Handle);
        break;
      case SQL_HANDLE_STMT:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::StmtHandle>(Handle);
        break;
      case SQL_HANDLE_DESC:
        // Descriptor handles - not yet implemented
        MCO_LOG_INFO("SQL_HANDLE_DESC not currently supported in GetDiagRecW");
        return SQL_ERROR;
      default:
        MCO_LOG_ERROR("Unknown HandleType in SQLGetDiagRecW: {}", HandleType);
        return SQL_ERROR;
    }

    if (!baseHandle) {
      MCO_LOG_ERROR("Invalid handle in SQLGetDiagRecW, HandleType: {}",
                    HandleType);
      return SQL_INVALID_HANDLE;
    }

    // Get diagnostic record from handle
    const std::vector<maxcompute_odbc::DiagRecord> &diagRecords =
        baseHandle->getDiagRecords();

    // Check if requested record number exists (RecNumber is 1-based)
    if (RecNumber > static_cast<SQLSMALLINT>(diagRecords.size())) {
      MCO_LOG_DEBUG("No diagnostic record at RecNumber: {}", RecNumber);
      return SQL_NO_DATA;
    }

    // Get the record at RecNumber-1 (0-based index in vector)
    const maxcompute_odbc::DiagRecord &record = diagRecords[RecNumber - 1];

    // Write SQLSTATE if provided
    if (Sqlstate) {
      // Create wide string of exactly 5 characters
      std::string sqlstate_str = record.sql_state;
      sqlstate_str.resize(5, ' ');  // pad to exactly 5 chars
      sqlstate_str += '\0';         // null terminate
      for (int i = 0; i < 5 && i < sqlstate_str.length(); ++i) {
        Sqlstate[i] = static_cast<SQLWCHAR>(sqlstate_str[i]);
      }
      // Pad with spaces if shorter
      for (int i = sqlstate_str.length(); i < 5; ++i) {
        Sqlstate[i] = L' ';
      }
      Sqlstate[5] = L'\0';  // null terminate
    }

    // Write native error if provided
    if (NativeError) {
      *NativeError = record.native_error_code;
    }

    // Prepare message string if provided
    if (MessageText) {
      // Convert message to wide character string using proper UTF-8 to UTF-16
      // conversion
      //
      // IMPORTANT: According to ODBC specification, SQLGetDiagRecW's
      // BufferLength is in CHARACTERS, not bytes. But StdStringToOdbcWString
      // uses bytes. We need to convert between them.
      std::string message = record.message;
      SQLSMALLINT actual_bytes = 0;

      if (BufferLength > 0) {
        // Convert character count to byte count for StdStringToOdbcWString
        SQLSMALLINT buffer_bytes =
            static_cast<SQLSMALLINT>(BufferLength * sizeof(SQLWCHAR));
        StdStringToOdbcWString(message, MessageText, buffer_bytes,
                               &actual_bytes);
      } else {
        // Calculate required length even with no buffer
        StdStringToOdbcWString(message, nullptr, 0, &actual_bytes);
      }

      // Report the actual length in characters if requested
      // (ODBC spec requires TextLengthPtr to return character count)
      if (TextLengthPtr) {
        *TextLengthPtr = actual_bytes / sizeof(SQLWCHAR);
      }

      // If the message was truncated due to buffer size, return
      // SQL_SUCCESS_WITH_INFO
      if (BufferLength > 0) {
        // Calculate required buffer size in bytes
        SQLSMALLINT required_bytes = 0;
        StdStringToOdbcWString(message, nullptr, 0, &required_bytes);
        SQLSMALLINT required_chars = required_bytes / sizeof(SQLWCHAR);
        if (required_chars > BufferLength) {
          MCO_LOG_WARNING(
              "SQLGetDiagRecW: Buffer too small for message, truncated");
          return SQL_SUCCESS_WITH_INFO;
        }
      }
    } else if (TextLengthPtr) {
      // If message buffer not provided but length is, report required length
      // in characters
      std::string message = record.message;
      SQLSMALLINT required_bytes = 0;
      StdStringToOdbcWString(message, nullptr, 0, &required_bytes);
      *TextLengthPtr = required_bytes / sizeof(SQLWCHAR);
    }

    MCO_LOG_DEBUG("SQLGetDiagRecW successful, RecNumber: {}, SQLSTATE: {}",
                  RecNumber, record.sql_state);
    return SQL_SUCCESS;
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLGetDiagRecW failed with exception: {}", e.what());
    return SQL_ERROR;
  } catch (...) {
    MCO_LOG_ERROR("SQLGetDiagRecW failed with unknown exception");
    return SQL_ERROR;
  }
}

// SQLGetDiagField for retrieving specific fields from diagnostic records (fixed
// for compilation)
SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                  SQLSMALLINT RecNumber,
                                  SQLSMALLINT DiagIdentifier,
                                  SQLPOINTER DiagInfoPtr,
                                  SQLSMALLINT BufferLength,
                                  SQLSMALLINT *StringLengthPtr) {
  MCO_LOG_DEBUG(
      "SQLGetDiagField called with HandleType: {}, RecNumber: {}, "
      "DiagIdentifier: {}",
      HandleType, RecNumber, DiagIdentifier);

  try {
    // First, we need to retrieve the appropriate handle based on the handle
    // type
    maxcompute_odbc::OdbcHandle *baseHandle = nullptr;

    switch (HandleType) {
      case SQL_HANDLE_ENV:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::EnvHandle>(Handle);
        break;
      case SQL_HANDLE_DBC:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::ConnHandle>(Handle);
        break;
      case SQL_HANDLE_STMT:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::StmtHandle>(Handle);
        break;
      case SQL_HANDLE_DESC:
        // Descriptor handles - not yet implemented
        MCO_LOG_INFO("SQL_HANDLE_DESC not currently supported in GetDiagField");
        return SQL_ERROR;
      default:
        MCO_LOG_ERROR("Unknown HandleType in SQLGetDiagField: {}", HandleType);
        return SQL_ERROR;
    }

    if (!baseHandle) {
      MCO_LOG_ERROR("Invalid handle in SQLGetDiagField, HandleType: {}",
                    HandleType);
      return SQL_INVALID_HANDLE;
    }

    // Handle the main diagnostic identifier that's always available:
    // SQL_DIAG_NUMBER (number of diagnostic records)
    if (DiagIdentifier == SQL_DIAG_NUMBER) {
      const std::vector<maxcompute_odbc::DiagRecord> &diagRecords =
          baseHandle->getDiagRecords();
      SQLINTEGER numRecs = static_cast<SQLINTEGER>(diagRecords.size());

      if (DiagInfoPtr) {
        *(SQLINTEGER *)DiagInfoPtr = numRecs;
      }

      if (StringLengthPtr) {
        *StringLengthPtr = 0;  // For numeric fields
      }

      MCO_LOG_DEBUG(
          "SQLGetDiagField returning number of diagnostic records: {}",
          numRecs);
      return SQL_SUCCESS;
    }

    // Check for specific diagnostic records (RecNumber > 0)
    if (RecNumber > 0) {
      // Check if requested record number exists (RecNumber is 1-based)
      const std::vector<maxcompute_odbc::DiagRecord> &diagRecords =
          baseHandle->getDiagRecords();
      if (RecNumber > static_cast<SQLSMALLINT>(diagRecords.size())) {
        MCO_LOG_DEBUG("No diagnostic record at RecNumber: {}", RecNumber);
        return SQL_NO_DATA;
      }

      // Get the record at RecNumber-1 (0-based index in vector)
      const maxcompute_odbc::DiagRecord &record = diagRecords[RecNumber - 1];

      // Handle various diagnostic identifiers per ODBC spec
      switch (DiagIdentifier) {
        case SQL_DIAG_CLASS_ORIGIN:
        case SQL_DIAG_SUBCLASS_ORIGIN: {
          // For ODBC compliance, these are usually "ODBC 3.0" or similar
          std::string value;
          if (DiagIdentifier == SQL_DIAG_CLASS_ORIGIN) {
            value = "ODBC 3.0";
          } else {  // SQL_DIAG_SUBCLASS_ORIGIN
            value = "ODBC 3.0";
          }

          if (DiagInfoPtr && BufferLength > 0) {
            SQLSMALLINT actual_length =
                static_cast<SQLSMALLINT>(value.length());
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), value.length());
            memcpy(DiagInfoPtr, value.c_str(), copy_len);
            static_cast<SQLCHAR *>(DiagInfoPtr)[copy_len] = '\0';

            if (StringLengthPtr) {
              *StringLengthPtr = static_cast<SQLSMALLINT>(copy_len);
            }

            if (value.length() > copy_len) {
              return SQL_SUCCESS_WITH_INFO;
            }
          } else if (StringLengthPtr) {
            *StringLengthPtr = static_cast<SQLSMALLINT>(value.length());
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_CONNECTION_NAME: {
          // For connection handle, return the connection name
          std::string value = "MaxCompute ODBC Connection";
          if (HandleType == SQL_HANDLE_DBC) {
            // This could potentially return the project name or some connection
            // identifier from config
            auto *conn = maxcompute_odbc::HandleRegistry::instance()
                             .get<maxcompute_odbc::ConnHandle>(Handle);
            if (conn) {
              auto &config = conn->getConfigForUpdate();
              if (!config.project.empty()) {
                value = config.project;
              }
            }
          }

          if (DiagInfoPtr && BufferLength > 0) {
            SQLSMALLINT actual_length =
                static_cast<SQLSMALLINT>(value.length());
            size_t copy_len =
                std::min(static_cast<size_t>(BufferLength - 1), value.length());
            memcpy(DiagInfoPtr, value.c_str(), copy_len);
            static_cast<SQLCHAR *>(DiagInfoPtr)[copy_len] = '\0';

            if (StringLengthPtr) {
              *StringLengthPtr = static_cast<SQLSMALLINT>(copy_len);
            }

            if (value.length() > copy_len) {
              return SQL_SUCCESS_WITH_INFO;
            }
          } else if (StringLengthPtr) {
            *StringLengthPtr = static_cast<SQLSMALLINT>(value.length());
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_SQLSTATE: {
          std::string sqlstate_str = record.sql_state;
          sqlstate_str.resize(5, ' ');  // pad to exactly 5 chars

          if (DiagInfoPtr && BufferLength >= 6) {  // 5 chars + null terminator
            // Ensure there's space for null terminator
            size_t copy_len = std::min(static_cast<size_t>(BufferLength - 1),
                                       sqlstate_str.length());
            memcpy(DiagInfoPtr, sqlstate_str.c_str(), copy_len);

            // Ensure null-termination
            if (copy_len < 5) {
              memset(static_cast<SQLCHAR *>(DiagInfoPtr) + copy_len, ' ',
                     5 - copy_len);
            }
            static_cast<SQLCHAR *>(DiagInfoPtr)[5] = '\0';

            if (StringLengthPtr) {
              *StringLengthPtr =
                  5;  // ODBC standard says SQLSTATE is 5 characters
            }
          } else if (StringLengthPtr) {
            *StringLengthPtr = 5;
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_NATIVE: {
          if (DiagInfoPtr) {
            *(SQLINTEGER *)DiagInfoPtr = record.native_error_code;
          }
          if (StringLengthPtr) {
            *StringLengthPtr = 0;  // For numeric fields
          }
          return SQL_SUCCESS;
        }
        case SQL_DIAG_MESSAGE_TEXT: {
          std::string message = record.message;
          SQLSMALLINT actual_length =
              static_cast<SQLSMALLINT>(message.length());

          if (DiagInfoPtr && BufferLength > 0) {
            size_t copy_len = std::min(static_cast<size_t>(BufferLength - 1),
                                       message.length());
            memcpy(DiagInfoPtr, message.c_str(), copy_len);
            static_cast<SQLCHAR *>(DiagInfoPtr)[copy_len] = '\0';

            if (StringLengthPtr) {
              *StringLengthPtr = static_cast<SQLSMALLINT>(copy_len);
            }

            if (message.length() > copy_len) {
              return SQL_SUCCESS_WITH_INFO;
            }
          } else if (StringLengthPtr) {
            *StringLengthPtr = actual_length;
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_ROW_NUMBER: {
          // For record, return 0 (undefined row for general diagnostic info)
          if (DiagInfoPtr) {
            *(SQLLEN *)DiagInfoPtr =
                0;  // SQL_ROW_NUMBER for error is typically 0
          }
          if (StringLengthPtr) {
            *StringLengthPtr = 0;  // For numeric fields
          }
          return SQL_SUCCESS;
        }
        case SQL_DIAG_COLUMN_NUMBER: {
          // For record, return 0 (undefined column for general diagnostic info)
          if (DiagInfoPtr) {
            *(SQLSMALLINT *)DiagInfoPtr =
                0;  // SQL_COLUMN_NUMBER for error is typically 0
          }
          if (StringLengthPtr) {
            *StringLengthPtr = 0;  // For numeric fields
          }
          return SQL_SUCCESS;
        }
        default:
          MCO_LOG_WARNING("SQLGetDiagField: Unsupported DiagIdentifier: {}",
                          DiagIdentifier);
          return SQL_ERROR;
      }
    } else if (RecNumber == 0) {  // RecNumber == 0 - header field access (only
                                  // SQL_DIAG_NUMBER needed)
      // Handle only the SQL_DIAG_NUMBER case for header, which is processed
      // before this section
      MCO_LOG_ERROR(
          "SQLGetDiagField: Unexpected call for RecNumber 0 and identifier not "
          "SQL_DIAG_NUMBER");
      return SQL_ERROR;
    } else {  // RecNumber < 0 (invalid)
      MCO_LOG_ERROR("Invalid RecNumber in SQLGetDiagField: {}", RecNumber);
      return SQL_ERROR;
    }
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLGetDiagField failed with exception: {}", e.what());
    return SQL_ERROR;
  } catch (...) {
    MCO_LOG_ERROR("SQLGetDiagField failed with unknown exception");
    return SQL_ERROR;
  }
}

// SQLGetDiagFieldW for retrieving wide character diagnostic fields (fixed for
// compilation)
SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   SQLSMALLINT RecNumber,
                                   SQLSMALLINT DiagIdentifier,
                                   SQLPOINTER DiagInfoPtr,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT *StringLengthPtr) {
  MCO_LOG_DEBUG(
      "SQLGetDiagFieldW called with HandleType: {}, RecNumber: {}, "
      "DiagIdentifier: {}",
      HandleType, RecNumber, DiagIdentifier);

  try {
    // First, we need to retrieve the appropriate handle based on the handle
    // type
    maxcompute_odbc::OdbcHandle *baseHandle = nullptr;

    switch (HandleType) {
      case SQL_HANDLE_ENV:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::EnvHandle>(Handle);
        break;
      case SQL_HANDLE_DBC:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::ConnHandle>(Handle);
        break;
      case SQL_HANDLE_STMT:
        baseHandle = maxcompute_odbc::HandleRegistry::instance()
                         .get<maxcompute_odbc::StmtHandle>(Handle);
        break;
      case SQL_HANDLE_DESC:
        // Descriptor handles - not yet implemented
        MCO_LOG_INFO(
            "SQL_HANDLE_DESC not currently supported in GetDiagFieldW");
        return SQL_ERROR;
      default:
        MCO_LOG_ERROR("Unknown HandleType in SQLGetDiagFieldW: {}", HandleType);
        return SQL_ERROR;
    }

    if (!baseHandle) {
      MCO_LOG_ERROR("Invalid handle in SQLGetDiagFieldW, HandleType: {}",
                    HandleType);
      return SQL_INVALID_HANDLE;
    }

    // Handle the main diagnostic identifier that's always available:
    // SQL_DIAG_NUMBER (number of diagnostic records)
    if (DiagIdentifier == SQL_DIAG_NUMBER) {
      const std::vector<maxcompute_odbc::DiagRecord> &diagRecords =
          baseHandle->getDiagRecords();
      SQLINTEGER numRecs = static_cast<SQLINTEGER>(diagRecords.size());

      if (DiagInfoPtr) {
        *(SQLINTEGER *)DiagInfoPtr = numRecs;
      }

      if (StringLengthPtr) {
        *StringLengthPtr = 0;  // For numeric fields
      }

      MCO_LOG_DEBUG(
          "SQLGetDiagFieldW returning number of diagnostic records: {}",
          numRecs);
      return SQL_SUCCESS;
    }

    // Check for specific diagnostic records (RecNumber > 0)
    if (RecNumber > 0) {
      // Check if requested record number exists (RecNumber is 1-based)
      const std::vector<maxcompute_odbc::DiagRecord> &diagRecords =
          baseHandle->getDiagRecords();
      if (RecNumber > static_cast<SQLSMALLINT>(diagRecords.size())) {
        MCO_LOG_DEBUG("No diagnostic record at RecNumber: {}", RecNumber);
        return SQL_NO_DATA;
      }

      // Get the record at RecNumber-1 (0-based index in vector)
      const maxcompute_odbc::DiagRecord &record = diagRecords[RecNumber - 1];

      // Handle various diagnostic identifiers per ODBC spec
      switch (DiagIdentifier) {
        case SQL_DIAG_CLASS_ORIGIN:
        case SQL_DIAG_SUBCLASS_ORIGIN: {
          // For ODBC compliance, these are usually "ODBC 3.0" or similar
          std::string value;
          if (DiagIdentifier == SQL_DIAG_CLASS_ORIGIN) {
            value = "ODBC 3.0";
          } else {  // SQL_DIAG_SUBCLASS_ORIGIN
            value = "ODBC 3.0";
          }

          SQLSMALLINT actual_length = 0;
          if (DiagInfoPtr && BufferLength > 0) {
            StdStringToOdbcWString(value, static_cast<SQLWCHAR *>(DiagInfoPtr),
                                   BufferLength, &actual_length);
            if (StringLengthPtr) {
              *StringLengthPtr = actual_length;
            }
            // Check for truncation
            SQLSMALLINT required_length = 0;
            StdStringToOdbcWString(value, nullptr, 0, &required_length);
            if (required_length > BufferLength) {
              return SQL_SUCCESS_WITH_INFO;
            }
          } else if (StringLengthPtr) {
            StdStringToOdbcWString(value, nullptr, 0, &actual_length);
            *StringLengthPtr = actual_length;
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_CONNECTION_NAME: {
          // For connection handle, return the connection name
          std::string value = "MaxCompute ODBC Connection";
          if (HandleType == SQL_HANDLE_DBC) {
            // This could potentially return the project name or some connection
            // identifier from config
            auto *conn = maxcompute_odbc::HandleRegistry::instance()
                             .get<maxcompute_odbc::ConnHandle>(Handle);
            if (conn) {
              auto &config = conn->getConfigForUpdate();
              if (!config.project.empty()) {
                value = config.project;
              }
            }
          }

          SQLSMALLINT actual_length = 0;
          if (DiagInfoPtr && BufferLength > 0) {
            StdStringToOdbcWString(value, static_cast<SQLWCHAR *>(DiagInfoPtr),
                                   BufferLength, &actual_length);
            if (StringLengthPtr) {
              *StringLengthPtr = actual_length;
            }
            // Check for truncation
            SQLSMALLINT required_length = 0;
            StdStringToOdbcWString(value, nullptr, 0, &required_length);
            if (required_length > BufferLength) {
              return SQL_SUCCESS_WITH_INFO;
            }
          } else if (StringLengthPtr) {
            StdStringToOdbcWString(value, nullptr, 0, &actual_length);
            *StringLengthPtr = actual_length;
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_SQLSTATE: {
          std::string sqlstate_str = record.sql_state;
          sqlstate_str.resize(5, ' ');  // pad to exactly 5 chars

          if (DiagInfoPtr &&
              BufferLength >=
                  static_cast<SQLSMALLINT>(
                      6 * sizeof(SQLWCHAR))) {  // 5 chars + null terminator
            // SQLSTATE is always 5 ASCII characters, so simple conversion is OK
            SQLWCHAR *dest = static_cast<SQLWCHAR *>(DiagInfoPtr);
            for (int i = 0; i < 5; ++i) {
              dest[i] = static_cast<SQLWCHAR>(sqlstate_str[i]);
            }
            dest[5] = L'\0';

            if (StringLengthPtr) {
              *StringLengthPtr =
                  5 * sizeof(SQLWCHAR);  // ODBC standard: SQLSTATE is 5 chars
            }
          } else if (StringLengthPtr) {
            *StringLengthPtr = 5 * sizeof(SQLWCHAR);
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_NATIVE: {
          if (DiagInfoPtr) {
            *(SQLINTEGER *)DiagInfoPtr = record.native_error_code;
          }
          if (StringLengthPtr) {
            *StringLengthPtr = 0;  // For numeric fields
          }
          return SQL_SUCCESS;
        }
        case SQL_DIAG_MESSAGE_TEXT: {
          std::string message = record.message;
          SQLSMALLINT actual_length = 0;

          if (DiagInfoPtr && BufferLength > 0) {
            StdStringToOdbcWString(message,
                                   static_cast<SQLWCHAR *>(DiagInfoPtr),
                                   BufferLength, &actual_length);
            if (StringLengthPtr) {
              *StringLengthPtr = actual_length;
            }
            // Check for truncation
            SQLSMALLINT required_length = 0;
            StdStringToOdbcWString(message, nullptr, 0, &required_length);
            if (required_length > BufferLength) {
              return SQL_SUCCESS_WITH_INFO;
            }
          } else if (StringLengthPtr) {
            StdStringToOdbcWString(message, nullptr, 0, &actual_length);
            *StringLengthPtr = actual_length;
          }

          return SQL_SUCCESS;
        }
        case SQL_DIAG_ROW_NUMBER: {
          // For record, return 0 (undefined row for general diagnostic info)
          if (DiagInfoPtr) {
            *(SQLLEN *)DiagInfoPtr =
                0;  // SQL_ROW_NUMBER for error is typically 0
          }
          if (StringLengthPtr) {
            *StringLengthPtr = 0;  // For numeric fields
          }
          return SQL_SUCCESS;
        }
        case SQL_DIAG_COLUMN_NUMBER: {
          // For record, return 0 (undefined column for general diagnostic info)
          if (DiagInfoPtr) {
            *(SQLSMALLINT *)DiagInfoPtr =
                0;  // SQL_COLUMN_NUMBER for error is typically 0
          }
          if (StringLengthPtr) {
            *StringLengthPtr = 0;  // For numeric fields
          }
          return SQL_SUCCESS;
        }
        default:
          MCO_LOG_WARNING("SQLGetDiagFieldW: Unsupported DiagIdentifier: {}",
                          DiagIdentifier);
          return SQL_ERROR;
      }
    } else if (RecNumber == 0) {  // RecNumber == 0 - header field access (only
                                  // SQL_DIAG_NUMBER needed)
      // Handle only the SQL_DIAG_NUMBER case for header, which is processed
      // before this section
      MCO_LOG_ERROR(
          "SQLGetDiagFieldW: Unexpected call for RecNumber 0 and identifier "
          "not SQL_DIAG_NUMBER");
      return SQL_ERROR;
    } else {  // RecNumber < 0 (invalid)
      MCO_LOG_ERROR("Invalid RecNumber in SQLGetDiagFieldW: {}", RecNumber);
      return SQL_ERROR;
    }
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLGetDiagFieldW failed with exception: {}", e.what());
    return SQL_ERROR;
  } catch (...) {
    MCO_LOG_ERROR("SQLGetDiagFieldW failed with unknown exception");
    return SQL_ERROR;
  }
}

// SQLSetEnvAttr for setting environment attributes (including ODBC version)
SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute,
                                SQLPOINTER ValuePtr, SQLINTEGER StringLength) {
  MCO_LOG_DEBUG("SQLSetEnvAttr called with env handle: {}, attribute: {}",
                reinterpret_cast<void *>(EnvironmentHandle), Attribute);

  (void)StringLength;  // Suppress unused parameter warning

  auto *env = maxcompute_odbc::HandleRegistry::instance()
                  .get<maxcompute_odbc::EnvHandle>(EnvironmentHandle);
  if (!env) {
    return SQL_INVALID_HANDLE;
  }

  switch (Attribute) {
    case SQL_ATTR_ODBC_VERSION: {
      SQLINTEGER ver = (SQLINTEGER)(intptr_t)ValuePtr;
      if (ver == SQL_OV_ODBC3 || ver == SQL_OV_ODBC3_80) {
        env->setOdbcVersion(ver);
        MCO_LOG_INFO("SQLSetEnvAttr: ODBC version set to {}", ver);
        return SQL_SUCCESS;
      } else {
        // 不支持的版本
        MCO_LOG_ERROR("SQLSetEnvAttr: unsupported ODBC version {}", ver);
        env->addDiagRecord({0, "HYC00", "Unsupported ODBC version requested"});
        return SQL_ERROR;
      }
    }

    // 你可以以后支持更多属性，如 SQL_ATTR_CONNECTION_POOLING 等
    default:
      MCO_LOG_WARNING("SQLSetEnvAttr: unsupported attribute {}", Attribute);
      env->addDiagRecord({0, "HYC00", "Unsupported environment attribute"});
      return SQL_ERROR;
  }
}

// SQLGetStmtAttr function for retrieving statement attributes
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                 SQLPOINTER ValuePtr, SQLINTEGER BufferLength,
                                 SQLINTEGER *StringLengthPtr) {
  MCO_LOG_DEBUG("SQLGetStmtAttr called with stmt handle: {}, attribute: {}",
                reinterpret_cast<void *>(StatementHandle), Attribute);

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) {
    MCO_LOG_ERROR("SQLGetStmtAttr: Invalid statement handle received");
    return SQL_INVALID_HANDLE;
  }

  // 根据不同属性处理
  switch (Attribute) {
    // Common statement attributes that most drivers should support
    case SQL_ATTR_APP_PARAM_DESC:
      // 返回默认参数描述符
      if (ValuePtr) {
        *(SQLHDESC *)ValuePtr = stmt->getInputDescriptor();
        if (StringLengthPtr) *StringLengthPtr = SQL_IS_POINTER;
      }
      break;

    case SQL_ATTR_APP_ROW_DESC:
      // 返回默认行描述符
      if (ValuePtr) {
        *(SQLHDESC *)ValuePtr = stmt->getOutputDescriptor();
        if (StringLengthPtr) *StringLengthPtr = SQL_IS_POINTER;
      }
      break;

    case SQL_ATTR_IMP_PARAM_DESC:
      // 实现参数描述符
      if (ValuePtr) {
        *(SQLHDESC *)ValuePtr = stmt->getInputDescriptor();
        if (StringLengthPtr) *StringLengthPtr = SQL_IS_POINTER;
      }
      break;

    case SQL_ATTR_IMP_ROW_DESC:
      // 实现行描述符
      if (ValuePtr) {
        *(SQLHDESC *)ValuePtr = stmt->getOutputDescriptor();
        if (StringLengthPtr) *StringLengthPtr = SQL_IS_POINTER;
      }
      break;

    case SQL_ATTR_CURSOR_TYPE:
      // 默认支持只读游标
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_CURSOR_FORWARD_ONLY;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_CONCURRENCY:
      // 默认支持只读并发
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_CONCUR_READ_ONLY;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_QUERY_TIMEOUT:
      // 默认设置超时为1小时（3600秒）
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = 3600;  // 1 hour timeout
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_RETRIEVE_DATA:
      // 默认为启用状态
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_RD_ON;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_ROW_NUMBER:
      // 返回当前行号，MaxCompute不支持随机访问，返回 -1 表示无效
      if (ValuePtr) {
        *(SQLLEN *)ValuePtr = -1;  // 表示当前没有有效行号
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLLEN);
      }
      break;

    case SQL_ATTR_ASYNC_ENABLE:
      // 默认禁用异步操作
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_ASYNC_ENABLE_OFF;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_CURSOR_SCROLLABLE:
      // 不支持可滚动游标
      if (ValuePtr) {
        *(SQLLEN *)ValuePtr = SQL_NONSCROLLABLE;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLLEN);
      }
      break;

    case SQL_ATTR_CURSOR_SENSITIVITY:
      // 非敏感游标
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_INSENSITIVE;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_ENABLE_AUTO_IPD:
      // 默认不启用自动IPD更新
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_FALSE;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_FETCH_BOOKMARK_PTR:
      // 不支持书签取值
      if (ValuePtr) {
        *(void **)ValuePtr = nullptr;
        if (StringLengthPtr) *StringLengthPtr = SQL_IS_POINTER;
      }
      break;

    case SQL_ATTR_KEYSET_SIZE:
      // 不支持键集游标
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = 0;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_MAX_LENGTH:
      // 默认最大长度（适用于MaxCompute的输出）
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = 0;  // 0表示无限制
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_MAX_ROWS:
      // 默认最大行数（适用于MaxCompute的输出）
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = 0;  // 0表示无限制
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_NOSCAN:
      // 扫描开关
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_NOSCAN_OFF;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      // 参数绑定偏移指针
      if (ValuePtr) {
        *(SQLULEN **)ValuePtr = nullptr;  // 默认为 nullptr
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLULEN *);
      }
      break;

    case SQL_ATTR_PARAM_BIND_TYPE:
      // 参数绑定类型
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_PARAM_BIND_BY_COLUMN;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_PARAM_OPERATION_PTR:
      // 参数操作指针
      if (ValuePtr) {
        *(SQLUSMALLINT **)ValuePtr = nullptr;  // 默认为 nullptr
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUSMALLINT *);
      }
      break;

    case SQL_ATTR_PARAM_STATUS_PTR:
      // 参数状态指针
      if (ValuePtr) {
        *(SQLUSMALLINT **)ValuePtr = nullptr;  // 默认为 nullptr
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUSMALLINT *);
      }
      break;

    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      // 已处理参数指针
      if (ValuePtr) {
        *(SQLULEN **)ValuePtr = nullptr;  // 默认为 nullptr
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLULEN *);
      }
      break;

    case SQL_ATTR_PARAMSET_SIZE:
      // 参数集大小
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = 1;  // 默认为1
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      // 行绑定偏移指针
      if (ValuePtr) {
        *(SQLULEN **)ValuePtr = nullptr;  // 默认为 nullptr
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLULEN *);
      }
      break;

    case SQL_ATTR_ROW_BIND_TYPE:
      // 行绑定类型
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_BIND_BY_COLUMN;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_ROW_OPERATION_PTR:
      // 行操作指针
      if (ValuePtr) {
        *(SQLUSMALLINT **)ValuePtr = nullptr;  // 默认为 nullptr
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUSMALLINT *);
      }
      break;

    case SQL_ATTR_ROW_STATUS_PTR:
      // 行状态指针
      if (ValuePtr) {
        *(SQLUSMALLINT **)ValuePtr = nullptr;  // 默认为 nullptr
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUSMALLINT *);
      }
      break;

    case SQL_ATTR_ROWS_FETCHED_PTR:
      // 读取的行指针
      if (ValuePtr) {
        *(SQLLEN **)ValuePtr =
            stmt->getFetchedRowsPtr();  // 指向句柄内部的计数器，类型要匹配
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLLEN *);
      }
      break;

    case SQL_ATTR_SIMULATE_CURSOR:
      // 模拟游标
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_SC_NON_UNIQUE;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_USE_BOOKMARKS:
      // 使用书签
      if (ValuePtr) {
        *(SQLUINTEGER *)ValuePtr = SQL_UB_OFF;
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
      }
      break;

    case SQL_ATTR_ROW_ARRAY_SIZE:
      // 行数组大小
      if (ValuePtr) {
        *(SQLULEN *)ValuePtr = 1;  // 默认为1
        if (StringLengthPtr) *StringLengthPtr = sizeof(SQLULEN);
      }
      break;

      // case SQL_ATTR_ROW_ARRAY_SIZE_PTR:  // This might not be available in
      // all ODBC headers
      //   // 行数组大小指针
      //   if (ValuePtr) {
      //     *(SQLULEN**)ValuePtr = nullptr; // 默认为 nullptr
      //     if (StringLengthPtr) *StringLengthPtr = sizeof(SQLULEN*);
      //   }
      //   break;

    default:
      // 不支持的属性
      MCO_LOG_WARNING("SQLGetStmtAttr: unsupported attribute {}", Attribute);
      stmt->addDiagRecord({0, "HYC00", "Unsupported statement attribute"});
      return SQL_ERROR;
  }

  MCO_LOG_DEBUG("SQLGetStmtAttr: returning attribute {} value successfully",
                Attribute);
  return SQL_SUCCESS;
}

// SQLSetStmtAttrW function for setting statement attributes (wide version)
SQLRETURN SQL_API SQLSetStmtAttrW(SQLHSTMT StatementHandle,
                                  SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                  SQLINTEGER StringLength) {
  MCO_LOG_ERROR("SQLSetStmtAttrW called, StatementHandle={}",
                (void *)StatementHandle);
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  MCO_LOG_ERROR("SQLSetStmtAttrW: stmt pointer={}", (void *)stmt);
  if (!stmt) {
    MCO_LOG_ERROR("SQLSetStmtAttrW: Invalid statement handle received");
    return SQL_INVALID_HANDLE;
  }

  // 根据不同属性处理
  switch (Attribute) {
    // Support SQL_ATTR_ROW_ARRAY_SIZE and SQL_ROWSET_SIZE (which is equivalent)
    case SQL_ATTR_ROW_ARRAY_SIZE:
    case SQL_ROWSET_SIZE:
      // Accept any reasonable value for row array size since we don't restrict
      // it For this implementation, we just accept the value without making
      // changes since MaxCompute is not typically used for bulk operations in
      // the same way traditional database systems are
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting ROW_ARRAY_SIZE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      // Parameter bind offset pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttrW: Setting PARAM_BIND_OFFSET_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_BIND_TYPE:
      // Parameter bind type
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting PARAM_BIND_TYPE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_OPERATION_PTR:
      // Parameter operation pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttrW: Setting PARAM_OPERATION_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_STATUS_PTR:
      // Parameter status pointer
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting PARAM_STATUS_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      // Parameters processed pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttrW: Setting PARAMS_PROCESSED_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAMSET_SIZE:
      // Parameter set size
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting PARAMSET_SIZE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      // Row bind offset pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttrW: Setting ROW_BIND_OFFSET_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_BIND_TYPE:
      // Row bind type
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting ROW_BIND_TYPE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_OPERATION_PTR:
      // Row operation pointer
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting ROW_OPERATION_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_STATUS_PTR:
      // Row status pointer
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting ROW_STATUS_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_ROWS_FETCHED_PTR:
      // Applications like Power BI use this to set a pointer where the number
      // of rows fetched will be stored. This is supported by the statement
      // handle which has m_fetched_rows For ODBC compliance, we accept the
      // pointer but we don't need to store it directly since SQLGetStmtAttr
      // handles the retrieval. The statement handle has the internal tracking
      // of rows fetched in m_fetched_rows.
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting ROWS_FETCHED_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_CURSOR_TYPE:
      // Support setting cursor type to forward-only
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting CURSOR_TYPE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_CONCURRENCY:
      // Support setting concurrency to read-only
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting CONCURRENCY to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ASYNC_ENABLE:
      // Support async enable/disable
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting ASYNC_ENABLE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_NOSCAN:
      // Support no scan option
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting NOSCAN to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_QUERY_TIMEOUT:
      // Support query timeout setting
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting QUERY_TIMEOUT to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ENABLE_AUTO_IPD:
      // Support auto IPD updates
      MCO_LOG_DEBUG("SQLSetStmtAttrW: Setting ENABLE_AUTO_IPD to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    // Add other statement attributes as needed
    default:
      // 不支持的属性
      MCO_LOG_DEBUG(
          "SQLSetStmtAttrW: unsupported attribute {}, reporting success anyway",
          Attribute);
      // We return SQL_SUCCESS for many unsupported attributes to ensure
      // compatibility with applications like Power BI that may set optional
      // attributes
      return SQL_SUCCESS;
  }
}

// SQLSetStmtAttr function for setting statement attributes
SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                 SQLPOINTER ValuePtr, SQLINTEGER StringLength) {
  MCO_LOG_DEBUG("SQLSetStmtAttr called with stmt handle: {}, attribute: {}",
                reinterpret_cast<void *>(StatementHandle), Attribute);

  (void)StringLength;  // Suppress unused parameter warning

  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) {
    MCO_LOG_ERROR("SQLSetStmtAttr: Invalid statement handle received");
    return SQL_INVALID_HANDLE;
  }

  // 根据不同属性处理
  switch (Attribute) {
    // Support SQL_ATTR_ROW_ARRAY_SIZE and SQL_ROWSET_SIZE (which is equivalent)
    case SQL_ATTR_ROW_ARRAY_SIZE:
    case SQL_ROWSET_SIZE:
      // Accept any reasonable value for row array size since we don't restrict
      // it For this implementation, we just accept the value without making
      // changes since MaxCompute is not typically used for bulk operations in
      // the same way traditional database systems are
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting ROW_ARRAY_SIZE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      // Parameter bind offset pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttr: Setting PARAM_BIND_OFFSET_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_BIND_TYPE:
      // Parameter bind type
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting PARAM_BIND_TYPE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_OPERATION_PTR:
      // Parameter operation pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttr: Setting PARAM_OPERATION_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAM_STATUS_PTR:
      // Parameter status pointer
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting PARAM_STATUS_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      // Parameters processed pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttr: Setting PARAMS_PROCESSED_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_PARAMSET_SIZE:
      // Parameter set size
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting PARAMSET_SIZE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      // Row bind offset pointer
      MCO_LOG_DEBUG(
          "SQLSetStmtAttr: Setting ROW_BIND_OFFSET_PTR to address: {}",
          reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_BIND_TYPE:
      // Row bind type
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting ROW_BIND_TYPE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_OPERATION_PTR:
      // Row operation pointer
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting ROW_OPERATION_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_ROW_STATUS_PTR:
      // Row status pointer
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting ROW_STATUS_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_ROWS_FETCHED_PTR:
      // Applications like Power BI use this to set a pointer where the number
      // of rows fetched will be stored. This is supported by the statement
      // handle which has m_fetched_rows For ODBC compliance, we accept the
      // pointer but we don't need to store it directly since SQLGetStmtAttr
      // handles the retrieval. The statement handle has the internal tracking
      // of rows fetched in m_fetched_rows.
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting ROWS_FETCHED_PTR to address: {}",
                    reinterpret_cast<void *>(ValuePtr));
      return SQL_SUCCESS;

    case SQL_ATTR_CURSOR_TYPE:
      // Support setting cursor type to forward-only
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting CURSOR_TYPE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_CONCURRENCY:
      // Support setting concurrency to read-only
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting CONCURRENCY to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ASYNC_ENABLE:
      // Support async enable/disable
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting ASYNC_ENABLE to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_NOSCAN:
      // Support no scan option
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting NOSCAN to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_QUERY_TIMEOUT:
      // Support query timeout setting
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting QUERY_TIMEOUT to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    case SQL_ATTR_ENABLE_AUTO_IPD:
      // Support auto IPD updates
      MCO_LOG_DEBUG("SQLSetStmtAttr: Setting ENABLE_AUTO_IPD to value: {}",
                    (SQLUINTEGER)(intptr_t)ValuePtr);
      return SQL_SUCCESS;

    // Add other statement attributes as needed
    default:
      // 不支持的属性
      MCO_LOG_DEBUG(
          "SQLSetStmtAttr: unsupported attribute {}, reporting success anyway",
          Attribute);
      // We return SQL_SUCCESS for many unsupported attributes to ensure
      // compatibility with applications like Power BI that may set optional
      // attributes
      return SQL_SUCCESS;
  }
}

// SQLGetInfo function for retrieving information from the driver/data source
SQLRETURN SQL_API SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                             SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                             SQLSMALLINT *StringLengthPtr) {
  return GetInfoImpl<char>(ConnectionHandle, InfoType, InfoValuePtr,
                           BufferLength, StringLengthPtr);
}

// SQLGetInfoW for wide character version
SQLRETURN SQL_API SQLGetInfoW(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                              SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                              SQLSMALLINT *StringLengthPtr) {
  return GetInfoImpl<char>(ConnectionHandle, InfoType, InfoValuePtr,
                           BufferLength, StringLengthPtr);
}

// SQLMoreResults function to return SQL_NO_DATA
SQLRETURN SQL_API SQLMoreResults(SQLHSTMT StatementHandle) {
  MCO_LOG_DEBUG("SQLMoreResults called with statement handle: {}",
                reinterpret_cast<void *>(StatementHandle));

  // Check for valid handle, but we don't need to fail on invalid handles since
  // this implementation just returns SQL_NO_DATA regardless
  auto *stmt = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::StmtHandle>(StatementHandle);
  if (!stmt) {
    MCO_LOG_DEBUG(
        "SQLMoreResults: Invalid statement handle, returning SQL_NO_DATA");
    return SQL_NO_DATA;
  }

  // MaxCompute SQL queries only return a single result set, so there are never
  // more results This is the expected behavior for this API in context of
  // MaxCompute
  MCO_LOG_DEBUG(
      "SQLMoreResults: No additional result sets available, returning "
      "SQL_NO_DATA");
  return SQL_NO_DATA;
}

// SQLGetConnectAttr function for retrieving connection attributes
SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle,
                                    SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                    SQLINTEGER BufferLength,
                                    SQLINTEGER *StringLengthPtr) {
  MCO_LOG_DEBUG(
      "SQLGetConnectAttr called with connection handle: {}, attribute: {}",
      reinterpret_cast<void *>(ConnectionHandle), Attribute);

  auto *conn = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
  if (!conn) {
    MCO_LOG_ERROR("SQLGetConnectAttr: Invalid connection handle received");
    return SQL_INVALID_HANDLE;
  }

  try {
    switch (Attribute) {
      case SQL_ATTR_AUTOCOMMIT: {
        // MaxCompute doesn't support transactions, so we're always in
        // autocommit mode
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = SQL_AUTOCOMMIT_ON;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        MCO_LOG_DEBUG(
            "SQLGetConnectAttr: SQL_ATTR_AUTOCOMMIT = SQL_AUTOCOMMIT_ON");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_TXN_ISOLATION: {
        // Return READ COMMITTED as default isolation level
        // Even though MaxCompute doesn't support transactions, ODBC Driver
        // Manager expects a valid isolation level
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = SQL_TXN_READ_COMMITTED;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        MCO_LOG_DEBUG(
            "SQLGetConnectAttr: SQL_ATTR_TXN_ISOLATION = "
            "SQL_TXN_READ_COMMITTED");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_CONNECTION_DEAD: {
        // Check if connection is still active
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr =
              (conn->getSDK() != nullptr) ? SQL_CD_FALSE : SQL_CD_TRUE;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        MCO_LOG_DEBUG(
            "SQLGetConnectAttr: SQL_ATTR_CONNECTION_DEAD = {}",
            (conn->getSDK() != nullptr) ? "SQL_CD_FALSE" : "SQL_CD_TRUE");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_ANSI_APP: {
        // Default to ANSI application mode
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = SQL_AA_TRUE;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        return SQL_SUCCESS;
      }

      case SQL_ATTR_PACKET_SIZE: {
        // Default packet size
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = 4096;  // Standard packet size
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        return SQL_SUCCESS;
      }

      default:
        MCO_LOG_WARNING("SQLGetConnectAttr: unsupported attribute {}",
                        Attribute);
        conn->addDiagRecord({0, "HYC00", "Unsupported connection attribute"});
        return SQL_ERROR;
    }
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLGetConnectAttr failed with exception: {}", e.what());
    conn->addDiagRecord(
        {0, "HY000", std::string("General error: ") + e.what()});
    return SQL_ERROR;
  }
}

// SQLGetConnectAttrW function for retrieving connection attributes (wide
// version)
SQLRETURN SQL_API SQLGetConnectAttrW(SQLHDBC ConnectionHandle,
                                     SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                     SQLINTEGER BufferLength,
                                     SQLINTEGER *StringLengthPtr) {
  MCO_LOG_DEBUG(
      "SQLGetConnectAttrW called with connection handle: {}, attribute: {}",
      reinterpret_cast<void *>(ConnectionHandle), Attribute);

  auto *conn = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
  if (!conn) {
    MCO_LOG_ERROR("SQLGetConnectAttrW: Invalid connection handle received");
    return SQL_INVALID_HANDLE;
  }

  try {
    switch (Attribute) {
      case SQL_ATTR_AUTOCOMMIT: {
        // MaxCompute doesn't support transactions, so we're always in
        // autocommit mode
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = SQL_AUTOCOMMIT_ON;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        MCO_LOG_DEBUG(
            "SQLGetConnectAttrW: SQL_ATTR_AUTOCOMMIT = SQL_AUTOCOMMIT_ON");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_TXN_ISOLATION: {
        // Return READ COMMITTED as default isolation level
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = SQL_TXN_READ_COMMITTED;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        MCO_LOG_DEBUG(
            "SQLGetConnectAttrW: SQL_ATTR_TXN_ISOLATION = "
            "SQL_TXN_READ_COMMITTED");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_CONNECTION_DEAD: {
        // Check if connection is still active
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr =
              (conn->getSDK() != nullptr) ? SQL_CD_FALSE : SQL_CD_TRUE;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        MCO_LOG_DEBUG(
            "SQLGetConnectAttrW: SQL_ATTR_CONNECTION_DEAD = {}",
            (conn->getSDK() != nullptr) ? "SQL_CD_FALSE" : "SQL_CD_TRUE");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_ANSI_APP: {
        // Default to ANSI application mode
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = SQL_AA_TRUE;
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        return SQL_SUCCESS;
      }

      case SQL_ATTR_PACKET_SIZE: {
        // Default packet size
        if (ValuePtr) {
          *(SQLUINTEGER *)ValuePtr = 4096;  // Standard packet size
        }
        if (StringLengthPtr) {
          *StringLengthPtr = sizeof(SQLUINTEGER);
        }
        return SQL_SUCCESS;
      }

      default:
        MCO_LOG_WARNING("SQLGetConnectAttrW: unsupported attribute {}",
                        Attribute);
        conn->addDiagRecord({0, "HYC00", "Unsupported connection attribute"});
        return SQL_ERROR;
    }
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLGetConnectAttrW failed with exception: {}", e.what());
    conn->addDiagRecord(
        {0, "HY000", std::string("General error: ") + e.what()});
    return SQL_ERROR;
  }
}

// SQLSetConnectAttr function for setting connection attributes
SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle,
                                    SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                    SQLINTEGER StringLength) {
  MCO_LOG_DEBUG(
      "SQLSetConnectAttr called with connection handle: {}, attribute: {}",
      reinterpret_cast<void *>(ConnectionHandle), Attribute);

  (void)StringLength;  // Suppress unused parameter warning

  auto *conn = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
  if (!conn) {
    MCO_LOG_ERROR("SQLSetConnectAttr: Invalid connection handle received");
    return SQL_INVALID_HANDLE;
  }

  try {
    switch (Attribute) {
      case SQL_ATTR_AUTOCOMMIT: {
        // MaxCompute doesn't support transactions, so autocommit is always ON
        // Accept the setting but ensure it's SQL_AUTOCOMMIT_ON
        SQLUINTEGER auto_commit = (SQLUINTEGER)(intptr_t)ValuePtr;
        if (auto_commit != SQL_AUTOCOMMIT_ON) {
          MCO_LOG_WARNING(
              "SQLSetConnectAttr: Attempted to disable autocommit, but "
              "MaxCompute doesn't support transactions");
          conn->addDiagRecord({0, "01S02", "Option value changed"});
          // Accept the setting but keep autocommit on internally
        }
        MCO_LOG_DEBUG(
            "SQLSetConnectAttr: SQL_ATTR_AUTOCOMMIT set to SQL_AUTOCOMMIT_ON");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_TXN_ISOLATION: {
        // Accept any isolation level but use READ COMMITTED internally
        // Since MaxCompute doesn't support transactions, isolation levels are
        // not meaningful
        SQLUINTEGER isolation = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG(
            "SQLSetConnectAttr: SQL_ATTR_TXN_ISOLATION requested as {}, using "
            "SQL_TXN_READ_COMMITTED",
            isolation);
        return SQL_SUCCESS;
      }

      case SQL_ATTR_ANSI_APP: {
        // Accept ANSI application mode setting
        MCO_LOG_DEBUG("SQLSetConnectAttr: SQL_ATTR_ANSI_APP set");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_PACKET_SIZE: {
        // Accept packet size setting but may ignore it
        SQLUINTEGER packet_size = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG("SQLSetConnectAttr: SQL_ATTR_PACKET_SIZE set to {}",
                      packet_size);
        return SQL_SUCCESS;
      }

      case SQL_ATTR_LOGIN_TIMEOUT: {
        // Accept login timeout setting
        SQLUINTEGER timeout = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG("SQLSetConnectAttr: SQL_ATTR_LOGIN_TIMEOUT set to {}",
                      timeout);
        return SQL_SUCCESS;
      }

      case SQL_ATTR_CONNECTION_TIMEOUT: {
        // Accept connection timeout setting
        SQLUINTEGER timeout = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG(
            "SQLSetConnectAttr: SQL_ATTR_CONNECTION_TIMEOUT set to {}",
            timeout);
        return SQL_SUCCESS;
      }

      default:
        MCO_LOG_DEBUG(
            "SQLSetConnectAttr: unsupported attribute {}, accepting for "
            "compatibility",
            Attribute);
        // Accept most unsupported attributes for better application
        // compatibility
        return SQL_SUCCESS;
    }
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLSetConnectAttr failed with exception: {}", e.what());
    conn->addDiagRecord(
        {0, "HY000", std::string("General error: ") + e.what()});
    return SQL_ERROR;
  }
}

// SQLSetConnectAttrW function for setting connection attributes (wide version)
SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC ConnectionHandle,
                                     SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                     SQLINTEGER StringLength) {
  MCO_LOG_DEBUG(
      "SQLSetConnectAttrW called with connection handle: {}, attribute: {}",
      reinterpret_cast<void *>(ConnectionHandle), Attribute);

  (void)StringLength;  // Suppress unused parameter warning

  auto *conn = maxcompute_odbc::HandleRegistry::instance()
                   .get<maxcompute_odbc::ConnHandle>(ConnectionHandle);
  if (!conn) {
    MCO_LOG_ERROR("SQLSetConnectAttrW: Invalid connection handle received");
    return SQL_INVALID_HANDLE;
  }

  try {
    switch (Attribute) {
      case SQL_ATTR_AUTOCOMMIT: {
        // MaxCompute doesn't support transactions, so autocommit is always ON
        SQLUINTEGER auto_commit = (SQLUINTEGER)(intptr_t)ValuePtr;
        if (auto_commit != SQL_AUTOCOMMIT_ON) {
          MCO_LOG_WARNING(
              "SQLSetConnectAttrW: Attempted to disable autocommit, but "
              "MaxCompute doesn't support transactions");
          conn->addDiagRecord({0, "01S02", "Option value changed"});
        }
        MCO_LOG_DEBUG(
            "SQLSetConnectAttrW: SQL_ATTR_AUTOCOMMIT set to SQL_AUTOCOMMIT_ON");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_TXN_ISOLATION: {
        // Accept any isolation level but use READ COMMITTED internally
        SQLUINTEGER isolation = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG(
            "SQLSetConnectAttrW: SQL_ATTR_TXN_ISOLATION requested as {}, using "
            "SQL_TXN_READ_COMMITTED",
            isolation);
        return SQL_SUCCESS;
      }

      case SQL_ATTR_ANSI_APP: {
        // Accept ANSI application mode setting
        MCO_LOG_DEBUG("SQLSetConnectAttrW: SQL_ATTR_ANSI_APP set");
        return SQL_SUCCESS;
      }

      case SQL_ATTR_PACKET_SIZE: {
        // Accept packet size setting
        SQLUINTEGER packet_size = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG("SQLSetConnectAttrW: SQL_ATTR_PACKET_SIZE set to {}",
                      packet_size);
        return SQL_SUCCESS;
      }

      case SQL_ATTR_LOGIN_TIMEOUT: {
        // Accept login timeout setting
        SQLUINTEGER timeout = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG("SQLSetConnectAttrW: SQL_ATTR_LOGIN_TIMEOUT set to {}",
                      timeout);
        return SQL_SUCCESS;
      }

      case SQL_ATTR_CONNECTION_TIMEOUT: {
        // Accept connection timeout setting
        SQLUINTEGER timeout = (SQLUINTEGER)(intptr_t)ValuePtr;
        MCO_LOG_DEBUG(
            "SQLSetConnectAttrW: SQL_ATTR_CONNECTION_TIMEOUT set to {}",
            timeout);
        return SQL_SUCCESS;
      }

      default:
        MCO_LOG_DEBUG(
            "SQLSetConnectAttrW: unsupported attribute {}, accepting for "
            "compatibility",
            Attribute);
        // Accept most unsupported attributes for better application
        // compatibility
        return SQL_SUCCESS;
    }
  } catch (const std::exception &e) {
    MCO_LOG_ERROR("SQLSetConnectAttrW failed with exception: {}", e.what());
    conn->addDiagRecord(
        {0, "HY000", std::string("General error: ") + e.what()});
    return SQL_ERROR;
  }
}