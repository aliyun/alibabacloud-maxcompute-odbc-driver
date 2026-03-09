#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/odbc_api/conversions.h"
#include "maxcompute_odbc/odbc_api/handles.h"

namespace maxcompute_odbc {

SQLRETURN ConnHandle::connect(const std::string &dsn, const std::string &user,
                              const std::string &pass) {
  MCO_LOG_DEBUG("Attempting connection with input string: '{}' and user: '{}'",
                dsn, user);

  try {
    // 1. 约定：如果 dsn 含有 '='，视为完整连接字符串；
    //    否则视为 DSN 名，只作为一个普通参数传入，不在 driver 内部展开。
    std::string conn_str = dsn;

    if (dsn.find('=') == std::string::npos) {
      // 这里我们不再尝试解析 ODBC.INI 或注册表。
      // 让上层负责把 DSN 展开为完整的 connection string。
      MCO_LOG_WARNING(
          "Received DSN name '{}' without explicit connection parameters. "
          "Driver will treat it as a raw connection string. "
          "It need to pass a full DSN-less connection string "
          "(e.g. 'Endpoint=...;Project=...;Uid=...;Pwd=...;').",
          dsn);
      // 在这里直接返回 IM002
      addDiagRecord(
          {0, "IM002", "Data source name not found and no default specified"});
      return SQL_ERROR;
    } else {
      MCO_LOG_DEBUG("Input appears to be a DSN-less connection string.");
    }

    // 2. 解析连接字符串
    MCO_LOG_DEBUG("Parsing connection string: '{}'", conn_str);
    m_config = ConnectionStringParser::parse(conn_str);

    // 3. 覆盖用户/密码（如果上层通过 SQLConnect/SQLDriverConnect 传了
    // user/pass）
    if (!user.empty()) {
      m_config.accessKeyId = user;
      MCO_LOG_DEBUG("Setting access key ID from user parameter");
    }
    if (!pass.empty()) {
      m_config.accessKeySecret = pass;
      MCO_LOG_DEBUG("Setting access key secret from password parameter");
    }

    // 4. 打日志（不包含敏感信息）
    MCO_LOG_DEBUG("Using project: '{}', endpoint: '{}'", m_config.project,
                  m_config.endpoint);
    if (m_config.accessKeyId) {
      MCO_LOG_DEBUG(
          "Using access key ID: '{}****'",
          m_config.accessKeyId.value().substr(
              0, std::min<size_t>(4, m_config.accessKeyId.value().size())));
    }

    // 5. 校验配置
    MCO_LOG_DEBUG("Validating connection configuration");
    m_config.validate();

    // 6. 创建 SDK 客户端并建立连接
    MCO_LOG_DEBUG("Creating MaxCompute client with parsed configuration");
    m_sdk = std::make_unique<MaxComputeClient>(m_config);

    // 7. 如果启用了交互模式，调用 getConnection API 获取 MaxQA Session ID
    if (m_config.interactiveMode) {
      MCO_LOG_INFO("Interactive mode enabled, calling getConnection API");
      auto session_id_result = m_sdk->getConnection();
      if (!session_id_result.has_value()) {
        std::string error_msg = "Failed to get MaxQA connection: " +
                                session_id_result.error().message;
        addDiagRecord({0, "08001", error_msg});
        MCO_LOG_ERROR("getConnection failed: {}", error_msg);
        return SQL_ERROR;
      }
      m_maxQASessionID = session_id_result.value();
      MCO_LOG_INFO("MaxQA session established: {}", m_maxQASessionID);
    }

    MCO_LOG_INFO("Successfully connected to MaxCompute project: '{}'",
                 m_config.project);
    return SQL_SUCCESS;

  } catch (const std::exception &e) {
    std::string error_msg = std::string("Connection failed: ") + e.what();
    addDiagRecord({0, "08001", error_msg});
    MCO_LOG_ERROR("Connection failure: {}", e.what());

    // 记录更多诊断信息（不包含敏感信息）
    MCO_LOG_ERROR("Connection configuration details:");
    MCO_LOG_ERROR("  Input DSN/conn string: '{}'", dsn);
    MCO_LOG_ERROR("  User: '{}'", user);
    MCO_LOG_ERROR("  Project: '{}'", m_config.project);
    MCO_LOG_ERROR("  Endpoint: '{}'", m_config.endpoint);
    MCO_LOG_ERROR("  Access key ID set: {}", m_config.accessKeyId.has_value());
    MCO_LOG_ERROR("  Access key secret set: {}",
                  m_config.accessKeySecret.has_value());

    return SQL_ERROR;
  }
}

SQLRETURN StmtHandle::prepare(const std::string &sql) {
  MCO_LOG_DEBUG("Preparing statement: {}", sql);
  // Store the prepared SQL statement
  m_prepared_sql = sql;
  MCO_LOG_DEBUG("Statement prepared successfully.");
  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::execute() {
  if (m_prepared_sql.empty()) {
    addDiagRecord(
        {0, "HY010", "Function sequence error. Call SQLPrepare first."});
    return SQL_ERROR;
  }

  if (!m_parent_conn || !m_parent_conn->getSDK()) {
    addDiagRecord({0, "08003", "Connection not open"});
    MCO_LOG_ERROR(
        "Connection not established when trying to execute prepared "
        "statement.");
    return SQL_ERROR;
  }

  MCO_LOG_DEBUG("Executing prepared statement: {}", m_prepared_sql);
  auto result = m_parent_conn->getSDK()->executeQuery(m_prepared_sql);
  if (!result.has_value()) {
    addDiagRecord(
        {(SQLINTEGER)result.error().code, "HY000", result.error().message});
    MCO_LOG_ERROR("executeQuery failed: {}", result.error().message);
    return SQL_ERROR;
  }

  m_result_stream = std::move(result.value());
  MCO_LOG_DEBUG("Query submitted, stream handle obtained.");
  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::executeDirect(const std::string &sql) {
  if (!m_parent_conn || !m_parent_conn->getSDK()) {
    addDiagRecord({0, "08003", "Connection not open"});
    MCO_LOG_ERROR(
        "Connection not established when trying to execute direct statement.");
    return SQL_ERROR;
  }

  // Handle empty query - return success with no result set
  if (sql.empty() || sql.find_first_not_of(" \t\r\n") == std::string::npos) {
    MCO_LOG_DEBUG("Empty query received, returning success with no results");
    // Reset result stream to indicate no results
    m_result_stream.reset();
    return SQL_SUCCESS;
  }

  MCO_LOG_DEBUG("Executing direct: {}", sql);
  auto result = m_parent_conn->getSDK()->executeQuery(sql);

  if (!result.has_value()) {
    addDiagRecord(
        {(SQLINTEGER)result.error().code, "HY000", result.error().message});
    MCO_LOG_ERROR("executeQuery failed: {}", result.error().message);
    return SQL_ERROR;
  }

  m_result_stream = std::move(result.value());
  MCO_LOG_DEBUG("Query submitted, stream handle obtained.");
  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::getNumResultCols(SQLSMALLINT *count) {
  // Handle case where there's no result stream (e.g., empty query)
  if (!m_result_stream) {
    *count = 0;
    return SQL_SUCCESS;
  }
  auto result = m_result_stream->getSchema();
  if (!result.has_value()) {
    addDiagRecord({0, "00000", result.error().message});
    return SQL_ERROR;
  }
  auto &schema = result.value();
  *count = static_cast<SQLSMALLINT>(schema->getColumnCount());
  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::describeCol(SQLUSMALLINT col_num, SQLCHAR *col_name_buf,
                                  SQLSMALLINT buf_len,
                                  SQLSMALLINT *name_len_ptr,
                                  SQLSMALLINT *data_type_ptr,
                                  SQLULEN *col_size_ptr,
                                  SQLSMALLINT *decimal_digits_ptr,
                                  SQLSMALLINT *nullable_ptr) {
  if (!m_result_stream) {
    addDiagRecord(
        {0, "HY010", "Function sequence error. Call SQLExecDirect first."});
    return SQL_ERROR;
  }

  auto result = m_result_stream->getSchema();
  if (!result.has_value()) {
    addDiagRecord({0, "00000", result.error().message});
  }
  auto &schema = result.value();
  if (col_num == 0 || col_num > schema->getColumnCount()) {
    addDiagRecord({0, "07009", "Invalid descriptor index."});
    return SQL_ERROR;
  }

  // ODBC列号从1开始，我们的vector索引从0开始
  const auto &col_schema = schema->getColumn(col_num - 1);
  auto odbc_column = convertColumn(col_schema);
  if (!odbc_column.has_value()) {
    addDiagRecord({0, "00000", odbc_column.error().message});
    return SQL_ERROR;
  }
  auto &col = odbc_column.value();

  // 填充列名
  if (col_name_buf) {
    // 安全地拷贝字符串
    size_t len_to_copy =
        std::min(col_schema.name.length(), static_cast<size_t>(buf_len - 1));
    memcpy(col_name_buf, col_schema.name.c_str(), len_to_copy);
    col_name_buf[len_to_copy] = '\0';
  }
  if (name_len_ptr) {
    *name_len_ptr = static_cast<SQLSMALLINT>(col_schema.name.length());
  }

  // 填充类型信息
  if (data_type_ptr) *data_type_ptr = col->sql_type;
  if (col_size_ptr) *col_size_ptr = col->column_size;
  if (decimal_digits_ptr) *decimal_digits_ptr = col->decimal_digits;
  if (nullable_ptr) *nullable_ptr = col->nullable;

  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::bindCol(SQLUSMALLINT col_num, SQLSMALLINT target_type,
                              SQLPOINTER target_buf, SQLLEN buf_len,
                              SQLLEN *indicator) {
  if (!m_result_stream) {
    // 理论上可以在 SQLExecDirect 之前绑定，但为了简化，我们要求在之后
    addDiagRecord({0, "HY010", "Function sequence error."});
    return SQL_ERROR;
  }

  // 确保绑定向量有足够的空间
  if (col_num > m_bindings.size()) {
    // 将向量大小调整为列数，所有未绑定的列将保持默认构造状态
    auto result = m_result_stream->getSchema();
    if (!result.has_value()) {
      addDiagRecord({0, "00000", result.error().message});
    }
    auto &schema = result.value();
    m_bindings.resize(schema->getColumnCount());
  }

  if (col_num == 0) {
    addDiagRecord({0, "07009", "Invalid descriptor index."});
    return SQL_ERROR;
  }

  // 存储绑定信息。ODBC列号从1开始，vector索引从0开始。
  m_bindings[col_num - 1] = {target_type, target_buf, buf_len, indicator};

  MCO_LOG_DEBUG("Column {} bound to target type {}", col_num, target_type);
  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::fetch() {
  if (!m_result_stream) {
    addDiagRecord({0, "HY010", "Function sequence error"});
    return SQL_ERROR;
  }

  if (m_end_of_stream) {
    return SQL_NO_DATA;
  }

  // 获取下一行数据
  auto result = m_result_stream->fetchNextRow();

  if (!result.has_value()) {
    addDiagRecord(
        {(SQLINTEGER)result.error().code, "HYT00", result.error().message});
    return SQL_ERROR;
  }

  m_current_row = std::move(result.value());

  if (!m_current_row.has_value()) {
    MCO_LOG_DEBUG("End of stream reached.");
    m_end_of_stream = true;
    return SQL_NO_DATA;
  }

  MCO_LOG_DEBUG("New row received with {} columns.",
                m_current_row->values.size());

  // 确保绑定向量的大小与列数一致
  if (m_bindings.size() != m_current_row->values.size()) {
    m_bindings.resize(m_current_row->values.size());
  }

  // 遍历所有列，对已绑定的列进行数据转换
  for (size_t i = 0; i < m_current_row->values.size(); ++i) {
    const auto &binding = m_bindings[i];
    if (binding.target_buffer == nullptr) {
      continue;  // 该列未绑定，跳过
    }

    const auto &column_data = m_current_row->values[i];
    SQLRETURN conv_ret = convertAndWrite(column_data, binding);
    if (conv_ret != SQL_SUCCESS) {
      // 在 convertAndWrite 中应添加诊断记录
      return SQL_ERROR;
    }
  }

  // 增加已获取行数计数
  m_fetched_rows++;

  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::getRowCount(SQLLEN *row_count) {
  // SQLRowCount 对SELECT语句返回的是已获取(已fetch)的行数, 而非总行数
  if (row_count == nullptr) {
    addDiagRecord({0, "HY009", "Invalid use of null pointer"});
    return SQL_ERROR;
  }

  *row_count = m_fetched_rows;
  return SQL_SUCCESS;
}

SQLRETURN StmtHandle::getColAttribute(SQLUSMALLINT column_number,
                                      SQLUSMALLINT field_identifier,
                                      SQLPOINTER character_attribute_ptr,
                                      SQLSMALLINT buffer_length,
                                      SQLSMALLINT *string_length_ptr,
                                      SQLLEN *numeric_attribute_ptr,
                                      bool is_wide) {
  if (!m_result_stream) {
    addDiagRecord({0, "HY010", "Function sequence error"});
    return SQL_ERROR;
  }

  auto result = m_result_stream->getSchema();
  if (!result.has_value()) {
    addDiagRecord({0, "00000", result.error().message});
  }
  auto &schema = result.value();
  if (column_number == 0 || column_number > schema->getColumnCount()) {
    addDiagRecord({0, "07009", "Invalid descriptor index"});
    return SQL_ERROR;
  }

  // ODBC列号从1开始，我们vector索引从0开始
  const auto &col_schema = schema->getColumn(column_number - 1);
  auto odbc_column = convertColumn(col_schema);
  if (!odbc_column.has_value()) {
    addDiagRecord({0, "00000", odbc_column.error().message});
    return SQL_ERROR;
  }
  auto &col = odbc_column.value();

  // 处理不同的字段标识符
  // need support 2, 1011, 1008, 14, 1003, 32, 1006, 1013
  switch (field_identifier) {
    case SQL_DESC_NAME:
    case SQL_COLUMN_NAME:
      // 返回列名
      if (character_attribute_ptr && buffer_length > 0) {
        if (is_wide) {
          // Wide character version - convert to UTF-16
          SQLWCHAR *dest = static_cast<SQLWCHAR *>(character_attribute_ptr);
          size_t max_chars =
              static_cast<size_t>(buffer_length) / sizeof(SQLWCHAR) - 1;
          size_t out_pos = 0;
          const char *src = col_schema.name.c_str();
          size_t src_len = col_schema.name.length();

          for (size_t i = 0; i < src_len && out_pos < max_chars; ++i) {
            unsigned char c = static_cast<unsigned char>(src[i]);
            uint32_t codepoint;

            if (c < 0x80) {
              codepoint = c;
            } else if ((c & 0xE0) == 0xC0) {
              if (i + 1 >= src_len) break;
              codepoint = ((c & 0x1F) << 6) |
                          (static_cast<unsigned char>(src[i + 1]) & 0x3F);
              i += 1;
            } else if ((c & 0xF0) == 0xE0) {
              if (i + 2 >= src_len) break;
              codepoint =
                  ((c & 0x0F) << 12) |
                  ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(src[i + 2]) & 0x3F);
              i += 2;
            } else if ((c & 0xF8) == 0xF0) {
              if (i + 3 >= src_len || out_pos + 1 >= max_chars) break;
              codepoint =
                  ((c & 0x07) << 18) |
                  ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 12) |
                  ((static_cast<unsigned char>(src[i + 2]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(src[i + 3]) & 0x3F);
              i += 3;
              codepoint -= 0x10000;
              dest[out_pos++] =
                  static_cast<SQLWCHAR>(0xD800 | (codepoint >> 10));
              dest[out_pos++] =
                  static_cast<SQLWCHAR>(0xDC00 | (codepoint & 0x3FF));
              continue;
            } else {
              continue;
            }
            dest[out_pos++] = static_cast<SQLWCHAR>(codepoint);
          }
          dest[out_pos] = 0;
          if (string_length_ptr) {
            // 返回字符数，而不是字节数
            *string_length_ptr = static_cast<SQLSMALLINT>(out_pos);
          }
        } else {
          // Narrow character version
          size_t len_to_copy = std::min(col_schema.name.length(),
                                        static_cast<size_t>(buffer_length - 1));
          memcpy(character_attribute_ptr, col_schema.name.c_str(), len_to_copy);
          reinterpret_cast<char *>(character_attribute_ptr)[len_to_copy] = '\0';
          if (string_length_ptr) {
            *string_length_ptr =
                static_cast<SQLSMALLINT>(col_schema.name.length());
          }
        }
      } else if (string_length_ptr) {
        *string_length_ptr = static_cast<SQLSMALLINT>(col_schema.name.length());
      }
      return SQL_SUCCESS;

    case SQL_DESC_LABEL:
      // 返回列标签（通常和列名相同）
      if (character_attribute_ptr && buffer_length > 0) {
        if (is_wide) {
          // Wide character version - convert to UTF-16
          SQLWCHAR *dest = static_cast<SQLWCHAR *>(character_attribute_ptr);
          size_t max_chars =
              static_cast<size_t>(buffer_length) / sizeof(SQLWCHAR) - 1;
          size_t out_pos = 0;
          const char *src = col_schema.name.c_str();
          size_t src_len = col_schema.name.length();

          for (size_t i = 0; i < src_len && out_pos < max_chars; ++i) {
            unsigned char c = static_cast<unsigned char>(src[i]);
            uint32_t codepoint;

            if (c < 0x80) {
              codepoint = c;
            } else if ((c & 0xE0) == 0xC0) {
              if (i + 1 >= src_len) break;
              codepoint = ((c & 0x1F) << 6) |
                          (static_cast<unsigned char>(src[i + 1]) & 0x3F);
              i += 1;
            } else if ((c & 0xF0) == 0xE0) {
              if (i + 2 >= src_len) break;
              codepoint =
                  ((c & 0x0F) << 12) |
                  ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(src[i + 2]) & 0x3F);
              i += 2;
            } else if ((c & 0xF8) == 0xF0) {
              if (i + 3 >= src_len || out_pos + 1 >= max_chars) break;
              codepoint =
                  ((c & 0x07) << 18) |
                  ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 12) |
                  ((static_cast<unsigned char>(src[i + 2]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(src[i + 3]) & 0x3F);
              i += 3;
              codepoint -= 0x10000;
              dest[out_pos++] =
                  static_cast<SQLWCHAR>(0xD800 | (codepoint >> 10));
              dest[out_pos++] =
                  static_cast<SQLWCHAR>(0xDC00 | (codepoint & 0x3FF));
              continue;
            } else {
              continue;
            }
            dest[out_pos++] = static_cast<SQLWCHAR>(codepoint);
          }
          dest[out_pos] = 0;
          if (string_length_ptr) {
            // 返回字符数，而不是字节数
            *string_length_ptr = static_cast<SQLSMALLINT>(out_pos);
          }
        } else {
          // Narrow character version
          size_t len_to_copy = std::min(col_schema.name.length(),
                                        static_cast<size_t>(buffer_length - 1));
          memcpy(character_attribute_ptr, col_schema.name.c_str(), len_to_copy);
          reinterpret_cast<char *>(character_attribute_ptr)[len_to_copy] = '\0';
          if (string_length_ptr) {
            *string_length_ptr =
                static_cast<SQLSMALLINT>(col_schema.name.length());
          }
        }
      } else if (string_length_ptr) {
        *string_length_ptr = static_cast<SQLSMALLINT>(col_schema.name.length());
      }
      return SQL_SUCCESS;

    case SQL_DESC_TYPE:
    case SQL_COLUMN_TYPE:
      // 返回SQL数据类型
      if (numeric_attribute_ptr) {
        *numeric_attribute_ptr = col->sql_type;
      }
      return SQL_SUCCESS;

    case SQL_COLUMN_LENGTH:
    case SQL_DESC_LENGTH:
      // 返回列长度
      if (numeric_attribute_ptr) {
        *numeric_attribute_ptr = col->column_size;
      }
      return SQL_SUCCESS;

    case SQL_DESC_PRECISION:
      // 返回精度
      if (numeric_attribute_ptr) {
        *numeric_attribute_ptr = col->column_size;
      }
      return SQL_SUCCESS;

    case SQL_DESC_SCALE:
    case SQL_COLUMN_SCALE:
      // 返回小数位数
      if (numeric_attribute_ptr) {
        *numeric_attribute_ptr = col->decimal_digits;
      }
      return SQL_SUCCESS;

    case SQL_DESC_NULLABLE:
    case SQL_COLUMN_NULLABLE:
      // 返回是否可空
      if (numeric_attribute_ptr) {
        *numeric_attribute_ptr = col->nullable;
      }
      return SQL_SUCCESS;

    case SQL_DESC_TYPE_NAME:
      // 返回数据类型的名称
      if (character_attribute_ptr && buffer_length > 0) {
        // Convert SQL type to string representation
        std::string type_name;
        switch (col->sql_type) {
          case SQL_CHAR:
            type_name = "CHAR";
            break;
          case SQL_VARCHAR:
            type_name = "VARCHAR";
            break;
          case SQL_LONGVARCHAR:
            type_name = "LONGVARCHAR";
            break;
          case SQL_WCHAR:
            type_name = "WCHAR";
            break;
          case SQL_WVARCHAR:
            type_name = "WVARCHAR";
            break;
          case SQL_WLONGVARCHAR:
            type_name = "WLONGVARCHAR";
            break;
          case SQL_DECIMAL:
            type_name = "DECIMAL";
            break;
          case SQL_NUMERIC:
            type_name = "NUMERIC";
            break;
          case SQL_SMALLINT:
            type_name = "SMALLINT";
            break;
          case SQL_INTEGER:
            type_name = "INTEGER";
            break;
          case SQL_REAL:
            type_name = "REAL";
            break;
          case SQL_FLOAT:
            type_name = "FLOAT";
            break;
          case SQL_DOUBLE:
            type_name = "DOUBLE";
            break;
          case SQL_BIT:
            type_name = "BIT";
            break;
          case SQL_TINYINT:
            type_name = "TINYINT";
            break;
          case SQL_BIGINT:
            type_name = "BIGINT";
            break;
          case SQL_BINARY:
            type_name = "BINARY";
            break;
          case SQL_VARBINARY:
            type_name = "VARBINARY";
            break;
          case SQL_LONGVARBINARY:
            type_name = "LONGVARBINARY";
            break;
          case SQL_TYPE_DATE:
            type_name = "DATE";
            break;
          case SQL_TYPE_TIME:
            type_name = "TIME";
            break;
          case SQL_TYPE_TIMESTAMP:
            type_name = "TIMESTAMP";
            break;
          case SQL_GUID:
            type_name = "GUID";
            break;
          default:
            type_name = "UNKNOWN";
            break;
        }
        size_t len_to_copy = std::min(type_name.length(),
                                      static_cast<size_t>(buffer_length - 1));
        memcpy(character_attribute_ptr, type_name.c_str(), len_to_copy);
        reinterpret_cast<char *>(character_attribute_ptr)[len_to_copy] = '\0';
      }
      if (string_length_ptr) {
        // Get the correct length for column type name by referring the relevant
        // part from colSchema
        *string_length_ptr =
            static_cast<SQLSMALLINT>(col_schema.type_info->toString().length());
      }
      return SQL_SUCCESS;

    case SQL_DESC_OCTET_LENGTH:
      // 返回列的字节长度 - This refers to the max number of bytes required to
      // store data
      if (numeric_attribute_ptr) {
        *numeric_attribute_ptr = col->octet_length;
      }
      return SQL_SUCCESS;

    case SQL_DESC_UNSIGNED:
      // 返回列是否为无符号类型
      if (numeric_attribute_ptr) {
        // 对于 MaxCompute 的常见数据类型，检查是否有符号
        // INT、BIGINT、DECIMAL 等通常是带符号的，返回SQL_FALSE
        // 对于无符号类型可以返回SQL_TRUE，这里我们暂时为常见类型返回相应的值
        bool is_unsigned = false;

        switch (col->sql_type) {
          case SQL_SMALLINT:
          case SQL_INTEGER:
          case SQL_BIGINT:
          case SQL_TINYINT:
          case SQL_NUMERIC:
          case SQL_DECIMAL:
          case SQL_FLOAT:
          case SQL_REAL:
          case SQL_DOUBLE:
            // 这些是带符号类型的例子，对于MaxCompute默认为带符号
            is_unsigned = false;
            break;
          // 如果有任何无符号类型，可以在此添加case，并将is_unsigned设为true
          default:
            is_unsigned = false;
            break;
        }
        *numeric_attribute_ptr = is_unsigned ? SQL_TRUE : SQL_FALSE;
      }
      return SQL_SUCCESS;

    case 32:  // This might be SQL_DESC_BASE_COLUMN_NAME or a driver-specific
              // field
      // For Power BI compatibility, return column name if requested for field
      // 32
      if (character_attribute_ptr && buffer_length > 0) {
        size_t len_to_copy = std::min(col_schema.name.length(),
                                      static_cast<size_t>(buffer_length - 1));
        memcpy(character_attribute_ptr, col_schema.name.c_str(), len_to_copy);
        reinterpret_cast<char *>(character_attribute_ptr)[len_to_copy] = '\0';
      }
      if (string_length_ptr) {
        *string_length_ptr = static_cast<SQLSMALLINT>(col_schema.name.length());
      }
      return SQL_SUCCESS;
    default:
      // 对于不支持的字段，返回错误
      addDiagRecord({0, "HY091", "Invalid descriptor identifier"});
      return SQL_ERROR;
  }
}

SQLRETURN StmtHandle::getData(SQLUSMALLINT col_num, SQLSMALLINT target_type,
                              SQLPOINTER target_buf, SQLLEN buf_len,
                              SQLLEN *indicator) {
  if (!m_result_stream) {
    addDiagRecord({0, "HY010", "Function sequence error"});
    return SQL_ERROR;
  }

  if (!m_current_row.has_value()) {
    addDiagRecord({0, "HY010", "Function sequence error - no current row"});
    return SQL_ERROR;
  }

  auto result = m_result_stream->getSchema();
  if (!result.has_value()) {
    addDiagRecord({0, "00000", result.error().message});
  }
  auto &schema = result.value();
  if (col_num == 0 || col_num > schema->getColumnCount()) {
    addDiagRecord({0, "07009", "Invalid descriptor index"});
    return SQL_ERROR;
  }

  if (col_num > m_current_row->values.size()) {
    addDiagRecord(
        {0, "07009",
         "Invalid descriptor index - column number exceeds row size"});
    return SQL_ERROR;
  }

  // ODBC列号从1开始，我们的vector索引从0开始
  const auto &column_data = m_current_row->values[col_num - 1];

  // 创建一个 ColumnBinding 临时结构来利用 convertAndWrite 函数
  ColumnBinding binding{target_type, target_buf, buf_len, indicator};

  // 使用现有的数据转换功能
  SQLRETURN conv_ret = convertAndWrite(column_data, binding);
  if (conv_ret != SQL_SUCCESS) {
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

// Implementation of helper functions in StmtHandle.cpp

/**
 * @brief Matches a string against an SQL LIKE pattern.
 * @param str The string to test.
 * @param pattern The SQL LIKE pattern with '%' and '_'.
 * @return True if the string matches the pattern.
 *
 * This implementation converts the SQL LIKE pattern to a regex pattern.
 */
bool StmtHandle::sqlLikeMatch(const std::string &str,
                              const std::string &pattern) {
  if (pattern.empty() || pattern == "%") {
    return true;  // Empty or '%' pattern matches everything.
  }

  std::string regex_pattern;
  regex_pattern.reserve(pattern.length() * 2);

  for (char c : pattern) {
    switch (c) {
      case '%':
        regex_pattern += ".*";
        break;
      case '_':
        regex_pattern += ".";
        break;
      // Escape special regex characters
      case '.':
      case '\\':
      case '+':
      case '*':
      case '?':
      case '[':
      case ']':
      case '^':
      case '$':
      case '(':
      case ')':
      case '{':
      case '}':
      case '=':
      case '!':
      case '|':
      case ':':
      case '-':
        regex_pattern += '\\';
        regex_pattern += c;
        break;
      default:
        regex_pattern += c;
        break;
    }
  }

  try {
    std::regex re(regex_pattern,
                  std::regex::icase);  // ODBC patterns are case-insensitive
    return std::regex_match(str, re);
  } catch (const std::regex_error &e) {
    MCO_LOG_WARNING(
        "Invalid regex pattern generated from SQL LIKE pattern '{}': {}",
        pattern, e.what());
    // Fallback to simple equality check if pattern is not a valid regex
    return str == pattern;
  }
}

/**
 * @brief Fetches tables for a given schema, filters them, and adds them to the
 * result set builder.
 * @param builder The result set builder.
 * @param catalogName The name of the catalog (project) to be added to each row.
 * @param schemaName The specific schema to list tables from.
 * @param tablePattern The SQL LIKE pattern to filter table names.
 * @param requestedTypes A parsed list of uppercase table types to filter by.
 * @return SQL_SUCCESS on success, SQL_ERROR on failure.
 */
SQLRETURN StmtHandle::fetchAndAddTablesForSchema(
    StaticResultSetBuilder &builder, const std::string &catalogName,
    const std::string &schemaName, const std::string &tablePattern,
    const std::vector<std::string> &requestedTypes) {
  auto result = m_parent_conn->getSDK()->listTables(schemaName);
  if (!result.has_value()) {
    addDiagRecord(
        {(SQLINTEGER)result.error().code, "HY000", result.error().message});
    MCO_LOG_ERROR("listTables for schema '{}' failed: {}", schemaName,
                  result.error().message);
    return SQL_ERROR;
  }

  bool typeFilterExists =
      !requestedTypes.empty() &&
      !(requestedTypes.size() == 1 && requestedTypes[0] == "%");

  for (const auto &table : result.value()) {
    // 1. Filter by table name pattern
    if (!sqlLikeMatch(table.name, tablePattern)) {
      continue;
    }

    // 2. Filter by table type
    std::string tableType = "TABLE";  // MaxCompute tables are generally "TABLE"
    if (typeFilterExists) {
      bool typeMatch = false;
      for (const auto &reqType : requestedTypes) {
        if (reqType == tableType) {
          typeMatch = true;
          break;
        }
      }
      if (!typeMatch) {
        continue;
      }
    }

    // 3. Add row to result set if all filters pass
    builder.addRowValues(catalogName, schemaName, table.name, tableType, "");
  }

  return SQL_SUCCESS;
}

// see
// https://learn.microsoft.com/zh-cn/sql/odbc/reference/syntax/sqltables-function?view=sql-server-ver17
SQLRETURN StmtHandle::tables(const std::string &catalog,
                             const std::string &schema,
                             const std::string &table,
                             const std::string &type) {
  MCO_LOG_DEBUG(
      "SQLTables called with catalog='{}', schema='{}', table='{}', type='{}'",
      catalog, schema, table, type);

  if (!m_parent_conn || !m_parent_conn->getSDK()) {
    addDiagRecord({0, "08003", "Connection not open"});
    return SQL_ERROR;
  }

  // Per ODBC spec, catalog parameter is a search pattern. We only support
  // current project.
  const auto &projectName = m_parent_conn->getConfigForUpdate().project;
  if (!catalog.empty() && !sqlLikeMatch(projectName, catalog)) {
    // Catalog pattern doesn't match our current project, return empty result.
    auto emptyBuilder = std::make_unique<StaticResultSetBuilder>();
    emptyBuilder->addColumn("TABLE_CAT", PrimitiveTypeInfo(OdpsType::STRING))
        .addColumn("TABLE_SCHEM", PrimitiveTypeInfo(OdpsType::STRING))
        .addColumn("TABLE_NAME", PrimitiveTypeInfo(OdpsType::STRING))
        .addColumn("TABLE_TYPE", PrimitiveTypeInfo(OdpsType::STRING))
        .addColumn("REMARKS", PrimitiveTypeInfo(OdpsType::STRING));
    m_result_stream = std::move(emptyBuilder->build());
    return SQL_SUCCESS;
  }

  // --- Handle metadata-only requests first ---
  if (table == "%" && schema == SQL_ALL_SCHEMAS && type.empty() &&
      catalog == SQL_ALL_CATALOGS) {
    // Special case: List all catalogs (projects)
    auto builder = std::make_unique<StaticResultSetBuilder>();
    builder->addColumn("TABLE_CAT", PrimitiveTypeInfo(OdpsType::STRING));
    builder->addRowValues(projectName);
    m_result_stream = std::move(builder->build());
    return SQL_SUCCESS;
  }

  auto builder = std::make_unique<StaticResultSetBuilder>();
  builder->addColumn("TABLE_CAT", PrimitiveTypeInfo(OdpsType::STRING))
      .addColumn("TABLE_SCHEM", PrimitiveTypeInfo(OdpsType::STRING))
      .addColumn("TABLE_NAME", PrimitiveTypeInfo(OdpsType::STRING))
      .addColumn("TABLE_TYPE", PrimitiveTypeInfo(OdpsType::STRING))
      .addColumn("REMARKS", PrimitiveTypeInfo(OdpsType::STRING));

  try {
    // --- Determine which schemas to query ---
    std::vector<std::string> schemasToQuery;
    bool schemaIsPattern = (schema.find('%') != std::string::npos ||
                            schema.find('_') != std::string::npos);

    if (schema.empty() || schema == "%" || schema == SQL_ALL_SCHEMAS ||
        schemaIsPattern) {
      // Request for all or a pattern of schemas, so we must list them all from
      // the API.
      auto schemasResult = m_parent_conn->getSDK()->listSchemas();
      if (!schemasResult.has_value()) {
        addDiagRecord({(SQLINTEGER)schemasResult.error().code, "HY000",
                       schemasResult.error().message});
        MCO_LOG_ERROR("listSchemas failed: {}", schemasResult.error().message);
        return SQL_ERROR;
      }

      for (const auto &s : schemasResult.value()) {
        // If schema param was a pattern, filter against it.
        if (schemaIsPattern && !sqlLikeMatch(s, schema)) {
          continue;
        }
        schemasToQuery.push_back(s);
      }
    } else {
      // Request for a single, specific schema.
      schemasToQuery.push_back(schema);
    }

    // --- Parse requested table types ---
    std::vector<std::string> requestedTypes;
    if (!type.empty()) {
      std::istringstream typeStream(type);
      std::string singleType;
      while (std::getline(typeStream, singleType, ',')) {
        // Trim whitespace
        singleType.erase(0, singleType.find_first_not_of(" \t'\""));
        singleType.erase(singleType.find_last_not_of(" \t'\"") + 1);
        if (!singleType.empty()) {
          std::transform(singleType.begin(), singleType.end(),
                         singleType.begin(), ::toupper);
          requestedTypes.push_back(singleType);
        }
      }
    }

    // --- Iterate through schemas and fetch/filter tables ---
    for (const auto &schemaName : schemasToQuery) {
      SQLRETURN status = fetchAndAddTablesForSchema(
          *builder, projectName, schemaName, table, requestedTypes);
      if (status == SQL_ERROR) {
        // Diagnostic record is already added in the helper function.
        return SQL_ERROR;
      }
    }

    m_result_stream = std::move(builder->build());
    return SQL_SUCCESS;

  } catch (const std::exception &e) {
    addDiagRecord({0, "HY000", e.what()});
    MCO_LOG_ERROR("SQLTables failed with exception: {}", e.what());
    return SQL_ERROR;
  }
}

SQLRETURN StmtHandle::columns(const std::string &catalog,
                              const std::string &schema,
                              const std::string &table,
                              const std::string &column_pattern) {
  MCO_LOG_DEBUG(
      "SQLColumns called with catalog='{}', schema='{}', table='{}', "
      "column='{}'",
      catalog, schema, table, column_pattern);
  m_result_stream.reset();

  if (!m_parent_conn || !m_parent_conn->getSDK()) {
    addDiagRecord({0, "08003", "Connection not open"});
    MCO_LOG_ERROR("Connection not established when calling SQLColumns.");
    return SQL_ERROR;
  }

  try {
    // In MaxCompute, the typical command to describe a table is "DESCRIBE
    // table_name" This shows the structure of the table, similar to "DESCRIBE"
    // in other databases
    if (table.empty()) {
      addDiagRecord({0, "HY000", "Table name is required for SQLColumns call"});
      MCO_LOG_ERROR("Table name required for SQLColumns call");
      return SQL_ERROR;
    }

    std::string schemaName = "default";
    if (!schema.empty() && schema != "%") {
      // Include schema name to fully qualify the table
      schemaName = schema;
    }

    // Submit the query to the SDK
    auto table_result = m_parent_conn->getSDK()->getTable(schemaName, table);
    if (!table_result.has_value()) {
      addDiagRecord({(SQLINTEGER)table_result.error().code, "HY000",
                     table_result.error().message});
      MCO_LOG_ERROR("Get table failed: {}", table_result.error().message);
      return SQL_ERROR;
    }

    Table &fetched_table = table_result.value();

    auto stream_result = convertTable(fetched_table, column_pattern);

    // 3. 处理转换结果
    if (!stream_result.has_value()) {
      // 如果转换本身失败（比如内部逻辑错误），报告为 SQL_ERROR
      addDiagRecord({(SQLINTEGER)stream_result.error().code, "HY000",
                     stream_result.error().message});
      MCO_LOG_ERROR("Failed to convert table metadata to ODBC result set: {}",
                    stream_result.error().message);
      return SQL_ERROR;
    }
    m_result_stream = std::move(stream_result.value());
    return SQL_SUCCESS;
  } catch (const std::exception &e) {
    addDiagRecord({0, "HY000", e.what()});
    MCO_LOG_ERROR("SQLColumns failed: {}", e.what());
    return SQL_ERROR;
  }
}

SQLRETURN ConnHandle::disconnect() {
  MCO_LOG_DEBUG("Disconnecting from MaxCompute project: '{}'",
                m_config.project);

  // Reset the SDK to close the connection
  m_sdk.reset();

  MCO_LOG_INFO("Successfully disconnected from MaxCompute project: '{}'",
               m_config.project);
  return SQL_SUCCESS;
}

}  // namespace maxcompute_odbc
