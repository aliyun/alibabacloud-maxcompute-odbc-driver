#pragma once
#include "maxcompute_odbc/config/config.h"
#include "maxcompute_odbc/maxcompute_client/client.h"
#include "maxcompute_odbc/platform.h"  // Must include before sql.h on Windows
#include <memory>
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <vector>

namespace maxcompute_odbc {

// 诊断信息记录
struct DiagRecord {
  SQLINTEGER native_error_code;
  std::string sql_state;
  std::string message;
};

class EnvHandle;
class ConnHandle;
class StmtHandle;

class OdbcHandle {
 public:
  virtual ~OdbcHandle() = default;
  void addDiagRecord(const DiagRecord &record) {
    m_diag_records.push_back(record);
  }
  const std::vector<DiagRecord> &getDiagRecords() const {
    return m_diag_records;
  }

 protected:
  std::vector<DiagRecord> m_diag_records;
};

// 环境句柄封装
class EnvHandle : public OdbcHandle {
 public:
  EnvHandle() : odbc_version(SQL_OV_ODBC3) {}  // 默认 3

  void setOdbcVersion(SQLINTEGER ver) { odbc_version = ver; }
  SQLINTEGER getOdbcVersion() const { return odbc_version; }

 private:
  SQLINTEGER odbc_version;
};

// 连接句柄封装
class ConnHandle : public OdbcHandle {
 public:
  explicit ConnHandle(EnvHandle *parent) : m_parent_env(parent) {}

  ConnHandle(const ConnHandle &) = delete;
  ConnHandle &operator=(const ConnHandle &) = delete;

  void registerStmt(StmtHandle *stmt) {
    m_statements.push_back(stmt);  // 存 raw ptr 即可，生命周期由注册表管理
  }

  void unregisterStmt(StmtHandle *stmt) {
    m_statements.erase(
        std::remove(m_statements.begin(), m_statements.end(), stmt),
        m_statements.end());
  }

  SQLRETURN connect(const std::string &dsn, const std::string &user,
                    const std::string &pass);

  Config &getConfigForUpdate() { return m_config; }

  void setSDK(std::unique_ptr<MaxComputeClient> sdk) { m_sdk = std::move(sdk); }

  MaxComputeClient *getSDK() const { return m_sdk.get(); }

  SQLRETURN disconnect();

  // Get MaxQA session ID (for interactive mode)
  const std::string &getMaxQASessionID() const { return m_maxQASessionID; }

 private:
  Config m_config;
  std::unique_ptr<MaxComputeClient> m_sdk;
  std::vector<StmtHandle *> m_statements;
  EnvHandle *m_parent_env;
  std::string m_maxQASessionID;  // MaxQA Session ID for interactive mode
};

// 语句句柄封装
class StmtHandle : public OdbcHandle {
 public:
  explicit StmtHandle(ConnHandle *parent) : m_parent_conn(parent) {
    if (parent) {
      parent->registerStmt(this);
    }
  }

  ~StmtHandle() override {
    if (m_parent_conn) {
      m_parent_conn->unregisterStmt(this);
    }
  }

  SQLRETURN prepare(const std::string &sql);
  SQLRETURN execute();
  SQLRETURN executeDirect(const std::string &sql);
  SQLRETURN getNumResultCols(SQLSMALLINT *count);
  SQLRETURN describeCol(SQLUSMALLINT col_num, SQLCHAR *col_name,
                        SQLSMALLINT buf_len, SQLSMALLINT *name_len,
                        SQLSMALLINT *data_type, SQLULEN *col_size,
                        SQLSMALLINT *decimal_digits, SQLSMALLINT *nullable);
  SQLRETURN fetch();
  SQLRETURN
  bindCol(SQLUSMALLINT col_num, SQLSMALLINT target_type, SQLPOINTER target_buf,
          SQLLEN buf_len, SQLLEN *indicator);
  SQLRETURN getRowCount(SQLLEN *row_count);
  SQLRETURN getColAttribute(SQLUSMALLINT column_number,
                            SQLUSMALLINT field_identifier,
                            SQLPOINTER character_attribute_ptr,
                            SQLSMALLINT buffer_length,
                            SQLSMALLINT *string_length_ptr,
                            SQLLEN *numeric_attribute_ptr,
                            bool is_wide = false);
  SQLRETURN getData(SQLUSMALLINT col_num, SQLSMALLINT target_type,
                    SQLPOINTER target_buf, SQLLEN buf_len, SQLLEN *indicator);

  // Catalog functions
  SQLRETURN tables(const std::string &catalog, const std::string &schema,
                   const std::string &table, const std::string &type);
  SQLRETURN columns(const std::string &catalog, const std::string &schema,
                    const std::string &table, const std::string &column);

  ConnHandle *getParentConn() { return m_parent_conn; }

  // Statement attribute accessors needed for SQLGetStmtAttr
  SQLLEN getFetchedRows() const { return m_fetched_rows; }
  SQLLEN *getFetchedRowsPtr() { return &m_fetched_rows; }
  SQLHDESC getInputDescriptor() {
    return reinterpret_cast<SQLHDESC>(this);
  }  // Default implementation
  SQLHDESC getOutputDescriptor() {
    return reinterpret_cast<SQLHDESC>(this);
  }  // Default implementation

  struct ColumnBinding {
    SQLSMALLINT target_type;
    SQLPOINTER target_buffer;
    SQLLEN buffer_length;
    SQLLEN *indicator_ptr;
  };

  // In StmtHandle.h, add these to the private section:
 private:
  // Helper to match SQL LIKE patterns ('%' and '_')
  bool sqlLikeMatch(const std::string &str, const std::string &pattern);

  // Helper to fetch, filter, and add tables for a specific schema
  SQLRETURN fetchAndAddTablesForSchema(
      StaticResultSetBuilder &builder, const std::string &catalogName,
      const std::string &schemaName, const std::string &tablePattern,
      const std::vector<std::string> &requestedTypes);

  ConnHandle *m_parent_conn;

  std::string m_prepared_sql;
  std::unique_ptr<ResultStream> m_result_stream;
  std::optional<Record> m_current_row;
  bool m_end_of_stream = false;
  SQLLEN m_fetched_rows = 0;  // 记录已获取的行数

  std::vector<ColumnBinding> m_bindings;
};

}  // namespace maxcompute_odbc
