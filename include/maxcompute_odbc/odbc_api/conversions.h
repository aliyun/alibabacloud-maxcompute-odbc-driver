#pragma once
#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#define NOMINMAX
#endif

#include "maxcompute_odbc/maxcompute_client/models.h"  // For ColumnData
#include <sql.h>
#include <sqlext.h>
#include "handles.h"  // For ColumnBinding

namespace maxcompute_odbc {

// 将ColumnData中的一个值，根据绑定信息，转换并写入用户缓冲区
SQLRETURN convertAndWrite(const ColumnData &data,
                          const StmtHandle::ColumnBinding &binding);

struct OdbcColumn {
  std::string name;  // 列名 (用于 SQLDescribeCol 的 ColumnName)

  // --- 核心类型信息 ---
  SQLSMALLINT sql_type;  // ODBC SQL类型码, e.g., SQL_VARCHAR, SQL_BIGINT
  int column_size;       // 列的大小 (e.g., VARCHAR(255) -> 255)
  SQLSMALLINT decimal_digits;  // 小数位数 (e.g., DECIMAL(10, 2) -> 2)
  SQLSMALLINT
  nullable;  // 是否可空 (SQL_NULLABLE = 1, SQL_NO_NULLS = 0, UNKNOWN = 2)
  SQLLEN
  octet_length;  // 列的字节长度 - 最大字节数以存储列数据 (for
                 // SQL_DESC_OCTET_LENGTH)
};

Result<std::unique_ptr<OdbcColumn>> convertColumn(const Column &column);

// 将 MaxCompute Table 类转换了 ODBC 标准 ResultSet, 用于 SQLColumns
// 不支持通配符
// @see
// https://learn.microsoft.com/zh-cn/sql/odbc/reference/syntax/sqlcolumns-function?view=sql-server-ver17
Result<std::unique_ptr<ResultStream>> convertTable(
    const Table &table, const std::string &column_pattern);

// 将 MaxCompute Table 列表转换了 ODBC 标准 ResultSet, 用于 SQLTables
// @see
// https://learn.microsoft.com/zh-cn/sql/odbc/reference/syntax/sqltables-function?view=sql-server-ver17
Result<std::unique_ptr<ResultStream>> convertTables(
    const std::vector<TableId> &tables);
}  // namespace maxcompute_odbc
