# MaxCompute ODBC Driver Usage Guide

## Prerequisites

- ODBC Driver Manager installed:
  - **Windows**: Built-in (ODBC Data Source Administrator)
  - **Linux**: [unixODBC](http://www.unixodbc.org/) (`sudo apt install unixodbc` or `sudo yum install unixODBC`)
  - **macOS**: [unixODBC](http://www.unixodbc.org/) via Homebrew (`brew install unixodbc`)
- A MaxCompute project with valid credentials (AccessKey ID/Secret)

## Installation

### Windows

1. Copy `maxcompute_odbc.dll` and all dependent DLLs to a directory (e.g., `C:\MaxComputeODBC\`).
2. Register the driver via the ODBC Data Source Administrator or by running:
   ```cmd
   regsvr32 "C:\MaxComputeODBC\maxcompute_odbc.dll"
   ```
3. Optionally create a System DSN or User DSN through the ODBC Data Source Administrator.

### Linux

1. Copy `libmaxcompute_odbc.so` to a library directory (e.g., `/usr/local/lib/`).
2. Register the driver in `/etc/odbcinst.ini`:
   ```ini
   [MaxCompute ODBC Driver]
   Description = MaxCompute ODBC Driver
   Driver      = /usr/local/lib/libmaxcompute_odbc.so
   ```

### macOS

1. Copy `libmaxcompute_odbc.dylib` to a library directory (e.g., `/usr/local/lib/`).
2. Register the driver in `/usr/local/etc/odbcinst.ini`:
   ```ini
   [MaxCompute ODBC Driver]
   Description = MaxCompute ODBC Driver
   Driver      = /usr/local/lib/libmaxcompute_odbc.dylib
   ```

## Connection String

### Basic Format

```
DRIVER={MaxCompute ODBC Driver};
Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;
Project=my_project;
AccessKeyId=YOUR_ACCESS_KEY_ID;
AccessKeySecret=YOUR_ACCESS_KEY_SECRET
```

### MaxQA Interactive Mode

For faster query responses on interactive workloads:

```
DRIVER={MaxCompute ODBC Driver};
Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;
Project=my_project;
AccessKeyId=YOUR_ACCESS_KEY_ID;
AccessKeySecret=YOUR_ACCESS_KEY_SECRET;
InteractiveMode=true;
QuotaName=my_quota
```

### Using Environment Variables

Credentials can be provided via environment variables to avoid embedding secrets in connection strings:

```bash
export ALIBABA_CLOUD_ENDPOINT="https://service.cn-shanghai.maxcompute.aliyun.com/api"
export ALIBABA_CLOUD_PROJECT="my_project"
export ALIBABA_CLOUD_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="YOUR_ACCESS_KEY_SECRET"
```

Then connect with a minimal connection string:

```
DRIVER={MaxCompute ODBC Driver};InteractiveMode=true
```

## Connection Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `Endpoint` | Yes | -- | MaxCompute service URL. Format: `https://service.{region}.maxcompute.aliyun.com/api` |
| `Project` | Yes | -- | MaxCompute project name |
| `AccessKeyId` | Yes* | -- | Alibaba Cloud AccessKey ID (*or via env var) |
| `AccessKeySecret` | Yes* | -- | Alibaba Cloud AccessKey Secret (*or via env var) |
| `Schema` | No | `default` | Default schema name |
| `LoginTimeout` | No | `30` | Connection timeout in seconds |
| `ReadTimeout` | No | `60` | Read timeout in seconds |
| `InteractiveMode` | No | `false` | Enable MaxQA interactive mode |
| `QuotaName` | No | -- | MaxQA quota name |
| `TunnelEndpoint` | No | -- | Custom Tunnel service endpoint |
| `TunnelQuotaName` | No | -- | Tunnel quota name |
| `HttpProxy` | No | -- | HTTP proxy address |
| `HttpsProxy` | No | -- | HTTPS proxy address |
| `ProxyUser` | No | -- | Proxy authentication username |
| `ProxyPassword` | No | -- | Proxy authentication password |
| `SecurityToken` | No | -- | STS temporary credential token |
| `UserAgent` | No | -- | Custom User-Agent string |

## Supported ODBC Functions

### Connection & Handle Management

| Function | Status |
|----------|--------|
| `SQLAllocHandle` | Supported |
| `SQLFreeHandle` | Supported |
| `SQLConnect` | Supported |
| `SQLDriverConnect` | Supported |
| `SQLDisconnect` | Supported |
| `SQLGetInfo` | Supported |
| `SQLSetEnvAttr` / `SQLGetEnvAttr` | Supported |
| `SQLSetConnectAttr` / `SQLGetConnectAttr` | Supported |
| `SQLSetStmtAttr` / `SQLGetStmtAttr` | Supported |

### Query Execution

| Function | Status |
|----------|--------|
| `SQLExecDirect` | Supported |
| `SQLPrepare` / `SQLExecute` | Supported |
| `SQLNumResultCols` | Supported |
| `SQLDescribeCol` | Supported |
| `SQLColAttribute` | Supported |
| `SQLRowCount` | Supported |
| `SQLBindParameter` | Not yet implemented |

### Result Set Retrieval

| Function | Status |
|----------|--------|
| `SQLFetch` | Supported |
| `SQLGetData` | Supported |
| `SQLBindCol` | Supported |
| `SQLCloseCursor` | Supported |
| `SQLFetchScroll` | Not supported |
| `SQLSetPos` / `SQLBulkOperations` | Not supported |

### Catalog Functions

| Function | Status |
|----------|--------|
| `SQLTables` | Supported |
| `SQLColumns` | Supported |
| `SQLGetTypeInfo` | Not yet implemented |
| `SQLSpecialColumns` | Not yet implemented |
| `SQLStatistics` | Not yet implemented |
| `SQLPrimaryKeys` | Not yet implemented |
| `SQLForeignKeys` | Not yet implemented |

### Diagnostics

| Function | Status |
|----------|--------|
| `SQLGetDiagRec` | Supported |
| `SQLGetDiagField` | Supported |

## Per-Query Settings

You can prepend `SET key=value;` statements to your SQL to configure per-query settings:

```sql
SET odps.sql.type.system.odps2=true;SELECT * FROM my_table LIMIT 10
```

## Logging

Logging is configured via environment variables:

| Variable | Values | Default |
|----------|--------|---------|
| `MCO_LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `NONE` | `DEBUG` |
| `MCO_LOG_FILE` | File path | *(console/debugger output)* |

On Windows without a log file specified, log output is sent to `OutputDebugString` and can be monitored with tools like DebugView.

**Security note**: Log files may contain query text. Configure appropriate file permissions for log files in production environments.

## Code Examples

### Python (pyodbc)

```python
import pyodbc

conn_str = (
    "DRIVER={MaxCompute ODBC Driver};"
    "Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;"
    "Project=my_project;"
    "AccessKeyId=YOUR_KEY;"
    "AccessKeySecret=YOUR_SECRET"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# List tables
for row in cursor.tables():
    print(row.table_name)

# Execute a query
cursor.execute("SELECT * FROM my_table LIMIT 10")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
```

### C++

```cpp
#include <sql.h>
#include <sqlext.h>

SQLHENV henv;
SQLHDBC hdbc;
SQLHSTMT hstmt;

// Initialize
SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

// Connect
const char* connStr = "Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;"
                      "Project=my_project;"
                      "AccessKeyId=YOUR_KEY;"
                      "AccessKeySecret=YOUR_SECRET";
SQLDriverConnect(hdbc, NULL, (SQLCHAR*)connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

// Execute query
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM my_table LIMIT 10", SQL_NTS);

// Fetch results
SQLCHAR col1[256];
SQLLEN indicator;
while (SQLFetch(hstmt) == SQL_SUCCESS) {
    SQLGetData(hstmt, 1, SQL_C_CHAR, col1, sizeof(col1), &indicator);
    printf("Column 1: %s\n", col1);
}

// Cleanup
SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
SQLDisconnect(hdbc);
SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
SQLFreeHandle(SQL_HANDLE_ENV, henv);
```

## Regional Endpoints

| Region | Endpoint |
|--------|----------|
| Shanghai | `https://service.cn-shanghai.maxcompute.aliyun.com/api` |
| Hangzhou | `https://service.cn-hangzhou.maxcompute.aliyun.com/api` |
| Beijing | `https://service.cn-beijing.maxcompute.aliyun.com/api` |
| Shenzhen | `https://service.cn-shenzhen.maxcompute.aliyun.com/api` |
| Hong Kong | `https://service.cn-hongkong.maxcompute.aliyun.com/api` |

## Troubleshooting

### Driver not found

If the ODBC Driver Manager cannot find the driver, verify:
1. The driver is registered in `odbcinst.ini` (Linux/macOS) or the ODBC Data Source Administrator (Windows).
2. The shared library path is correct and the file exists.
3. All dependent shared libraries are accessible (check with `ldd` on Linux or `otool -L` on macOS).

### Connection failures

1. Verify your endpoint URL format: `https://service.{region}.maxcompute.aliyun.com/api`
2. Check that AccessKey ID and Secret are correct.
3. Enable debug logging (`MCO_LOG_LEVEL=DEBUG`) and check the log output.
4. If behind a proxy, configure `HttpProxy` / `HttpsProxy` parameters.

### Timeout errors

- Increase `LoginTimeout` for connection timeouts.
- Increase `ReadTimeout` for query execution timeouts.
- For long-running queries, consider using MaxQA interactive mode for better responsiveness.
