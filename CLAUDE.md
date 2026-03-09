# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MaxCompute ODBC Driver is a C++20 ODBC driver for Alibaba Cloud MaxCompute. It allows standard ODBC applications (Tableau, Power BI, DBeaver, etc.) to connect to and query MaxCompute data warehouses via the ODBC interface.

- **Language**: C++20
- **Build System**: CMake 3.20+
- **Package Manager**: vcpkg (manifest mode via `vcpkg.json`)
- **Test Framework**: Google Test (unit), Python pyodbc (e2e)
- **Output**: Shared library `libmaxcompute_odbc.dylib` / `maxcompute_odbc.dll` / `libmaxcompute_odbc.so`

## Architecture

```
ODBC Application -> ODBC Driver Manager -> MaxCompute ODBC Driver -> MaxCompute Service
                                            |
                                      +---------------------+
                                      | ODBC API Layer      |  entry_points, handles, conversions
                                      +---------------------+
                                      | MaxCompute Client   |  client (PIMPL), api_client,
                                      |                     |  request_signer, tunnel,
                                      |                     |  proto_deserializer, typeinfo
                                      +---------------------+
                                      | Config Layer        |  connection string, credentials
                                      +---------------------+
                                      | Common Layer        |  logging, errors (tl::expected),
                                      |                     |  checksum, utils
                                      +---------------------+
                                            |
                                      MaxCompute REST API + Tunnel Service
```

### Module Breakdown

| Module | Directory | Key Classes | Role |
|--------|-----------|-------------|------|
| **odbc_api** | `src/odbc_api/`, `include/maxcompute_odbc/odbc_api/` | `EnvHandle`, `ConnHandle`, `StmtHandle`, `HandleRegistry` | ODBC spec implementation (~40 entry points, ANSI + Wide) |
| **maxcompute_client** | `src/maxcompute_client/`, `include/maxcompute_odbc/maxcompute_client/` | `MaxComputeClient` (PIMPL), `ApiClient`, `RequestSigner`, `DownloadSession`, `ConcurrentBufferedRecordReader`, `ProtoDeserializer`, `TypeInfoParser`, `SettingParser` | All MaxCompute communication: REST API, auth, tunnel download, protobuf deserialization |
| **config** | `src/config/`, `include/maxcompute_odbc/config/` | `Config`, `ConnectionStringParser`, `StaticCredentialProvider` | Connection string parsing, credential management |
| **common** | `include/maxcompute_odbc/common/` | `Logger`, `ErrorInfo`, `Result<T>`, `Checksum` | Header-only utilities: logging, error types, CRC32-C |

### Build Targets (from `src/CMakeLists.txt`)

```
common (INTERFACE) -- tl::expected, crc32c
    |
config (STATIC) -- connection string parsing
    |
maxcompute_client (STATIC) -- REST API, tunnel, protobuf, auth
    |
odbc_api (SHARED) --> OUTPUT: "maxcompute_odbc"  (the ODBC driver)
```

## Development Commands

### Build (macOS/Linux)

```bash
# Ensure VCPKG_ROOT is set
export VCPKG_ROOT=/path/to/vcpkg

# Recommended: use build script
./scripts/build.sh

# Manual build
mkdir build && cd build
cmake -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
      -DCMAKE_CXX_STANDARD=20 ..
make -j$(nproc)

# Register as ODBC driver (optional)
./scripts/build.sh --install
```

### Build (Windows)

```cmd
cmake -S . -B out/build/x64-Release -G "Visual Studio 17 2022" -A x64 ^
      -DCMAKE_TOOLCHAIN_FILE="D:/vcpkg/scripts/buildsystems/vcpkg.cmake" ^
      -DCMAKE_CXX_STANDARD=20 -DVCPKG_TARGET_TRIPLET="x64-windows"
cmake --build out/build/x64-Release --config Release --target odbc_api

REM Or use batch script (supports --debug, --clean, --test, --vcpkg PATH)
scripts\build.bat
```

### Run Tests

```bash
cd build && ctest --output-on-failure

# Or run individual test executables
./bin/config_test
./bin/logging_test
./bin/models_test
./bin/setting_parser_test
```

### Code Formatting

```bash
make format          # Auto-format all source files
make check-format    # Check formatting compliance (CI)
```

Style: Google base style via `.clang-format`, with project headers (`maxcompute_odbc/`) sorted before system headers.

## Project Structure

```
maxcompute-odbc-driver/
+-- include/maxcompute_odbc/
|   +-- platform.h                    # Platform detection macros
|   +-- common/
|   |   +-- checksum.h                # CRC32-C wrapper
|   |   +-- errors.h                  # ErrorCode enum, ErrorInfo, Result<T>
|   |   +-- logging.h                 # Singleton Logger with MCO_LOG_* macros
|   |   +-- utils.h                   # String utilities
|   +-- config/
|   |   +-- config.h                  # Config struct, ConnectionStringParser
|   |   +-- credentials.h             # ICredentials, ICredentialsProvider interfaces
|   +-- maxcompute_client/
|   |   +-- api_client.h              # HTTP client (httplib wrapper, request signing)
|   |   +-- client.h                  # MaxComputeClient public API (PIMPL)
|   |   +-- models.h                  # Data models: Column, Record, ResultStream, Instance, Table, etc.
|   |   +-- proto_deserializer.h      # Tunnel V6 protobuf wire format parser
|   |   +-- request_signer.h          # HMAC-based request signing
|   |   +-- setting_parser.h          # SET key=value; SQL prefix parser
|   |   +-- tunnel.h                  # DownloadSession, BufferedRecordReader, ConcurrentBufferedRecordReader
|   |   +-- typeinfo.h                # Type hierarchy: PrimitiveTypeInfo, DecimalTypeInfo, ArrayTypeInfo, etc.
|   +-- odbc_api/
|       +-- conversions.h             # ColumnData -> SQL_C_xxx conversion
|       +-- entry_points.h            # All SQLxxx function declarations
|       +-- handle_registry.h         # Thread-safe handle -> object mapping
|       +-- handles.h                 # OdbcHandle, EnvHandle, ConnHandle, StmtHandle
+-- src/
|   +-- CMakeLists.txt                # Build targets: common, config, maxcompute_client, odbc_api
|   +-- config/config.cpp
|   +-- maxcompute_client/            # 8 cpp files (client, api_client, models, tunnel, etc.)
|   +-- odbc_api/
|       +-- entry_points.cpp          # ODBC function implementations
|       +-- handles.cpp               # Handle logic (connect, execute, fetch, catalog)
|       +-- conversions.cpp           # Data type conversion logic
|       +-- odbc_exports.def          # Windows DLL exports (37 functions)
+-- test/
|   +-- CMakeLists.txt
|   +-- test_main.cpp                 # GTest main
|   +-- unit/                         # 4 unit test files (config, logging, models, setting_parser)
|   +-- e2e/                          # Python pyodbc-based end-to-end tests
+-- scripts/
|   +-- build.sh / build.bat          # Platform build scripts
|   +-- check_format.sh              # clang-format checker
|   +-- build_installer.bat          # WiX MSI builder (Windows)
|   +-- install_dev_driver.bat       # Dev driver registration (Windows)
|   +-- uninstall_driver.ps1         # Driver uninstall (Windows)
+-- docs/
|   +-- design.md                     # Architecture design
|   +-- usage.md                      # Usage guide
+-- CMakeLists.txt                    # Root CMake config
+-- vcpkg.json                        # Dependency manifest (v0.1.0)
+-- .clang-format                     # Google style, custom include ordering
+-- Product.wxs                       # WiX installer definition
+-- .github/workflows/release.yml     # CI: Windows + macOS build and release
```

## Code Conventions

### Naming
- **Classes**: PascalCase (`MaxComputeClient`, `StmtHandle`)
- **Functions/methods**: camelCase (`executeQuery`, `fetchNextRow`)
- **Member variables**: trailing underscore (`is_connected_`, `config_`)
- **Constants/enums**: UPPER_SNAKE_CASE (`SQL_OV_ODBC3`, `ErrorCode::NetworkError`)
- **Files**: snake_case (`api_client.cpp`, `proto_deserializer.h`)

### Formatting
- 4-space indentation (overrides Google style default)
- K&R brace style (opening brace on same line)
- 120-character column limit
- Google style base (via `.clang-format`)

### Error Handling Pattern
```cpp
// All internal functions return Result<T> = tl::expected<T, ErrorInfo>
Result<Table> getTable(const std::string& name);

// Callers check and propagate errors
auto result = client->getTable(tableName);
if (!result.has_value()) {
    addDiagRecord({1, "HY000", result.error().message});
    return SQL_ERROR;
}
// use result.value()
```

- `ErrorCode` enum: Success, InvalidParameter, NetworkError, ParseError, TimeoutError, ExecutionError, DataError, InternalError, NotImplemented, etc.
- ODBC layer catches exceptions at boundary and converts to SQLRETURN + DiagRecord
- Common SQLSTATE codes: "08001" (connection failure), "HY000" (general error), "HY010" (sequence error), "07009" (invalid index), "01004" (string truncation)

### Handle Registry Pattern
The `HandleRegistry` is a thread-safe singleton that maps opaque `SQLHANDLE` tokens to `OdbcHandle` objects. This is required because the ODBC Driver Manager may wrap/unwrap handle pointers, so the driver cannot assume the SQLHANDLE value equals the C++ object pointer.

## Data Flow: Query Execution

1. `SQLExecDirect` -> `StmtHandle::executeDirect(sql)`
2. `SettingParser::parse(sql)` extracts `SET key=value;` prefixes
3. Schema discovery: submits `EXPLAIN OUTPUT <sql>`, waits, parses JSON result into `ResultSetSchema`
4. Query submission: POST `/projects/{project}/instances` with XML body
5. Returns `ResultStreamImpl` (lazy -- does NOT wait for completion)
6. On first `SQLFetch` -> `ResultStreamImpl::fetchNextRow()`:
   - Creates `DownloadSession` (initiates tunnel session)
   - Opens `ConcurrentBufferedRecordReader` (multi-threaded prefetch)
   - `ProtoDeserializer` decodes MaxCompute Tunnel V6 protobuf wire format
   - CRC32-C checksum validation per-record
7. `SQLGetData` / bound columns -> `convertAndWrite()` converts `ColumnData` variant to SQL_C_xxx types

## Dependencies (vcpkg.json)

| Library | Purpose |
|---------|---------|
| openssl | HTTPS communication |
| cpp-httplib | HTTP client (with openssl + zlib features) |
| cpp-base64 | Base64 encoding/decoding |
| pugixml | XML parsing (MaxCompute API responses) |
| nlohmann-json | JSON parsing |
| tl-expected | `tl::expected<T,E>` error handling |
| protobuf | Wire format API (pinned to v3.7.1, no .proto code generation) |
| crc32c | CRC32-C checksums (hardware accelerated) |
| date | Date/time handling |
| unixodbc | ODBC implementation (Linux/macOS only) |
| libiconv | Character encoding conversion (Linux/macOS only) |
| gtest | Unit testing |

Note: protobuf is used at the wire-format level only (via `CodedInputStream`). No `.proto` files exist; deserialization is hand-rolled in `ProtoDeserializer`.

## ODBC Functions Implemented

**Handle management**: SQLAllocHandle, SQLFreeHandle, SQLAllocEnv/Connect/Stmt, SQLFreeEnv/Connect/Stmt
**Connection**: SQLConnect, SQLDriverConnect(A/W), SQLDisconnect, SQLSetConnectAttr(W), SQLGetConnectAttr(W)
**Execution**: SQLExecDirect(W), SQLPrepare(W), SQLExecute
**Result set**: SQLFetch, SQLBindCol, SQLNumResultCols, SQLDescribeCol(W), SQLRowCount, SQLColAttribute(W), SQLGetData, SQLMoreResults
**Catalog**: SQLTables(W), SQLColumns(W)
**Diagnostics**: SQLGetDiagRec(W), SQLGetDiagField(W)
**Attributes**: SQLSetEnvAttr, SQLGetStmtAttr, SQLSetStmtAttr(W), SQLGetInfo(W)

**Not yet implemented**: SQLBrowseConnect, SQLGetTypeInfo, SQLSpecialColumns, SQLStatistics, SQLPrimaryKeys, SQLForeignKeys, SQLProcedures, SQLProcedureColumns, SQLNativeSql, SQLFetchScroll, SQLSetPos, SQLBulkOperations, SQLEndTran, parameter binding (SQLBindParameter, SQLDescribeParam, SQLParamData, SQLPutData).

## Testing

### Unit Tests (GTest, C++)
| Test | File | Coverage |
|------|------|----------|
| config_test | `test/unit/config_test.cpp` | Config defaults, validation, ConnectionStringParser (14 tests) |
| logging_test | `test/unit/logging_test.cpp` | Logger levels, file output, format strings (4 tests) |
| models_test | `test/unit/models_test.cpp` | InstanceStatus, ExecuteSQLRequest XML/JSON serialization (9 tests) |
| setting_parser_test | `test/unit/setting_parser_test.cpp` | SET statement parsing, edge cases (11 tests) |

### E2E Tests (Python pyodbc)
Located in `test/e2e/`. Requires a running MaxCompute service and registered ODBC driver. Tests cover connection, queries, metadata, data types, error handling.

### Test Coverage Gaps
No unit tests for: ApiClient, RequestSigner, ProtoDeserializer, DownloadSession, ConcurrentBufferedRecordReader, TypeInfoParser, ODBC entry points, data conversions, MaxComputeClient.

## Debugging

### Environment Variables
- `MCO_LOG_LEVEL`: Log level ("DEBUG", "INFO", "WARNING", "ERROR", "NONE"). Default: DEBUG
- `MCO_LOG_FILE`: Log file path. Default: stdout (macOS/Linux) or OutputDebugString (Windows)

### Logging
```cpp
MCO_LOG_DEBUG("Processing query: {}", sql);
MCO_LOG_ERROR("Connection failed: {}", error.message);
```

Format: `[YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] message`

On Windows without `MCO_LOG_FILE`, logs go to Windows debugger (viewable via DebugView).

## Connection String Format

```
DSN=MaxCompute;
Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;
Project=my_project;
AccessKeyId=YOUR_ACCESS_KEY_ID;
AccessKeySecret=YOUR_ACCESS_KEY_SECRET;
LoginTimeout=30
```

## CI/CD

GitHub Actions workflow (`.github/workflows/release.yml`) builds on Windows and macOS, producing release archives. Windows builds also produce an MSI installer via WiX Toolset (`Product.wxs`).
