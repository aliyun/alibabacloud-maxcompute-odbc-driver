# Design Document: MaxCompute ODBC Driver

**Version:** 1.0
**Date:** 2025-03-11

## 1. Overview

### 1.1. Project Goal

This project provides a fully functional ODBC driver for Alibaba Cloud MaxCompute (formerly ODPS).
The driver allows standard ODBC-compliant applications (such as Tableau, Power BI, DBeaver, or custom C++/Python
applications) to connect to and query data from MaxCompute projects.

### 1.2. Core Features

- **Connectivity**: Establish and manage connections to MaxCompute using AccessKey ID/Secret or STS Token authentication.
- **Query Execution**: Execute SQL queries against MaxCompute with asynchronous job polling and retrieve results.
- **High-Throughput Data Download**: MaxCompute Tunnel integration with concurrent prefetching for large result sets.
- **MaxQA Interactive Mode**: MaxCompute Query Acceleration for faster interactive query response times.
- **Metadata Access**: Standard ODBC catalog functions (`SQLTables`, `SQLColumns`, etc.).
- **Data Type Mapping**: Precise mapping between MaxCompute types, ODBC SQL types, and C buffer types.
- **Cross-Platform**: Builds on Windows (`.dll`), Linux (`.so`), and macOS (`.dylib`).

### 1.3. Non-Goals

- Full transaction semantics (`SQLEndTran`), as MaxCompute is primarily an analytical engine.
- Support for every possible ODBC escape sequence.
- Advanced DDL/DML operations beyond what MaxCompute SQL supports.
- Parameter binding (`SQLBindParameter`) -- planned for a future release.

### 1.4. C++20 Design Philosophy

This project is built using the **C++20 standard**. Our core design principles are:

1. **Explicit Error Handling with `tl::expected`**: All functions that can fail return `Result<T>` (an alias for `tl::expected<T, ErrorInfo>`). This eliminates hidden error channels and forces callers to explicitly handle success and failure cases.
2. **Memory Safety at the C Boundary**: Smart pointers and RAII patterns are used throughout. When interacting with C-style buffers provided by the ODBC application (via `SQLPOINTER`), careful bounds checking prevents buffer overflows.
3. **Modern I/O**: Standard C++ streams and `{fmt}`-style logging for reliable cross-platform diagnostics.
4. **Unicode Support**: Internal UTF-8 representation. Both narrow (`SQL...A`) and wide (`SQL...W`) ODBC API variants are supported with proper encoding conversion.

## 2. System Architecture

The driver follows a modular architecture to separate concerns, improve testability, and simplify maintenance.

```
+-------------------------------------------+
|  ODBC Application (e.g., Tableau, DBeaver)|
+-------------------------------------------+
               |
               v  (ODBC API calls)
+-------------------------------------------+
|  ODBC Driver Manager (OS-provided)        |
+-------------------------------------------+
               |
               v  (Driver-specific function calls)
+=======================================+
|  MaxCompute ODBC Driver               |
|                                       |
|  +-------------------------------+    |
|  |      ODBC API Layer           |    |  <-- SQLConnect, SQLExecute, etc.
|  +-------------------------------+    |
|               |                       |
|  +-------------------------------+    |
|  |   MaxCompute Client Layer     |    |  <-- API interactions, async polling,
|  |                               |    |      authentication, Tunnel download
|  +-------------------------------+    |
|               |                       |
|  +---------------+ +-------------+    |
|  | Config/Auth   | | Common/Log  |    |  <-- Utility Modules
|  +---------------+ +-------------+    |
+=======================================+
               |
               v  (REST API / Tunnel)
+-------------------------------------------+
|  MaxCompute Service                       |
+-------------------------------------------+
```

### 2.1. Module Breakdown

- **`odbc_api`**: The public-facing module. Implements ODBC specification functions, manages ODBC handles (environment, connection, statement), and exports the driver entry points. Uses a `HandleRegistry` (thread-safe singleton) for mapping opaque `SQLHANDLE` values to internal C++ objects.
- **`maxcompute_client`**: A unified client layer handling all MaxCompute communication. This includes:
  - REST API request signing (HMAC-based)
  - Job submission and asynchronous status polling
  - Result set fetching via REST API (small results) and MaxCompute Tunnel (large results)
  - Protobuf V6 wire format deserialization (hand-rolled, no `.proto` files)
  - `ConcurrentBufferedRecordReader` for multi-threaded Tunnel data download
  - CRC32-C checksum validation
- **`config`**: Connection string parsing, configuration management, and credential resolution (connection string, environment variables, or STS tokens).
- **`common`**: Shared utilities including logging (`MCO_LOG_*` macros), error types (`ErrorInfo`, `Result<T>`), and string helpers.

### 2.2. Character Set Handling (Unicode Support)

The driver operates internally using UTF-8 strings (`std::string`), aligning with MaxCompute's standard.

- **Narrow API (`SQL...A` functions)**: Input strings are assumed to be in the system's active code page (ACP on Windows). They are converted to UTF-8 upon entering the driver. Output strings are converted from UTF-8 back to ACP.
- **Wide API (`SQL...W` functions)**: Input strings (`SQLWCHAR*`) are treated as UTF-16 (on Windows) or `wchar_t` native encoding (on Linux/macOS). They are converted to UTF-8 internally. Output is converted from UTF-8 back to the wide format.

**Recommendation**: Use the Wide API (`SQL...W`) for maximum compatibility and to avoid data loss during code page conversions.

## 3. Detailed Module Design

### 3.1. Project Structure

```
maxcompute-odbc-driver/
├── include/maxcompute_odbc/       # Internal headers
│   ├── common/                    # Logging, errors, utilities
│   │   ├── logging.h
│   │   ├── errors.h
│   │   └── utils.h
│   ├── config/                    # Configuration and credentials
│   │   ├── config.h
│   │   └── credentials.h
│   ├── odbc_api/                  # ODBC API declarations
│   │   ├── handles.h
│   │   ├── statement.h
│   │   └── ...
│   └── maxcompute_client/         # MaxCompute client interface
│       ├── client.h
│       ├── models.h
│       └── ...
├── src/                           # Implementation files
│   ├── common/
│   ├── config/
│   ├── odbc_api/
│   │   ├── entry_points.cpp       # Exported ODBC C functions
│   │   ├── handles.cpp
│   │   ├── statement.cpp
│   │   └── ...
│   └── maxcompute_client/
│       ├── client.cpp
│       ├── api_client.cpp         # HTTP communication
│       ├── request_signer.cpp     # HMAC request signing
│       └── ...
├── test/                          # Unit and E2E tests
│   ├── config_test.cpp
│   ├── models_test.cpp
│   └── e2e/                       # End-to-end tests (Python/pyodbc)
├── scripts/                       # Build and utility scripts
├── CMakeLists.txt
├── vcpkg.json                     # Dependency manifest
└── .github/workflows/             # CI/CD
```

### 3.2. `maxcompute_client` Module

**Purpose**: A unified client layer that handles all MaxCompute communication. This module has no knowledge of ODBC; it provides a clean API for submitting SQL, polling job status, and retrieving results.

**Key Components**:

- **`RequestSigner`**: Implements the MaxCompute V1 HMAC signature algorithm, constructing the `StringToSign` and generating the `Authorization` header.
- **`ApiClient`**: Uses `cpp-httplib` to send HTTP requests (`POST` for job submission, `GET` for status/results). Supports HTTP/HTTPS proxy configuration.
- **`TunnelClient`**: Downloads large result sets via the MaxCompute Tunnel service using Protobuf V6 wire format.
- **`ConcurrentBufferedRecordReader`**: Multi-threaded prefetch reader that downloads Tunnel data in parallel for high throughput.

**Error Handling**:

```cpp
// All client functions return Result<T> = tl::expected<T, ErrorInfo>
enum class ErrorCode {
    HttpConnectionFailed,
    RequestTimeout,
    CredentialNotFound,
    SignatureGenerationFailed,
    HttpError,          // 4xx, 5xx status codes
    XmlParseError,
    MissingInstanceId,
    JobFailed,
    ResultFetchFailed,
    TunnelError,
    ProtobufDecodeError,
    ChecksumMismatch
};
```

### 3.3. `odbc_api` Module

This module contains the core ODBC handle management and exported entry points.

**Handle Classes**:
- `McoEnv` (Environment Handle)
- `McoConn` (Connection Handle)
- `McoStmt` (Statement Handle)
- `McoDesc` (Descriptor Handle)

**HandleRegistry**: A thread-safe singleton that maps opaque `SQLHANDLE` values (returned to the ODBC Driver Manager) to internal C++ handle objects. This is the bridge between the C API boundary and internal C++ objects.

**Key Entry Points (`entry_points.cpp`)**: These are the C-style functions exported by the driver DLL/SO:

```cpp
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle);
SQLRETURN SQL_API SQLDriverConnect(SQLHDBC hdbc, SQLHWND hwnd, SQLCHAR *szConnStrIn, ...);
SQLRETURN SQL_API SQLExecDirect(SQLHSTMT hstmt, SQLCHAR *szSqlStr, SQLINTEGER cbSqlStr);
SQLRETURN SQL_API SQLFetch(SQLHSTMT hstmt);
SQLRETURN SQL_API SQLGetData(SQLHSTMT hstmt, SQLUSMALLINT icol, ...);
SQLRETURN SQL_API SQLTables(SQLHSTMT hstmt, ...);
SQLRETURN SQL_API SQLColumns(SQLHSTMT hstmt, ...);
// ... and many others (both ANSI and Wide variants)
```

### 3.4. `config` Module

**`DriverConfig` Struct**: Holds all configuration values parsed from the connection string.

```cpp
struct DriverConfig {
    std::optional<std::string> endpoint;
    std::optional<std::string> project;
    std::optional<std::string> schema;           // default: "default"
    std::optional<std::string> accessKeyId;
    std::optional<std::string> accessKeySecret;
    std::optional<std::string> securityToken;    // STS Token
    std::optional<std::string> quotaName;
    std::optional<std::string> tunnelEndpoint;
    std::optional<std::string> tunnelQuotaName;
    std::optional<std::string> httpProxy;
    std::optional<std::string> httpsProxy;
    std::optional<std::string> proxyUser;
    std::optional<std::string> proxyPassword;
    bool interactiveMode = false;
    int loginTimeout = 30;
    int readTimeout = 60;
    // ...
};
```

**Credential Resolution Order**:
1. Connection string parameters
2. Environment variables (`ALIBABA_CLOUD_ACCESS_KEY_ID`, `ALIBABA_CLOUD_ACCESS_KEY_SECRET`, etc.)

## 4. Key Data Flows

### 4.1. Flow of a SELECT Query

1. **App**: `SQLExecDirect(hstmt, "SELECT * FROM my_table", SQL_NTS)`
2. **`odbc_api` (`McoStmt`)**: Receives the call. Checks for `SET key=value;` prefix and extracts per-query settings. Calls `maxcomputeClient->executeQuery(...)`.
3. **`maxcompute_client`**:
   a. Signs the request with HMAC and submits the SQL job via REST API.
   b. Enters an asynchronous polling loop, checking job status periodically.
   c. On success, determines if results should be fetched via REST API (small results) or Tunnel (large results).
   d. For Tunnel: opens a download session, creates `ConcurrentBufferedRecordReader` for parallel chunk download.
   e. Returns the result set to the ODBC layer.
4. **`odbc_api` (`McoStmt`)**: On `SQLFetch`, iterates through rows. On `SQLGetData` / bound columns, converts MaxCompute types to the requested C types and copies data into the application's buffers.

### 4.2. MaxQA Interactive Mode Flow

When `InteractiveMode=true`:
1. The query is submitted to the MaxQA interactive endpoint instead of the batch endpoint.
2. The client uses a different API path that provides lower-latency responses.
3. A `QuotaName` may be specified to route the query to a specific compute quota.

## 5. Error Handling and Diagnostics

- A `DiagRecord` struct holds `SQLSTATE`, a native error code, and a message string.
- Each handle (`McoEnv`, `McoConn`, `McoStmt`) maintains a `std::vector<DiagRecord>`.
- When an error occurs, a `DiagRecord` is pushed onto the appropriate handle's diagnostic list.
- `SQLGetDiagRec` / `SQLGetDiagField` retrieve these records.
- Logging uses `MCO_LOG_DEBUG`, `MCO_LOG_INFO`, `MCO_LOG_WARNING`, `MCO_LOG_ERROR` macros, configurable via `MCO_LOG_LEVEL` and `MCO_LOG_FILE` environment variables.

## 6. Dependencies

All third-party libraries are managed via [vcpkg](https://github.com/microsoft/vcpkg):

| Library | Purpose |
|---------|---------|
| **OpenSSL** | HTTPS communication and HMAC signing |
| **cpp-httplib** | HTTP/HTTPS client (with OpenSSL and zlib support) |
| **cpp-base64** | Base64 encoding/decoding |
| **pugixml** | XML parsing for MaxCompute API responses |
| **nlohmann-json** | JSON parsing |
| **tl-expected** | `tl::expected<T, E>` for explicit error handling |
| **protobuf** (3.7.1) | Protocol Buffers for Tunnel data deserialization |
| **crc32c** | CRC32-C checksum validation for Tunnel data integrity |
| **date** | Date/time handling and formatting |
| **unixodbc** | ODBC headers and driver manager (Linux/macOS) |
| **libiconv** | Character encoding conversion (Linux/macOS) |
| **gtest** | Google Test framework for unit testing |

## 7. Build Targets

The CMake build produces the following targets:

| Target | Type | Description |
|--------|------|-------------|
| `common` | INTERFACE | Header-only utilities (logging, errors) |
| `config` | STATIC | Configuration and credential management |
| `maxcompute_client` | STATIC | MaxCompute client (API + Tunnel) |
| `odbc_api` | SHARED | The ODBC driver (`maxcompute_odbc.dll` / `libmaxcompute_odbc.dylib` / `libmaxcompute_odbc.so`) |

## 8. Testing Strategy

Testing is conducted at multiple levels:

1. **Unit Tests (GTest)**: Each module with minimal dependencies (e.g., `ConfigTest`, `ModelsTest`, `LoggingTest`, `OdbcHandlesTest`) has comprehensive unit tests. These run on every CI build.
2. **End-to-End Tests (Python/pyodbc)**: Located in `test/e2e/`, these tests exercise the full stack from ODBC API down to MaxCompute. They require live MaxCompute credentials set via environment variables.
3. **Application Compatibility**: Manual testing with popular ODBC clients (DBeaver, Tableau, Power BI, Python with pyodbc).

## 9. Known Limitations (v1.0.0)

- `SQLBindParameter` is not yet implemented.
- `SQLFetchScroll` / `SQLSetPos` / `SQLBulkOperations` are not supported.
- `SQLGetTypeInfo`, `SQLSpecialColumns`, `SQLStatistics`, `SQLPrimaryKeys`, `SQLForeignKeys` are not yet implemented.
- `SQLEndTran` is not supported (MaxCompute lacks transaction semantics).
