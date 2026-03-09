# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-03-11

### Added
- Initial open-source release of MaxCompute ODBC Driver.
- ODBC 3.x specification support with both ANSI and Wide (Unicode) API variants.
- Core ODBC functions: handle management, connection, query execution, result set navigation, catalog functions, diagnostics.
- `SQLDriverConnect` / `SQLConnect` for establishing connections to MaxCompute.
- `SQLExecDirect` / `SQLPrepare` / `SQLExecute` for query execution.
- `SQLFetch` / `SQLGetData` / `SQLBindCol` for result set retrieval.
- `SQLTables` / `SQLColumns` for metadata queries.
- MaxCompute Tunnel integration for high-throughput data download with concurrent prefetching.
- Protobuf V6 wire format deserialization with CRC32-C checksum validation.
- MaxQA (MaxCompute Query Acceleration) interactive mode support.
- HMAC-based request signing for MaxCompute REST API authentication.
- AccessKey ID/Secret and STS Token authentication.
- Environment variable support for credentials (`ALIBABA_CLOUD_ACCESS_KEY_ID`, etc.).
- HTTP/HTTPS proxy configuration support with system proxy auto-detection.
- `SET key=value;` SQL prefix parsing for per-query settings.
- Cross-platform support: Windows (DLL), macOS (dylib), Linux (SO).
- Configurable logging with `MCO_LOG_LEVEL` and `MCO_LOG_FILE` environment variables.
- Windows MSI installer via WiX Toolset.
- Unit test suite (GTest) and Python E2E test suite (pyodbc).
- GitHub Actions CI/CD for Windows and macOS builds.
- Apache 2.0 license.

### Known Limitations
- Parameter binding (`SQLBindParameter`) is not yet implemented.
- `SQLFetchScroll` / `SQLSetPos` / `SQLBulkOperations` are not supported.
- `SQLGetTypeInfo`, `SQLSpecialColumns`, `SQLStatistics`, `SQLPrimaryKeys`, `SQLForeignKeys` are not yet implemented.
- `SQLEndTran` is not supported (MaxCompute is an analytical engine without transaction semantics).
