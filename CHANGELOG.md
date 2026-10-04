# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `SQLGetData` now returns variable-length character and binary values in
  parts: repeated calls for the same column hand over the next segment, with
  `SQL_SUCCESS_WITH_INFO` + SQLSTATE `01004` while data remains, a real length
  on the last part, and `SQL_NO_DATA` once the value has been delivered.
- Unit tests for the segmentation rules (`test/unit/column_fetch_test.cpp`)
  and a Driver Manager level contract probe
  (`test/e2e/fetch_contract.c`, build and run documented in
  `test/e2e/E2E_TEST_README.md`).
- E2E cases for long, multi-byte, NULL and empty values read through pyodbc
  with small application buffers (`test/e2e/test_long_data.py`).
- `BUILD_TESTING` build modes for the C++ unit-test suite: with the default `ON`,
  configuring fails when Google Test is unavailable or when a testing build
  registers no test case, instead of silently skipping the whole suite;
  `-DBUILD_TESTING=OFF` is the explicit build-only mode and says so in the log.
- `test/gate-check` fixture plus `scripts/check_test_gate.cmake`, which verify that
  the gate fails closed without needing the driver's dependencies. Both harnesses are
  CMake scripts, so they run unchanged on Linux, macOS and Windows and need nothing
  but CMake to reproduce.
- CI executes `ctest` on Linux, macOS and Windows through
  `scripts/run_unit_tests.cmake` and asserts the number of registered cases; a
  dedicated job checks the gate itself on Linux and Windows.

### Fixed
- `SQLGetData` reported `SQL_ERROR` for any value that did not fit the
  application buffer, so long columns were unreadable instead of
  continuable.
- `SQLGetData` ignored `SQL_NULL_DATA` semantics for NULL values and required
  no indicator variable; a NULL value with `StrLen_or_IndPtr == NULL` now
  reports SQLSTATE `22002` as specified.
- The `SQL_C_WCHAR` conversion computed its writable unit count as
  `BufferLength / sizeof(SQLWCHAR) - 1`, which underflowed for `BufferLength`
  0 or 1 and wrote past the caller's buffer; it also reported the number of
  bytes that happened to fit as the length, so callers could not detect
  truncation.
- Reusing a statement handle returned no rows: `SQLExecute` /
  `SQLExecDirect` / `SQLTables` / `SQLColumns` installed a new result stream
  but kept the previous row, the end-of-stream flag, the fetched-row counter
  and the `SQLGetData` read position. `SQLFreeStmt` now honours `SQL_CLOSE`
  and `SQL_UNBIND` instead of ignoring every option but `SQL_DROP`.
- A bound character column that did not fit its buffer aborted `SQLFetch` with
  `SQL_ERROR` and no diagnostic record. It now behaves as specified: the value
  is truncated in place, the remaining columns of the row are still converted,
  and `SQLFetch` returns `SQL_SUCCESS_WITH_INFO` with SQLSTATE `01004`.
- Statement diagnostics are cleared at the start of `SQLGetData`, so
  `SQLGetDiagRec` no longer keeps returning the first record the handle ever
  produced.
### Changed
- `logging_test` now uses a per-case file under the platform temp directory
  instead of the hard-coded `/tmp/mco_test_log.txt`, so it runs on Windows too and
  no longer needs to be excluded from `ctest`.

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
