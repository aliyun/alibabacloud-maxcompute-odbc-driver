# Roadmap

This document outlines the development roadmap for the MaxCompute ODBC Driver after the v1.0.0 open-source release.

> **Note:** This roadmap reflects our current planning and priorities. Actual delivery timelines are not guaranteed and may change based on community feedback and contributions. We welcome pull requests!

Priorities are labeled as:
- **P0**: Critical -- blocks mainstream adoption
- **P1**: High -- significant quality/usability improvement
- **P2**: Medium -- enhances competitiveness
- **P3**: Low -- long-term vision

---

## v1.1.0 -- Feature Completion & Distribution

### P0: SQLBindParameter Implementation

The most critical missing feature. Without parameter binding, most BI tools (Tableau, Power BI, DBeaver) fail when generating parameterized queries.

Scope:
- [ ] Add `SQLBindParameter` entry point and DLL export
- [ ] Add `ParameterBinding` storage in `StmtHandle`
- [ ] Implement parameter value substitution in the query execution path
- [ ] Add companion functions: `SQLNumParams`, `SQLDescribeParam`
- [ ] Unit tests and E2E tests with pyodbc `cursor.execute(sql, params)`

### ~~P0: Windows MSI Installer in Release Pipeline~~ (Done)

Completed. The release pipeline now builds MSI installers automatically:
- [x] `scripts/generate_license_rtf.py` generates `License.rtf` from the `LICENSE` file
- [x] `crc32c.dll` and `date-tz.dll` Component entries added to `Product.wxs`
- [x] Version parameterized in `Product.wxs` via `candle.exe -dProductVersion=...`
- [x] WiX Toolset installed via `choco install wixtoolset` in `release.yml`
- [x] MSI built after driver compilation in the `build-windows` job
- [x] MSI uploaded as a GitHub Release asset alongside zip/tar.gz archives

### P0: Catalog Functions Completion

Many BI tools call these functions during initial connection to probe database capabilities.

- [ ] `SQLGetTypeInfo` -- return MaxCompute type metadata
- [ ] `SQLPrimaryKeys` -- query MaxCompute primary key constraints
- [ ] `SQLStatistics` -- return table/index statistics (can return empty result set for MaxCompute)

### P1: BI Tool Compatibility Matrix

Systematically test and document compatibility with major tools:

- [ ] Tableau Desktop / Server
- [x] Power BI Desktop / Service
- [ ] DBeaver
- [ ] Apache Superset
- [ ] Excel (via ODBC data source)

Publish a compatibility matrix in `docs/compatibility.md` documenting known issues, workarounds, and tested versions.

### P1: Linux/macOS Packaging

Lower the barrier to installation -- users should not need to compile from source.

- [ ] Linux: `.deb` package (Ubuntu/Debian) and `.rpm` package (CentOS/RHEL)
- [ ] macOS: Homebrew formula (`brew install maxcompute-odbc`)
- [ ] Integrate package builds into `release.yml`

---

## v1.2.0 -- Performance & Reliability

### P1: Mock Server for CI E2E Tests

Current E2E tests require live MaxCompute credentials and cannot run in public CI.

- [ ] Build a lightweight HTTP mock server that simulates MaxCompute REST API and Tunnel protocol
- [ ] Enable E2E tests to run automatically in GitHub Actions
- [ ] This is a prerequisite for safely accepting community contributions

### P1: Memory Safety in CI

The driver operates at the C/C++ boundary with raw pointer buffers from ODBC applications.

- [ ] Add AddressSanitizer (ASAN) build configuration to CMake
- [ ] Add UndefinedBehaviorSanitizer (UBSAN) build configuration
- [ ] Run sanitizer builds in CI on every PR
- [ ] Integrate with fuzz testing (libFuzzer) for critical parsing paths (Protobuf deserializer, connection string parser)

### P2: Tunnel Download Optimization

The `ConcurrentBufferedRecordReader` provides a good foundation. Further improvements:

- [ ] Benchmark with different data volumes, column counts, and type distributions
- [ ] Adaptive concurrency: adjust worker count based on network throughput and server-side throttling
- [ ] Memory usage cap: limit total buffer memory to prevent OOM on large result sets
- [ ] Publish benchmark results in documentation

### P2: Connection Health & Auto-Reconnect

MaxCompute is a cloud service; network instability is expected.

- [ ] Connection heartbeat detection
- [ ] Transparent reconnection for dropped connections
- [ ] Retry logic with exponential backoff for transient HTTP errors

### P2: Performance Benchmark Suite

- [ ] Create reproducible benchmarks (varying data sizes, column counts, type complexity)
- [ ] Track regression across releases
- [ ] Publish results in `docs/benchmarks.md`

---

## v2.0.0 -- Architecture Evolution

### P2: Apache Arrow Format Support

MaxCompute supports Arrow-format data transfer. Compared to row-by-row Protobuf V6 deserialization, Arrow's columnar memory format enables near-zero-copy data transfer with potential order-of-magnitude performance improvement.

- [ ] Evaluate MaxCompute Arrow Tunnel API
- [ ] Implement Arrow-based download path alongside existing Protobuf path
- [ ] Allow runtime selection via connection parameter (e.g., `TunnelFormat=arrow`)

### P2: Scrollable Cursors

Some BI tools and reporting engines require scrollable cursors.

- [ ] Implement `SQLFetchScroll` with client-side result caching
- [ ] Support `SQL_FETCH_PRIOR`, `SQL_FETCH_ABSOLUTE`, `SQL_FETCH_RELATIVE`
- [ ] `SQLSetPos` for positioned updates (read-only positioning)

### P3: Write Support (INSERT/UPSERT)

The current driver is read-only. Enabling writes via Tunnel Upload opens up ETL use cases.

- [ ] Implement `SQLBulkOperations` for batch insert
- [ ] Tunnel Upload integration for high-throughput data ingestion
- [ ] `INSERT INTO ... VALUES (...)` via parameter binding

### P3: Connection Pooling

Implement driver-level connection pooling to avoid re-establishing HTTP connections and re-authenticating on every `SQLConnect`.

- [ ] Pool configuration via connection string parameters (`PoolSize`, `PoolTimeout`)
- [ ] Thread-safe pool management
- [ ] Connection validation before reuse

---

## Ongoing Engineering

These are continuous investments, not tied to specific releases.

### Community Infrastructure

- [ ] GitHub Issue templates (Bug Report, Feature Request)
- [ ] Pull Request template with checklist
- [ ] Enable GitHub Discussions for Q&A
- [ ] Per-tool integration guides (one doc per BI tool)
- [ ] Documentation site (GitHub Pages + MkDocs or Docusaurus)

### Security

- [ ] Evaluate upgrading `protobuf` from 3.7.1 to a maintained version
- [ ] Set up Dependabot or Renovate for dependency update PRs
- [ ] Periodic security audit of all dependencies
- [ ] Exercise the vulnerability reporting flow defined in `SECURITY.md`

### Release Automation

- [ ] Adopt Conventional Commits convention
- [ ] Integrate `release-please` or similar tool for automated changelog generation and release PR creation
- [ ] Automate version bumping across `vcpkg.json`, `CMakeLists.txt`, `Product.wxs`, `version.rc`

---

## Priority Summary

| Priority | Item | Rationale |
|----------|------|-----------|
| P0 | `SQLBindParameter` | Most BI tools cannot function without it |
| ~~P0~~ | ~~MSI in release pipeline~~ | ~~Done~~ |
| P0 | Catalog functions (`SQLGetTypeInfo`, etc.) | BI tools probe these on connect |
| P1 | BI compatibility matrix | Users need to know what works |
| P1 | Linux/macOS packages | Users should not need to build from source |
| P1 | Mock server + CI E2E | Cannot safely accept contributions without it |
| P1 | ASAN/UBSAN in CI | Safety net for C-boundary code |
| P2 | Tunnel optimization & benchmarks | Production performance |
| P2 | Connection health & auto-reconnect | Production reliability |
| P2 | Arrow format support | Order-of-magnitude perf improvement |
| P2 | Scrollable cursors | Required by some BI tools |
| P3 | Write support | Enables ETL use cases |
| P3 | Connection pooling | Reduces latency for short-lived connections |
