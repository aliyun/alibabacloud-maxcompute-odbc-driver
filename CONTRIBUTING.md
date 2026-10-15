# Contributing to MaxCompute ODBC Driver

Thank you for your interest in contributing! This document provides guidelines and information for contributors.

## How to Contribute

### Reporting Issues

- Use [GitHub Issues](https://github.com/aliyun/maxcompute-odbc-driver/issues) to report bugs or request features.
- Before creating an issue, please search existing issues to avoid duplicates.
- Include as much detail as possible: OS, compiler version, ODBC Driver Manager version, steps to reproduce.

### Submitting Pull Requests

1. Fork the repository.
2. Create a feature branch from `master`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes. Ensure they follow the coding standards below.
4. Run tests and formatting checks:
   ```bash
   cd build && ctest --output-on-failure
   make check-format
   ```
5. Commit with a clear, descriptive message.
6. Push to your fork and open a Pull Request against `master`.

### Pull Request Guidelines

- Keep PRs focused -- one feature or fix per PR.
- Include tests for new functionality.
- Update documentation if your change affects the public API or usage.
- Ensure all CI checks pass before requesting review.

## Development Setup

### Prerequisites

- CMake 3.20+
- C++20-capable compiler (GCC 10+, Clang 10+, MSVC 2019+)
- vcpkg package manager
- `VCPKG_ROOT` environment variable set

### Building

```bash
export VCPKG_ROOT=/path/to/vcpkg
./scripts/build.sh
```

### Running Tests

```bash
cd build && ctest --output-on-failure
```

`BUILD_TESTING=ON` (the default) makes the suite mandatory: configure fails if
Google Test is missing or if no test case is registered, so a build that quietly
ran zero tests can no longer look like a pass. Use `-DBUILD_TESTING=OFF` when you
intentionally want build-only/packaging output; the configure log then states that
no tests were built or run.

```bash
cmake -P scripts/check_test_gate.cmake                        # the gate must fail closed
cmake -DBUILD_DIR=build -P scripts/run_unit_tests.cmake        # same step CI runs
```

If you add or remove a test suite, update `MCO_EXPECTED_UNIT_TESTS` in
`test/CMakeLists.txt` and `-DMIN_TESTS` in `.github/workflows/ci.yml` in the same PR.

## Coding Standards

### Style

- Follow the `.clang-format` configuration (Google base style).
- Run `make format` before committing to auto-format all source files.
- Use 4-space indentation, K&R brace style, 120-character line limit.

### Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Classes | PascalCase | `MaxComputeClient` |
| Functions/methods | camelCase | `executeQuery` |
| Member variables | trailing underscore | `config_` |
| Constants/enums | UPPER_SNAKE_CASE | `ErrorCode::NetworkError` |
| Files | snake_case | `api_client.cpp` |

### Error Handling

- Use `Result<T>` (`tl::expected<T, ErrorInfo>`) for all functions that can fail.
- Never throw exceptions across module boundaries.
- Provide clear error messages with appropriate SQLSTATE codes at the ODBC layer.

### Logging

- Use `MCO_LOG_DEBUG`, `MCO_LOG_INFO`, `MCO_LOG_WARNING`, `MCO_LOG_ERROR` macros.
- Never log sensitive information (credentials, access keys) in plain text.

## Code of Conduct

Please review our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

## License

By contributing to this project, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
