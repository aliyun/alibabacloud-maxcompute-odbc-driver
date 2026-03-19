# MaxCompute ODBC Driver

[![C++](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://isocpp.org/)
[![CMake](https://img.shields.io/badge/CMake-3.20%2B-blue.svg)](https://cmake.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

A high-performance ODBC driver for Alibaba Cloud MaxCompute, enabling standard ODBC applications to connect to and query MaxCompute data warehouses.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Building](#building)
- [Installation (Windows)](#installation-windows)
- [Usage](#usage)
- [Connection Parameters](#connection-parameters)
- [Code Examples](#code-examples)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Overview

MaxCompute ODBC Driver is a professional-grade ODBC driver built with modern C++20. It allows ODBC-compliant applications such as Tableau, Power BI, DBeaver, and others to connect to and query Alibaba Cloud MaxCompute data warehouses.

### Key Features

- **Standard ODBC Interface** -- Full ODBC 3.x specification support, compatible with major BI tools and database clients
- **Modern C++ Architecture** -- Built on C++20 with a clean, modular design
- **Secure Authentication** -- Supports AccessKey ID/Secret and STS Token authentication
- **Async Query Processing** -- Asynchronous query execution with result-set polling
- **MaxQA Interactive Mode** -- MaxCompute Query Acceleration for faster interactive query response
- **Metadata Support** -- Implements SQLTables, SQLColumns, and other catalog functions
- **Type Mapping** -- Precise mapping between MaxCompute and ODBC data types
- **Cross-Platform** -- Supports Linux, macOS, and Windows
- **Built-in Logging** -- Configurable logging system for diagnostics and debugging
- **Proxy Support** -- HTTP/HTTPS proxy configuration with automatic system proxy detection

## Architecture

```
ODBC Application -> ODBC Driver Manager -> MaxCompute ODBC Driver -> MaxCompute Service
                                            |
                                      +---------------------+
                                      | ODBC API Layer      | (implements SQL functions)
                                      +---------------------+
                                      | SQL/Metadata Layer  | (query translation, metadata)
                                      +---------------------+
                                      | MaxCompute Client   | (unified layer handling
                                      |                     |  MaxCompute communication)
                                      +---------------------+
                                            |
                                      MaxCompute Service (REST API)
```

### Core Modules

- **odbc_api** -- Implements ODBC specification functions (SQLConnect, SQLExecute, etc.)
- **maxcompute_client** -- Unified MaxCompute client handling API interactions, async job polling, and authentication
- **config** -- Connection string parsing and configuration management
- **common** -- Shared utilities including logging and error handling

## Prerequisites

- CMake 3.20 or later
- A C++20-capable compiler (GCC 10+, Clang 10+, MSVC 2019+)
- [vcpkg](https://github.com/microsoft/vcpkg) package manager
- ODBC Driver Manager (unixODBC on Linux/macOS)

## Building

### Linux / macOS

#### Option 1: Build Script (Recommended)

```bash
# Make sure VCPKG_ROOT is set
export VCPKG_ROOT=/path/to/your/vcpkg

# Run the build script
./scripts/build.sh
```

#### Option 2: Manual Build

```bash
mkdir build && cd build

cmake -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
      -DCMAKE_CXX_STANDARD=20 \
      ..

make -j$(nproc)

# Install (optional)
sudo make install
```

### Windows

#### 推荐：使用 MSI 安装包（无需编译）

1. **安装前置依赖**：如果系统未安装 Visual C++ Redistributable，请先从 [Microsoft 官方页面](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist) 下载安装。

2. **下载安装包**：[MaxCompute_ODBC_Driver.msi](https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/odbc/MaxCompute_ODBC_Driver.msi)

3. **安装**：双击下载的 MSI 文件，按向导完成安装。

#### 从源码编译（可选）

##### Environment Setup

1. Install Visual Studio 2022 with the C++ x64/x86 build tools.
2. Install CMake.
3. Install vcpkg and set the `VCPKG_ROOT` environment variable.

##### Build Steps

**Option 1: CMake Command Line (Recommended)**

```cmd
mkdir out\build\x64-Release
cd out\build\x64-Release

cmake -S ..\..\.. -G "Visual Studio 17 2022" -A x64 ^
      -DCMAKE_TOOLCHAIN_FILE="%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake" ^
      -DVCPKG_TARGET_TRIPLET="x64-windows"

cmake --build . --config Release
```

**Option 2: Visual Studio**

Open the project root in Visual Studio, configure CMake settings to use the vcpkg toolchain file, and build.

##### Build Output

After a successful build the following files are produced:

- **Driver DLL**: `out\build\x64-Release\bin\Release\maxcompute_odbc.dll`
- **Static library**: `out\build\x64-Release\lib\Release\maxcompute_client.lib`
- **Test executables**: `out\build\x64-Release\bin\Release\*.exe`

## Installation (Windows)

### Automatic Installation (Recommended)

1. **Copy driver files** -- Copy `maxcompute_odbc.dll` and all dependent DLLs (e.g., `libcrypto-3-x64.dll`, `libssl-3-x64.dll`, `libprotobuf.dll`, `pugixml.dll`) to a directory such as `C:\Windows\System32`.
2. **Register via ODBC Data Source Administrator** -- Open "ODBC Data Sources (64-bit)", go to the "Drivers" tab, click "Add", select `maxcompute_odbc.dll`, set the driver name to "MaxCompute ODBC Driver", and confirm.

### Manual Registration

```cmd
regsvr32 "C:\path\to\maxcompute_odbc.dll"
```

## Usage

### Connection String Format

#### Basic

```
DRIVER={MaxCompute ODBC Driver};
Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;
Project=my_project;
AccessKeyId=YOUR_ACCESS_KEY_ID;
AccessKeySecret=YOUR_ACCESS_KEY_SECRET;
LoginTimeout=30
```

#### MaxQA Interactive Mode

MaxQA (MaxCompute Query Acceleration) provides faster query responses for interactive scenarios.

```
DRIVER={MaxCompute ODBC Driver};
Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;
Project=my_project;
AccessKeyId=YOUR_ACCESS_KEY_ID;
AccessKeySecret=YOUR_ACCESS_KEY_SECRET;
InteractiveMode=true;
QuotaName=my_quota
```

#### Quota Hints Mode

Specify a quota without enabling interactive mode:

```
DRIVER={MaxCompute ODBC Driver};
Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;
Project=my_project;
AccessKeyId=YOUR_ACCESS_KEY_ID;
AccessKeySecret=YOUR_ACCESS_KEY_SECRET;
InteractiveMode=false;
QuotaName=my_quota
```

### Environment Variables

Credentials can also be supplied via environment variables, which is useful for containerized deployments and security-sensitive scenarios:

```bash
export ALIBABA_CLOUD_ENDPOINT="https://service.cn-shanghai.maxcompute.aliyun.com/api"
export ALIBABA_CLOUD_PROJECT="my_project"
export ALIBABA_CLOUD_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="YOUR_ACCESS_KEY_SECRET"

# Then connect without embedding secrets in the connection string
conn_str = "DRIVER={MaxCompute ODBC Driver};InteractiveMode=true"
```

## Connection Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `Endpoint` | Yes | MaxCompute service URL. Format: `https://service.{region}.maxcompute.aliyun.com/api` |
| `Project` | Yes | MaxCompute project name |
| `AccessKeyId` | Yes* | Alibaba Cloud AccessKey ID (*can also be set via environment variable) |
| `AccessKeySecret` | Yes* | Alibaba Cloud AccessKey Secret (*can also be set via environment variable) |
| `Schema` | No | Default schema. Defaults to `default` |
| `LoginTimeout` | No | Connection timeout in seconds. Defaults to 30 |
| `ReadTimeout` | No | Read timeout in seconds. Defaults to 60 |
| `InteractiveMode` | No | Enable MaxQA interactive mode. Defaults to `false` |
| `QuotaName` | No | MaxQA quota name or quota hints |
| `TunnelEndpoint` | No | Custom Tunnel service endpoint |
| `TunnelQuotaName` | No | Tunnel quota name |
| `HttpProxy` | No | HTTP proxy address |
| `HttpsProxy` | No | HTTPS proxy address |
| `ProxyUser` | No | Proxy authentication username |
| `ProxyPassword` | No | Proxy authentication password |
| `SecurityToken` | No | STS temporary credential token |
| `UserAgent` | No | Custom User-Agent string |

## Code Examples

### C++

```cpp
#include <sql.h>
#include <sqlext.h>

// Allocate environment handle
SQLHENV henv;
SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

// Allocate connection handle
SQLHDBC hdbc;
SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

// Connect to MaxCompute
const char* connStr = "Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;"
                      "Project=my_project;"
                      "AccessKeyId=YOUR_KEY;"
                      "AccessKeySecret=YOUR_SECRET";
SQLDriverConnect(hdbc, NULL, (SQLCHAR*)connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

// Execute a query
SQLHSTMT hstmt;
SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM my_table LIMIT 10", SQL_NTS);

// Fetch results ...

// Clean up
SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
SQLDisconnect(hdbc);
SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
SQLFreeHandle(SQL_HANDLE_ENV, henv);
```

### Python (pyodbc)

```python
import pyodbc

# Standard mode
conn_str = (
    "DRIVER={MaxCompute ODBC Driver};"
    "Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;"
    "Project=my_project;"
    "AccessKeyId=YOUR_KEY;"
    "AccessKeySecret=YOUR_SECRET"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("SELECT * FROM my_table LIMIT 10")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
```

#### MaxQA Interactive Mode

```python
import pyodbc

# MaxQA interactive mode (faster query responses)
conn_str = (
    "DRIVER={MaxCompute ODBC Driver};"
    "Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/api;"
    "Project=my_project;"
    "AccessKeyId=YOUR_KEY;"
    "AccessKeySecret=YOUR_SECRET;"
    "InteractiveMode=true;"
    "QuotaName=my_quota"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("SELECT * FROM my_table WHERE dt='20240101' LIMIT 100")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
```

#### Using Environment Variables

```python
import os
import pyodbc

# Provide credentials via environment variables
os.environ['ALIBABA_CLOUD_ENDPOINT'] = 'https://service.cn-shanghai.maxcompute.aliyun.com/api'
os.environ['ALIBABA_CLOUD_PROJECT'] = 'my_project'
os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'] = 'YOUR_KEY'
os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'] = 'YOUR_SECRET'

# No secrets in the connection string
conn = pyodbc.connect("DRIVER={MaxCompute ODBC Driver};InteractiveMode=true")
```

### Windows ODBC Data Source Configuration

> **Note:** This driver does not provide a GUI configuration dialog. DSN configuration via the Windows ODBC Data Source Administrator ("Add" / "Configure" buttons) is **not supported**. All connection parameters must be specified through connection strings.

The MSI installer automatically creates a sample DSN named `MaxComputeTestDSN`. To use the driver:

1. **Recommended:** Use a connection string directly in your application (see examples above).
2. **Alternative:** If your application requires a DSN, use `MaxComputeTestDSN` as the DSN name and provide all other parameters via the connection string.

Regional endpoint examples:

| Region | Endpoint |
|--------|----------|
| Shanghai | `https://service.cn-shanghai.maxcompute.aliyun.com/api` |
| Hangzhou | `https://service.cn-hangzhou.maxcompute.aliyun.com/api` |
| Beijing | `https://service.cn-beijing.maxcompute.aliyun.com/api` |
| Shenzhen | `https://service.cn-shenzhen.maxcompute.aliyun.com/api` |
| Hong Kong | `https://service.cn-hongkong.maxcompute.aliyun.com/api` |

## Testing

### Unit Tests

```bash
cd build
ctest --output-on-failure

# Or run individual tests
./test/config_test
./test/models_test
```

### E2E Tests

E2E tests are located in `test/e2e/` and require MaxCompute connection credentials:

```bash
export ALIBABA_CLOUD_ENDPOINT="https://service.cn-shanghai.maxcompute.aliyun.com/api"
export ALIBABA_CLOUD_PROJECT="your_project"
export ALIBABA_CLOUD_ACCESS_KEY_ID="your_access_key_id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_access_key_secret"
export ALIBABA_CLOUD_QUOTA_NAME="your_quota"  # optional

cd test/e2e
python3 test_connection.py
python3 test_query.py
python3 test_maxqa.py
```

E2E tests cover:

- Connection (standard mode, MaxQA interactive mode, quota hints)
- Queries (simple, complex, parameterized)
- Data types (primitive and complex types)
- Metadata (table listing, column information)
- MaxQA features

### Code Formatting

```bash
make format          # auto-format
make check-format    # check formatting
```

## Project Structure

```
maxcompute-odbc/
├── include/maxcompute_odbc/     # Public headers
│   ├── common/                  # Utilities (logging, errors, helpers)
│   ├── config/                  # Configuration and authentication
│   ├── odbc_api/                # ODBC API declarations
│   └── maxcompute_client/       # MaxCompute client
├── src/                         # Implementation files
│   ├── common/
│   ├── config/
│   ├── odbc_api/
│   └── maxcompute_client/
├── test/                        # Unit and integration tests
├── docs/                        # Documentation
│   ├── design.md                # Architecture and design
│   └── usage.md                 # Usage guide
├── scripts/                     # Build and utility scripts
├── CMakeLists.txt               # CMake configuration
├── vcpkg.json                   # vcpkg dependency manifest
└── LICENSE                      # Apache 2.0 license
```

## Documentation

- [Design Document](docs/design.md) -- Architecture design and technical decisions
- [Usage Guide](docs/usage.md) -- Detailed usage instructions
- [E2E Test Guide](test/e2e/E2E_TEST_README.md) -- End-to-end testing instructions
- [Roadmap](ROADMAP.md) -- Development roadmap and future plans

## Contributing

1. Fork this repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

Please format your code before submitting:

```bash
make format
make check-format
```

## Issues

If you encounter a bug or have a feature request, please open an [Issue](https://github.com/aliyun/maxcompute-odbc-driver/issues).

## License

This project is licensed under the Apache License 2.0 -- see the [LICENSE](LICENSE) file for details.

## Contact

- Repository: [https://github.com/aliyun/maxcompute-odbc-driver](https://github.com/aliyun/maxcompute-odbc-driver)
- Issue Tracker: [https://github.com/aliyun/maxcompute-odbc-driver/issues](https://github.com/aliyun/maxcompute-odbc-driver/issues)

---

# MaxCompute ODBC Driver (中文)

[![C++](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://isocpp.org/)
[![CMake](https://img.shields.io/badge/CMake-3.20%2B-blue.svg)](https://cmake.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

阿里云 MaxCompute 的高性能 ODBC 驱动程序，支持标准 ODBC 应用程序访问 MaxCompute 数据仓库。

## 项目简介

MaxCompute ODBC Driver 是一个使用现代 C++20 开发的专业级 ODBC 驱动程序，允许标准 ODBC 应用程序（如 Tableau、Power BI、DBeaver 等）连接并查询阿里云 MaxCompute 数据仓库。

### 核心特性

- **标准 ODBC 接口** -- 完整实现 ODBC 3.x 规范，兼容主流 BI 工具和数据库客户端
- **现代 C++ 架构** -- 基于 C++20 标准，采用清晰的模块化设计
- **安全认证** -- 支持 AccessKey ID/Secret 和 STS Token 认证方式
- **异步查询** -- 支持异步查询处理和结果集轮询
- **MaxQA 交互模式** -- 支持 MaxCompute Query Acceleration，提供更快的查询响应
- **元数据支持** -- 实现 SQLTables、SQLColumns 等元数据查询功能
- **类型映射** -- 精准的 MaxCompute 与 ODBC 数据类型映射
- **跨平台支持** -- 支持 Linux、macOS 和 Windows
- **完整日志** -- 内置日志系统，便于问题诊断和调试
- **代理支持** -- 支持 HTTP/HTTPS 代理配置，包括系统代理自动检测

## 系统架构

```
ODBC Application -> ODBC Driver Manager -> MaxCompute ODBC Driver -> MaxCompute Service
                                            |
                                      +---------------------+
                                      | ODBC API Layer      | (实现 SQL 函数)
                                      +---------------------+
                                      | SQL/Metadata Layer  | (查询转换、元数据)
                                      +---------------------+
                                      | MaxCompute Client   | (统一层，处理
                                      |                     |  MaxCompute 通信)
                                      +---------------------+
                                            |
                                      MaxCompute Service (REST API)
```

## 环境要求

- CMake 3.20 或更高版本
- 支持 C++20 的编译器（GCC 10+、Clang 10+、MSVC 2019+）
- [vcpkg](https://github.com/microsoft/vcpkg) 包管理器
- ODBC Driver Manager（Linux/macOS 需要 unixODBC）

## 快速编译

### Linux / macOS

```bash
export VCPKG_ROOT=/path/to/your/vcpkg
./scripts/build.sh
```

或手动构建：

```bash
mkdir build && cd build
cmake -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
      -DCMAKE_CXX_STANDARD=20 ..
make -j$(nproc)
```

### Windows

```cmd
cmake -S . -B out/build/x64-Release -G "Visual Studio 17 2022" -A x64 ^
      -DCMAKE_TOOLCHAIN_FILE="%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake" ^
      -DVCPKG_TARGET_TRIPLET="x64-windows"
cmake --build out/build/x64-Release --config Release
```

### Windows ODBC 数据源配置

> **注意：** 本驱动不提供 GUI 配置界面。不支持通过 Windows ODBC 数据源管理器的"添加"/"配置"按钮进行 DSN 配置。所有连接参数必须通过连接字符串指定。

MSI 安装程序会自动创建一个名为 `MaxComputeTestDSN` 的示例 DSN。使用方法：

1. **推荐：** 在应用程序中直接使用连接字符串。
2. **备选：** 如果应用程序必须使用 DSN，可使用 `MaxComputeTestDSN` 作为 DSN 名称，其他参数通过连接字符串提供。

## 连接参数

| 参数 | 必填 | 说明 |
|------|------|------|
| `Endpoint` | 是 | MaxCompute 服务地址，格式：`https://service.{region}.maxcompute.aliyun.com/api` |
| `Project` | 是 | MaxCompute 项目名称 |
| `AccessKeyId` | 是* | 阿里云 AccessKey ID（*也可通过环境变量提供） |
| `AccessKeySecret` | 是* | 阿里云 AccessKey Secret（*也可通过环境变量提供） |
| `Schema` | 否 | 默认 schema，默认值为 `default` |
| `LoginTimeout` | 否 | 连接超时时间（秒），默认 30 |
| `ReadTimeout` | 否 | 读取超时时间（秒），默认 60 |
| `InteractiveMode` | 否 | 启用 MaxQA 交互模式，默认 `false` |
| `QuotaName` | 否 | MaxQA quota 名称或 quota hints |
| `TunnelEndpoint` | 否 | 自定义 Tunnel 服务地址 |
| `TunnelQuotaName` | 否 | Tunnel quota 名称 |
| `HttpProxy` | 否 | HTTP 代理地址 |
| `HttpsProxy` | 否 | HTTPS 代理地址 |
| `ProxyUser` | 否 | 代理认证用户名 |
| `ProxyPassword` | 否 | 代理认证密码 |
| `SecurityToken` | 否 | STS 临时凭证 Token |
| `UserAgent` | 否 | 自定义 User-Agent 字符串 |

### 环境变量

```bash
export ALIBABA_CLOUD_ENDPOINT="https://service.cn-shanghai.maxcompute.aliyun.com/api"
export ALIBABA_CLOUD_PROJECT="my_project"
export ALIBABA_CLOUD_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="YOUR_ACCESS_KEY_SECRET"

# 连接时无需在连接字符串中包含敏感信息
conn_str = "DRIVER={MaxCompute ODBC Driver};InteractiveMode=true"
```

## 测试

### 单元测试

```bash
cd build
ctest --output-on-failure
```

### E2E 测试

请参考 [E2E 测试说明](test/e2e/E2E_TEST_README.md)。

## 文档

- [设计文档](docs/design.md) -- 架构设计和技术决策
- [使用说明](docs/usage.md) -- 详细的使用指南
- [E2E 测试说明](test/e2e/E2E_TEST_README.md) -- E2E 测试详细说明
- [开发路线图](ROADMAP.md) -- 发展规划与未来计划

## 贡献

1. Fork 本项目
2. 创建特性分支（`git checkout -b feature/AmazingFeature`）
3. 提交更改（`git commit -m 'Add some AmazingFeature'`）
4. 推送到分支（`git push origin feature/AmazingFeature`）
5. 创建 Pull Request

## 问题反馈

如果遇到问题或有功能建议，请提交 [Issue](https://github.com/aliyun/maxcompute-odbc-driver/issues)。

## 许可证

本项目采用 Apache License 2.0 许可证 -- 详见 [LICENSE](LICENSE) 文件。
