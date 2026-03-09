# E2E 测试文档

本目录包含完整的端到端（E2E）测试套件，使用 pyodbc 验证 MaxCompute ODBC 驱动的所有功能。

## 测试结构

```
test/e2e/
├── config.py                 # 测试配置和环境变量加载
├── base_test.py              # 基础测试类和工具函数
├── test_connection.py        # 连接测试
├── test_maxqa.py             # MaxQA 功能测试
├── test_query.py             # 查询执行测试
├── test_metadata.py          # 元数据查询测试
├── test_data_types.py        # 数据类型测试
├── test_transactions.py      # 事务测试
├── test_errors.py            # 错误处理测试
├── test_performance.py       # 性能测试
├── runner.py                 # 测试运行器
├── check_odbc_setup.py       # ODBC 环境检查工具
└── E2E_TEST_README.md        # 本文档
```

## 前置要求

1. **Python 3.7+**
2. **依赖安装**:
   ```bash
   pip install pyodbc
   pip install pytest  # 可选，用于更高级的测试功能
   ```

3. **已构建的 ODBC 驱动**:
   ```bash
   cmake --build build --config Release
   ```

## 配置

### 方法 1: 环境变量（推荐）

创建 `.env` 文件或设置环境变量：

```bash
export MCO_DRIVER_PATH="/path/to/libmaxcompute_odbc.dylib"
export MAXCOMPUTE_ENDPOINT="https://service.cn-shanghai.maxcompute.aliyun.com/api"
export MAXCOMPUTE_PROJECT="your_project"
export ALIBABA_CLOUD_ACCESS_KEY_ID="your_access_key_id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_access_key_secret"
export MAXCOMPUTE_QUOTA_NAME="your_quota_name"  # 可选
```

### 方法 2: 配置文件

编辑 `config.py` 中的默认值。

## 运行测试

### 运行所有测试

```bash
cd test/e2e
python3 runner.py
```

### 运行特定测试

```bash
# 只运行连接测试
python3 test_connection.py

# 只运行 MaxQA 测试
python3 test_maxqa.py

# 只运行查询测试
python3 test_query.py
```

### 使用 pytest 运行

```bash
cd test/e2e
pytest -v
```

### 运行特定测试类或方法

```bash
# 运行所有 MaxQA 测试
pytest test_maxqa.py -v

# 运行特定测试方法
pytest test_query.py::TestQuery::test_select -v

# 运行带特定标记的测试
pytest -m "smoke"  # 运行冒烟测试
pytest -m "performance"  # 运行性能测试
```

## 测试套件

### 1. 连接测试 (`test_connection.py`)

验证基本的连接功能：

- ✅ 标准连接（无 MaxQA）
- ✅ 交互模式连接（默认 quota）
- ✅ 交互模式 + quota 名称
- ✅ Quota hints 模式（非交互 + quota）
- ✅ 多次连接/断开
- ✅ 连接字符串参数验证
- ✅ 连接超时处理
- ✅ 不同的认证方式

### 2. MaxQA 功能测试 (`test_maxqa.py`)

专门测试 MaxQA 功能：

- ✅ 标准模式（无 MaxQA）
- ✅ 交互模式（默认 quota）
- ✅ 交互模式 + 指定 quota
- ✅ Quota hints 模式
- ✅ 交互模式下的多次查询
- ✅ 同一会话中的多个查询
- ✅ SET 语句解析和传递
- ✅ 参数化查询
- ✅ Prepared Statement

### 3. 查询测试 (`test_query.py`)

测试查询执行功能：

- ✅ 简单 SELECT 查询
- ✅ 带条件的 SELECT 查询
- ✅ JOIN 查询
- ✅ 子查询
- ✅ UNION 查询
- ✅ GROUP BY 和聚合函数
- ✅ ORDER BY
- ✅ LIMIT 和 OFFSET
- ✅ INSERT 语句
- ✅ UPDATE 语句
- ✅ DELETE 语句
- ✅ DDL 语句（CREATE/ALTER/DROP）

### 4. 元数据测试 (`test_metadata.py`)

测试 ODBC 元数据功能：

- ✅ SQLTables - 获取表列表
- ✅ SQLColumns - 获取列信息
- ✅ SQLPrimaryKeys - 获取主键
- ✅ SQLStatistics - 获取统计信息
- ✅ SQLTablePrivileges - 获取表权限
- ✅ SQLColumnPrivileges - 获取列权限
- ✅ SQLGetTypeInfo - 获取类型信息
- ✅ SQLDescribeCol - 描述列属性
- ✅ SQLColAttribute - 获取列属性

### 5. 数据类型测试 (`test_data_types.py`)

测试所有 MaxCompute 数据类型：

- ✅ 基本类型：BIGINT, INT, SMALLINT, TINYINT
- ✅ 浮点类型：DOUBLE, FLOAT, DECIMAL
- ✅ 字符串类型：STRING, VARCHAR, CHAR
- ✅ 日期时间类型：DATE, DATETIME, TIMESTAMP
- ✅ 布尔类型：BOOLEAN
- ✅ 二进制类型：BINARY
- ✅ 复杂类型：ARRAY, MAP, STRUCT
- ✅ NULL 值处理
- ✅ 类型转换

### 6. 事务测试 (`test_transactions.py`)

测试事务功能：

- ✅ 自动提交模式
- ✅ 手动提交
- ✅ 回滚
- ✅ 嵌套事务
- ✅ 事务隔离级别

### 7. 错误处理测试 (`test_errors.py`)

测试错误处理：

- ✅ 无效的连接参数
- ✅ SQL 语法错误
- ✅ 表不存在
- ✅ 列不存在
- ✅ 认证失败
- ✅ 网络错误
- ✅ 超时错误
- ✅ 诊断信息获取 (SQLGetDiagRec)

### 8. 性能测试 (`test_performance.py`)

测试驱动性能：

- ✅ 连接建立时间
- ✅ 查询执行时间
- ✅ 大结果集获取
- ✅ 批量操作
- ✅ 并发连接
- ✅ 内存使用

## 测试标记

测试使用 pytest 标记进行分类：

- `@pytest.mark.smoke` - 冒烟测试（快速验证基本功能）
- `@pytest.mark.maxqa` - MaxQA 相关测试
- `@pytest.mark.query` - 查询执行测试
- `@pytest.mark.metadata` - 元数据测试
- `@pytest.mark.datatype` - 数据类型测试
- `@pytest.mark.error` - 错误处理测试
- `@pytest.mark.performance` - 性能测试
- `@pytest.mark.slow` - 慢速测试（需要真实数据）

## 预期输出

成功运行时，你应该看到类似以下的输出：

```
============================================================
MaxCompute ODBC Driver - E2E Test Suite
============================================================

[1/10] Running Connection Tests...
  ✓ test_standard_connection
  ✓ test_interactive_mode_default_quota
  ✓ test_interactive_mode_with_quota
  ✓ test_quota_hints_mode
  Connection Tests: 4/4 PASSED

[2/10] Running MaxQA Tests...
  ✓ test_standard_mode
  ✓ test_interactive_mode
  ✓ test_quota_hints_mode
  ✓ test_multiple_queries_session
  MaxQA Tests: 4/4 PASSED

[3/10] Running Query Tests...
  ✓ test_simple_select
  ✓ test_select_with_where
  ✓ test_join_query
  ✓ test_aggregate_functions
  Query Tests: 4/4 PASSED

...

============================================================
Test Summary
============================================================
Total Tests: 40
Passed: 38
Failed: 0
Skipped: 2
Duration: 45.2s

✓ All critical tests PASSED!
```

## 故障排除

### 1. 驱动未找到

```
✗ Driver not found: /path/to/libmaxcompute_odbc.dylib
```

**解决方案**：
```bash
# 确保已构建驱动
cmake --build build --config Release

# 检查驱动文件是否存在
ls -la build/lib/libmaxcompute_odbc.dylib

# 设置正确的路径
export MCO_DRIVER_PATH="/absolute/path/to/libmaxcompute_odbc.dylib"
```

### 2. pyodbc 未安装

```
✗ pyodbc not installed
```

**解决方案**：
```bash
pip install pyodbc
```

### 3. 连接失败

```
✗ Connection failed: [IM002] [unixODBC][Driver Manager]Data source name not found
```

**解决方案**：
- 使用环境变量配置
- 或在连接字符串中使用绝对路径
- 或注册 ODBC 驱动到系统

### 4. 认证失败

```
✗ Authentication failed
```

**解决方案**：
- 检查 Access Key ID 和 Access Key Secret 是否正确
- 确认账户权限是否足够
- 检查网络连接

### 5. 测试超时

```
✗ Test timeout after 30s
```

**解决方案**：
- 增加测试超时时间
- 检查网络连接
- 使用更简单的测试查询

## 添加新测试

### 使用基类

```python
from base_test import BaseTest
import pytest

class TestNewFeature(BaseTest):
    """测试新功能的基类"""
    
    @pytest.mark.smoke
    def test_basic_functionality(self):
        """测试基本功能"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 你的测试逻辑
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        
        conn.close()
    
    @pytest.mark.maxqa
    def test_with_maxqa(self):
        """测试 MaxQA 模式下的功能"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 你的测试逻辑
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        
        conn.close()
```

### 运行新测试

```bash
pytest test_new_feature.py -v
```

## 持续集成

### GitHub Actions 示例

```yaml
name: E2E Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  e2e-tests:
    runs-on: macos-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'
    
    - name: Install dependencies
      run: |
        pip install pyodbc pytest
    
    - name: Build driver
      run: |
        cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
        cmake --build build --config Release
    
    - name: Run E2E tests
      env:
        MCO_DRIVER_PATH: ${{ github.workspace }}/build/lib/libmaxcompute_odbc.dylib
        MAXCOMPUTE_ENDPOINT: ${{ secrets.MAXCOMPUTE_ENDPOINT }}
        MAXCOMPUTE_PROJECT: ${{ secrets.MAXCOMPUTE_PROJECT }}
        ALIBABA_CLOUD_ACCESS_KEY_ID: ${{ secrets.ALIBABA_CLOUD_ACCESS_KEY_ID }}
        ALIBABA_CLOUD_ACCESS_KEY_SECRET: ${{ secrets.ALIBABA_CLOUD_ACCESS_KEY_SECRET }}
      run: |
        cd test/e2e
        python3 runner.py --smoke --maxqa
```

## 最佳实践

1. **使用环境变量**: 避免在代码中硬编码敏感信息
2. **独立测试**: 每个测试应该独立运行，不依赖其他测试
3. **清理资源**: 确保每个测试都正确关闭连接和游标
4. **错误处理**: 捕获并记录所有异常
5. **清晰的输出**: 使用清晰的测试名称和输出
6. **标记测试**: 使用 pytest 标记对测试进行分类
7. **超时设置**: 为慢速测试设置合理的超时时间
8. **测试数据**: 使用专门的测试表和测试数据

## 相关文档

- [MaxCompute ODBC Driver README](../../README.md)
- [MaxCompute API 文档](../../docs/maxcompute-api.md)
- [类型映射](../../docs/typeinfo_parser.md)
- [设计文档](../../docs/design.md)