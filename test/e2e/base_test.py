"""
基础测试类和工具函数
"""

import pyodbc
import time
import traceback
from typing import Optional, List, Any
from config import config


class TestResult:
    """测试结果类"""
    def __init__(self, name: str, passed: bool, message: str = "", duration: float = 0):
        self.name = name
        self.passed = passed
        self.message = message
        self.duration = duration
    
    def __repr__(self):
        status = "✓ PASSED" if self.passed else "✗ FAILED"
        return f"{self.name}: {status} ({self.duration:.2f}s)"


class BaseTest:
    """基础测试类"""
    
    def __init__(self):
        self.results: List[TestResult] = []
    
    def connect(
        self,
        interactive_mode: bool = False,
        quota_name: str = None,
        timeout: int = None,
    ) -> pyodbc.Connection:
        """建立数据库连接"""
        conn_str = config.create_connection_string(
            interactive_mode=interactive_mode,
            quota_name=quota_name,
            timeout=timeout,
        )
        return pyodbc.connect(conn_str)
    
    def run_test(self, test_func, test_name: str) -> TestResult:
        """运行单个测试"""
        print(f"\n{'=' * 60}")
        print(f"Running: {test_name}")
        print(f"{'=' * 60}")
        
        start_time = time.time()
        try:
            test_func()
            duration = time.time() - start_time
            result = TestResult(test_name, True, "", duration)
            print(f"✓ {test_name} - PASSED ({duration:.2f}s)")
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            print(f"✗ {test_name} - FAILED: {error_msg}")
            print("\nTraceback:")
            traceback.print_exc()
            result = TestResult(test_name, False, error_msg, duration)
        
        self.results.append(result)
        return result
    
    def assert_equals(self, actual, expected, message: str = ""):
        """断言相等"""
        if actual != expected:
            raise AssertionError(f"{message}\nExpected: {expected}\nActual: {actual}")
    
    def assert_true(self, condition, message: str = ""):
        """断言为真"""
        if not condition:
            raise AssertionError(f"{message}\nExpected: True\nActual: False")
    
    def assert_false(self, condition, message: str = ""):
        """断言为假"""
        if condition:
            raise AssertionError(f"{message}\nExpected: False\nActual: True")
    
    def assert_not_none(self, value, message: str = ""):
        """断言非空"""
        if value is None:
            raise AssertionError(f"{message}\nExpected: not None\nActual: None")
    
    def assert_in(self, item, container, message: str = ""):
        """断言包含"""
        if item not in container:
            raise AssertionError(f"{message}\nExpected: {item} in {container}")
    
    def assert_greater(self, a, b, message: str = ""):
        """断言大于"""
        if a <= b:
            raise AssertionError(f"{message}\nExpected: {a} > {b}")
    
    def assert_less(self, a, b, message: str = ""):
        """断言小于"""
        if a >= b:
            raise AssertionError(f"{message}\nExpected: {a} < {b}")
    
    def assert_between(self, value, min_val, max_val, message: str = ""):
        """断言在范围内"""
        if not (min_val <= value <= max_val):
            raise AssertionError(
                f"{message}\nExpected: {min_val} <= {value} <= {max_val}"
            )
    
    def execute_query(
        self,
        conn: pyodbc.Connection,
        query: str,
        params: Optional[List[Any]] = None,
    ) -> List[tuple]:
        """执行查询并返回所有结果"""
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        return results
    
    def execute_query_one(
        self,
        conn: pyodbc.Connection,
        query: str,
        params: Optional[List[Any]] = None,
    ) -> Optional[tuple]:
        """执行查询并返回第一行结果"""
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchone()
    
    def get_column_names(self, conn: pyodbc.Connection, table_name: str) -> List[str]:
        """获取表的列名"""
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        columns = [column[0] for column in cursor.description]
        return columns
    
    def get_table_count(self, conn: pyodbc.Connection) -> int:
        """获取表数量"""
        result = self.execute_query_one(conn, "SELECT COUNT(*) FROM information_schema.tables")
        return result[0] if result else 0
    
    def print_results(self):
        """打印测试结果摘要"""
        print("\n" + "=" * 60)
        print("Test Results Summary")
        print("=" * 60)
        
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if r.passed == False)
        total = len(self.results)
        duration = sum(r.duration for r in self.results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Duration: {duration:.2f}s")
        
        if failed > 0:
            print("\nFailed Tests:")
            for result in self.results:
                if not result.passed:
                    print(f"  ✗ {result.name}")
                    print(f"    Error: {result.message}")
        
        print("=" * 60)
        
        return failed == 0


def print_section(title: str):
    """打印测试章节标题"""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def measure_time(func):
    """测量函数执行时间的装饰器"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        print(f"Execution time: {duration:.2f}s")
        return result
    return wrapper


if __name__ == "__main__":
    # 测试基础类
    test = BaseTest()
    
    def test_example():
        test.assert_equals(1 + 1, 2, "Math should work")
        test.assert_true(True, "True should be true")
        test.assert_false(False, "False should be false")
        test.assert_in(2, [1, 2, 3], "2 should be in list")
        test.assert_greater(5, 3, "5 should be greater than 3")
        test.assert_less(3, 5, "3 should be less than 5")
        test.assert_between(5, 1, 10, "5 should be between 1 and 10")
    
    test.run_test(test_example, "Example Test")
    test.print_results()
