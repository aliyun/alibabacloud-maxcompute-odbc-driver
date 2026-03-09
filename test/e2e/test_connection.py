"""
连接测试
"""

import sys
import os

# 添加父目录到路径以便导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pyodbc
from base_test import BaseTest, print_section
from config import config


class TestConnection(BaseTest):
    """连接测试类"""
    
    def test_standard_connection(self):
        """测试标准连接（无 MaxQA）"""
        conn = self.connect(interactive_mode=False)
        self.assert_not_none(conn, "Connection should not be None")
        
        # 验证连接可用
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Query result should be 1")
        
        conn.close()
    
    def test_interactive_mode_default_quota(self):
        """测试交互模式连接（默认 quota）"""
        conn = self.connect(interactive_mode=True)
        self.assert_not_none(conn, "Connection should not be None")
        
        # 验证连接可用
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Query result should be 1")
        
        conn.close()
    
    def test_interactive_mode_with_quota(self):
        """测试交互模式 + quota 名称"""
        conn = self.connect(interactive_mode=True, quota_name=config.quota_name)
        self.assert_not_none(conn, "Connection should not be None")
        
        # 验证连接可用
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Query result should be 1")
        
        conn.close()
    
    def test_quota_hints_mode(self):
        """测试 Quota hints 模式（非交互 + quota）"""
        conn = self.connect(interactive_mode=False, quota_name=config.quota_name)
        self.assert_not_none(conn, "Connection should not be None")
        
        # 验证连接可用
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Query result should be 1")
        
        conn.close()
    
    def test_multiple_connections(self):
        """测试多次连接/断开"""
        for i in range(3):
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.assert_equals(result[0], 1, f"Query {i+1} result should be 1")
            conn.close()
    
    def test_connection_with_timeout(self):
        """测试连接超时"""
        conn = self.connect(timeout=30)
        self.assert_not_none(conn, "Connection should not be None")
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Query result should be 1")
        
        conn.close()
    
    def test_connection_parameters(self):
        """测试连接字符串参数"""
        # 测试不同的参数组合
        conn_strs = [
            config.create_connection_string(interactive_mode=False),
            config.create_connection_string(interactive_mode=True),
            config.create_connection_string(interactive_mode=False, quota_name=config.quota_name),
        ]
        
        for i, conn_str in enumerate(conn_strs):
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.assert_equals(result[0], 1, f"Connection {i+1} query result should be 1")
            conn.close()


def run_all_tests():
    """运行所有连接测试"""
    print_section("Connection Tests")
    
    # 检查配置
    is_valid, message = config.validate()
    if not is_valid:
        print(f"✗ Configuration error: {message}")
        return False
    
    print(f"✓ Configuration valid")
    print(f"  Project: {config.project}")
    print(f"  Quota: {config.quota_name or 'Default'}")
    
    # 运行测试
    test = TestConnection()
    
    # SKIP: Interactive mode tests are temporarily disabled
    tests = [
        ("Standard Connection", test.test_standard_connection),
        # ("Interactive Mode (Default Quota)", test.test_interactive_mode_default_quota),  # SKIP: interactive mode
        # ("Interactive Mode with Quota", test.test_interactive_mode_with_quota),  # SKIP: interactive mode
        ("Quota Hints Mode", test.test_quota_hints_mode),
        ("Multiple Connections", test.test_multiple_connections),
        ("Connection with Timeout", test.test_connection_with_timeout),
        # ("Connection Parameters", test.test_connection_parameters),  # SKIP: contains interactive mode
    ]
    
    for test_name, test_func in tests:
        test.run_test(test_func, test_name)
    
    return test.print_results()


if __name__ == "__main__":
    import pyodbc
    success = run_all_tests()
    sys.exit(0 if success else 1)