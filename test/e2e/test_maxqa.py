"""
MaxQA 功能测试
"""

import sys
import os

# 添加父目录到路径以便导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_test import BaseTest, print_section
import pyodbc


class TestMaxQA(BaseTest):
    """MaxQA 功能测试类"""
    
    def test_standard_mode(self):
        """测试标准模式（无 MaxQA）"""
        conn = self.connect(interactive_mode=False)
        cursor = conn.cursor()
        
        # 执行简单查询
        cursor.execute("SELECT 1 as num, 'test' as str")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "First column should be 1")
        self.assert_equals(result[1], 'test', "Second column should be 'test'")
        
        conn.close()
    
    def test_interactive_mode(self):
        """测试交互模式"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 执行查询
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Result should be 1")
        
        conn.close()
    
    def test_quota_hints_mode(self):
        """测试 Quota hints 模式"""
        conn = self.connect(interactive_mode=False, quota_name="default")
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Result should be 1")
        
        conn.close()
    
    def test_multiple_queries_in_session(self):
        """测试同一会话中的多个查询"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 执行多个查询
        for i in range(1, 4):
            cursor.execute(f"SELECT {i}")
            result = cursor.fetchone()
            self.assert_equals(result[0], i, f"Query {i} result should be {i}")
        
        conn.close()
    
    def test_set_statement_parsing(self):
        """测试 SET 语句解析"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 带 SET 语句的查询
        query = "SET odps.sql.decimal.odps2=true; SELECT 1"
        cursor.execute(query)
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Result should be 1")
        
        conn.close()
    
    def test_prepared_statement(self):
        """测试 Prepared Statement"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 准备语句
        cursor.prepare("SELECT ? as param")
        
        # 执行带参数的查询
        cursor.execute(42)
        result = cursor.fetchone()
        self.assert_equals(result[0], 42, "Parameter should be 42")
        
        # 再次执行
        cursor.execute(100)
        result = cursor.fetchone()
        self.assert_equals(result[0], 100, "Parameter should be 100")
        
        conn.close()
    
    def test_parameterized_query(self):
        """测试参数化查询"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 使用参数
        cursor.execute("SELECT ? + ?", (10, 20))
        result = cursor.fetchone()
        self.assert_equals(result[0], 30, "10 + 20 should be 30")
        
        conn.close()
    
    def test_multiple_set_statements(self):
        """测试多个 SET 语句"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 多个 SET 语句
        query = """
            SET odps.sql.decimal.odps2=true;
            SET odps.sql.allow.fullscan=true;
            SELECT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Result should be 1")
        
        conn.close()
    
    def test_set_with_comments(self):
        """测试带注释的 SET 语句"""
        conn = self.connect(interactive_mode=True)
        cursor = conn.cursor()
        
        # 带注释的查询
        query = """
            -- This is a comment
            SET odps.sql.decimal.odps2=true;
            /* Another comment */
            SELECT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "Result should be 1")
        
        conn.close()


def run_all_tests():
    """运行所有 MaxQA 测试"""
    print_section("MaxQA Tests")
    
    # 检查配置
    from config import config
    is_valid, message = config.validate()
    if not is_valid:
        print(f"✗ Configuration error: {message}")
        return False
    
    if not config.quota_name:
        print("⚠ Warning: No quota name configured, some tests may be skipped")
    
    # 运行测试
    test = TestMaxQA()
    
    # SKIP: Interactive mode tests are temporarily disabled
    tests = [
        ("Standard Mode", test.test_standard_mode),
        # ("Interactive Mode", test.test_interactive_mode),  # SKIP: interactive mode
        ("Quota Hints Mode", test.test_quota_hints_mode),
        # ("Multiple Queries in Session", test.test_multiple_queries_in_session),  # SKIP: interactive mode
        # ("SET Statement Parsing", test.test_set_statement_parsing),  # SKIP: interactive mode
        # ("Prepared Statement", test.test_prepared_statement),  # SKIP: interactive mode
        # ("Parameterized Query", test.test_parameterized_query),  # SKIP: interactive mode
        # ("Multiple SET Statements", test.test_multiple_set_statements),  # SKIP: interactive mode
        # ("SET with Comments", test.test_set_with_comments),  # SKIP: interactive mode
    ]
    
    for test_name, test_func in tests:
        test.run_test(test_func, test_name)
    
    return test.print_results()


if __name__ == "__main__":
    import pyodbc
    success = run_all_tests()
    sys.exit(0 if success else 1)