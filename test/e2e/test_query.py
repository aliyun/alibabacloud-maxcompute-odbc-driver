"""
查询执行测试
"""

import sys
import os

# 添加父目录到路径以便导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_test import BaseTest, print_section


class TestQuery(BaseTest):
    """查询测试类"""
    
    def test_simple_select(self):
        """测试简单 SELECT 查询"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 as num, 'hello' as str, 3.14 as pi")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "First column should be 1")
        self.assert_equals(result[1], 'hello', "Second column should be 'hello'")
        self.assert_between(result[2], 3.13, 3.15, "Third column should be ~3.14")
        
        conn.close()
    
    def test_select_with_where(self):
        """测试带 WHERE 的 SELECT"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 使用子查询模拟 WHERE
        cursor.execute("SELECT * FROM (SELECT 1 as x UNION ALL SELECT 2 as x) t WHERE x > 0")
        results = cursor.fetchall()
        self.assert_greater(len(results), 0, "Should return at least one result")
        
        conn.close()
    
    def test_aggregate_functions(self):
        """测试聚合函数"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(1) as cnt, SUM(1) as sm, AVG(2.5) as avg FROM (SELECT 1 UNION ALL SELECT 2) t")
        result = cursor.fetchone()
        self.assert_equals(result[0], 2, "COUNT should be 2")
        self.assert_equals(result[1], 2, "SUM should be 2")
        self.assert_between(result[2], 2.4, 2.6, "AVG should be ~2.5")
        
        conn.close()
    
    def test_order_by(self):
        """测试 ORDER BY"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM (SELECT 3 as x UNION ALL SELECT 1 UNION ALL SELECT 2) t ORDER BY x")
        results = cursor.fetchall()
        self.assert_equals(len(results), 3, "Should return 3 results")
        self.assert_equals(results[0][0], 1, "First result should be 1")
        self.assert_equals(results[1][0], 2, "Second result should be 2")
        self.assert_equals(results[2][0], 3, "Third result should be 3")
        
        conn.close()
    
    def test_limit(self):
        """测试 LIMIT"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t LIMIT 2")
        results = cursor.fetchall()
        self.assert_equals(len(results), 2, "Should return 2 results")
        
        conn.close()
    
    def test_union(self):
        """测试 UNION"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 UNION SELECT 2 UNION SELECT 3")
        results = cursor.fetchall()
        self.assert_equals(len(results), 3, "Should return 3 results")
        
        conn.close()
    
    def test_subquery(self):
        """测试子查询"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM (SELECT 1 as x, 2 as y) t WHERE x = 1")
        result = cursor.fetchone()
        self.assert_equals(result[0], 1, "x should be 1")
        self.assert_equals(result[1], 2, "y should be 2")
        
        conn.close()
    
    def test_case_expression(self):
        """测试 CASE 表达式"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END")
        result = cursor.fetchone()
        self.assert_equals(result[0], 'yes', "CASE should return 'yes'")
        
        conn.close()
    
    def test_string_functions(self):
        """测试字符串函数"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CONCAT('hello', ' ', 'world'), UPPER('hello'), LOWER('WORLD')")
        result = cursor.fetchone()
        self.assert_equals(result[0], 'hello world', "CONCAT should work")
        self.assert_equals(result[1], 'HELLO', "UPPER should work")
        self.assert_equals(result[2], 'world', "LOWER should work")
        
        conn.close()
    
    def test_math_functions(self):
        """测试数学函数"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ABS(-5), ROUND(3.14159, 2), FLOOR(3.9), CEIL(3.1)")
        result = cursor.fetchone()
        self.assert_equals(result[0], 5, "ABS(-5) should be 5")
        self.assert_equals(result[1], 3.14, "ROUND(3.14159, 2) should be 3.14")
        self.assert_equals(result[2], 3, "FLOOR(3.9) should be 3")
        self.assert_equals(result[3], 4, "CEIL(3.1) should be 4")
        
        conn.close()
    
    def test_date_functions(self):
        """测试日期函数"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CURRENT_DATE(), CURRENT_TIMESTAMP()")
        result = cursor.fetchone()
        self.assert_not_none(result[0], "CURRENT_DATE should not be None")
        self.assert_not_none(result[1], "CURRENT_TIMESTAMP should not be None")
        
        conn.close()
    
    def test_fetchone(self):
        """测试 fetchone"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 UNION ALL SELECT 2")
        result1 = cursor.fetchone()
        result2 = cursor.fetchone()
        result3 = cursor.fetchone()  # Should be None
        
        self.assert_equals(result1[0], 1, "First fetch should be 1")
        self.assert_equals(result2[0], 2, "Second fetch should be 2")
        self.assert_equals(result3, None, "Third fetch should be None")
        
        conn.close()
    
    def test_fetchmany(self):
        """测试 fetchmany"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4")
        batch1 = cursor.fetchmany(2)
        batch2 = cursor.fetchmany(2)
        
        self.assert_equals(len(batch1), 2, "First batch should have 2 results")
        self.assert_equals(len(batch2), 2, "Second batch should have 2 results")
        
        conn.close()
    
    def test_fetchall(self):
        """测试 fetchall"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
        results = cursor.fetchall()
        
        self.assert_equals(len(results), 3, "Should have 3 results")
        
        conn.close()


def run_all_tests():
    """运行所有查询测试"""
    print_section("Query Tests")
    
    # 检查配置
    from config import config
    is_valid, message = config.validate()
    if not is_valid:
        print(f"✗ Configuration error: {message}")
        return False
    
    # 运行测试
    test = TestQuery()
    
    tests = [
        ("Simple SELECT", test.test_simple_select),
        ("SELECT with WHERE", test.test_select_with_where),
        ("Aggregate Functions", test.test_aggregate_functions),
        ("ORDER BY", test.test_order_by),
        ("LIMIT", test.test_limit),
        ("UNION", test.test_union),
        ("Subquery", test.test_subquery),
        ("CASE Expression", test.test_case_expression),
        ("String Functions", test.test_string_functions),
        ("Math Functions", test.test_math_functions),
        ("Date Functions", test.test_date_functions),
        ("fetchone", test.test_fetchone),
        ("fetchmany", test.test_fetchmany),
        ("fetchall", test.test_fetchall),
    ]
    
    for test_name, test_func in tests:
        test.run_test(test_func, test_name)
    
    return test.print_results()


if __name__ == "__main__":
    import pyodbc
    success = run_all_tests()
    sys.exit(0 if success else 1)