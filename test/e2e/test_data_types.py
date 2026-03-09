"""
数据类型测试
"""

import sys
import os
from datetime import datetime, date

# 添加父目录到路径以便导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_test import BaseTest, print_section


class TestDataTypes(BaseTest):
    """数据类型测试类"""
    
    def test_bigint(self):
        """测试 BIGINT 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(9223372036854775807 AS BIGINT)")
        result = cursor.fetchone()
        self.assert_equals(result[0], 9223372036854775807, "BIGINT value should match")
        
        conn.close()
    
    def test_int(self):
        """测试 INT 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(2147483647 AS INT)")
        result = cursor.fetchone()
        self.assert_equals(result[0], 2147483647, "INT value should match")
        
        conn.close()
    
    def test_smallint(self):
        """测试 SMALLINT 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(32767 AS SMALLINT)")
        result = cursor.fetchone()
        self.assert_equals(result[0], 32767, "SMALLINT value should match")
        
        conn.close()
    
    def test_tinyint(self):
        """测试 TINYINT 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(127 AS TINYINT)")
        result = cursor.fetchone()
        self.assert_equals(result[0], 127, "TINYINT value should match")
        
        conn.close()
    
    def test_double(self):
        """测试 DOUBLE 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(3.14159265359 AS DOUBLE)")
        result = cursor.fetchone()
        self.assert_between(result[0], 3.1415, 3.1416, "DOUBLE value should be ~3.14159")
        
        conn.close()
    
    def test_float(self):
        """测试 FLOAT 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(2.718 AS FLOAT)")
        result = cursor.fetchone()
        self.assert_between(result[0], 2.717, 2.719, "FLOAT value should be ~2.718")
        
        conn.close()
    
    def test_decimal(self):
        """测试 DECIMAL 类型"""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT CAST(123.456 AS DECIMAL(10,3))")
        result = cursor.fetchone()
        # DECIMAL is returned as string in MaxCompute, convert to float for comparison
        decimal_value = float(result[0])
        self.assert_between(decimal_value, 123.455, 123.457, "DECIMAL value should be ~123.456")

        conn.close()

    def test_string(self):
        """测试 STRING 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        test_str = "Hello, MaxCompute!"
        cursor.execute(f"SELECT CAST('{test_str}' AS STRING)")
        result = cursor.fetchone()
        self.assert_equals(result[0], test_str, "STRING value should match")
        
        conn.close()
    
    def test_varchar(self):
        """测试 VARCHAR 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        test_str = "varchar test"
        cursor.execute(f"SELECT CAST('{test_str}' AS VARCHAR(100))")
        result = cursor.fetchone()
        self.assert_equals(result[0], test_str, "VARCHAR value should match")
        
        conn.close()
    
    def test_char(self):
        """测试 CHAR 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        test_str = "char"
        cursor.execute(f"SELECT CAST('{test_str}' AS CHAR(10))")
        result = cursor.fetchone()
        self.assert_true(result[0].startswith('char'), "CHAR value should start with 'char'")
        
        conn.close()
    
    def test_boolean(self):
        """测试 BOOLEAN 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)")
        result = cursor.fetchone()
        self.assert_true(result[0], "true should be truthy")
        self.assert_false(result[1], "false should be falsy")
        
        conn.close()
    
    def test_date(self):
        """测试 DATE 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST('2024-01-01' AS DATE)")
        result = cursor.fetchone()
        self.assert_not_none(result[0], "DATE value should not be None")
        
        conn.close()
    
    def test_datetime(self):
        """测试 DATETIME 类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST('2024-01-01 12:34:56' AS DATETIME)")
        result = cursor.fetchone()
        self.assert_not_none(result[0], "DATETIME value should not be None")
        
        conn.close()
    
    def test_timestamp(self):
        """测试 TIMESTAMP 类型"""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        result = cursor.fetchone()
        self.assert_not_none(result[0], "TIMESTAMP value should not be None")

        conn.close()
    
    def test_null_values(self):
        """测试 NULL 值"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(NULL AS INT) as null_col, 1 as not_null_col")
        result = cursor.fetchone()
        self.assert_equals(result[0], None, "NULL value should be None")
        self.assert_equals(result[1], 1, "Non-NULL value should be 1")
        
        conn.close()
    
    def test_multiple_types_in_one_query(self):
        """测试同一查询中的多种类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                CAST(123 AS BIGINT) as bigint_val,
                CAST('test' AS STRING) as string_val,
                CAST(3.14 AS DOUBLE) as double_val,
                CAST(true AS BOOLEAN) as bool_val
        """)
        result = cursor.fetchone()
        
        self.assert_equals(result[0], 123, "BIGINT value should be 123")
        self.assert_equals(result[1], 'test', "STRING value should be 'test'")
        self.assert_between(result[2], 3.13, 3.15, "DOUBLE value should be ~3.14")
        self.assert_true(result[3], "BOOLEAN value should be true")
        
        conn.close()
    
    def test_type_conversion(self):
        """测试类型转换"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # INT to STRING
        cursor.execute("SELECT CAST(CAST(123 AS INT) AS STRING)")
        result = cursor.fetchone()
        self.assert_equals(result[0], '123', "INT to STRING conversion should work")
        
        # STRING to INT
        cursor.execute("SELECT CAST(CAST('456' AS STRING) AS INT)")
        result = cursor.fetchone()
        self.assert_equals(result[0], 456, "STRING to INT conversion should work")
        
        conn.close()
    
    def test_large_string(self):
        """测试大字符串"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 创建一个较长的字符串
        long_str = "a" * 1000
        cursor.execute(f"SELECT CAST('{long_str}' AS STRING)")
        result = cursor.fetchone()
        self.assert_equals(len(result[0]), 1000, "Long string should have correct length")
        
        conn.close()
    
    def test_negative_numbers(self):
        """测试负数"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(-123 AS INT), CAST(-3.14 AS DOUBLE)")
        result = cursor.fetchone()
        self.assert_equals(result[0], -123, "Negative INT should be -123")
        self.assert_between(result[1], -3.15, -3.13, "Negative DOUBLE should be ~-3.14")
        
        conn.close()
    
    def test_zero_values(self):
        """测试零值"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CAST(0 AS INT), CAST(0.0 AS DOUBLE)")
        result = cursor.fetchone()
        self.assert_equals(result[0], 0, "INT zero should be 0")
        self.assert_equals(result[1], 0.0, "DOUBLE zero should be 0.0")
        
        conn.close()


def run_all_tests():
    """运行所有数据类型测试"""
    print_section("Data Types Tests")
    
    # 检查配置
    from config import config
    is_valid, message = config.validate()
    if not is_valid:
        print(f"✗ Configuration error: {message}")
        return False
    
    # 运行测试
    test = TestDataTypes()
    
    tests = [
        ("BIGINT", test.test_bigint),
        ("INT", test.test_int),
        ("SMALLINT", test.test_smallint),
        ("TINYINT", test.test_tinyint),
        ("DOUBLE", test.test_double),
        ("FLOAT", test.test_float),
        ("DECIMAL", test.test_decimal),
        ("STRING", test.test_string),
        ("VARCHAR", test.test_varchar),
        ("CHAR", test.test_char),
        ("BOOLEAN", test.test_boolean),
        ("DATE", test.test_date),
        ("DATETIME", test.test_datetime),
        ("TIMESTAMP", test.test_timestamp),
        ("NULL Values", test.test_null_values),
        ("Multiple Types in One Query", test.test_multiple_types_in_one_query),
        ("Type Conversion", test.test_type_conversion),
        ("Large String", test.test_large_string),
        ("Negative Numbers", test.test_negative_numbers),
        ("Zero Values", test.test_zero_values),
    ]
    
    for test_name, test_func in tests:
        test.run_test(test_func, test_name)
    
    return test.print_results()


if __name__ == "__main__":
    import pyodbc
    success = run_all_tests()
    sys.exit(0 if success else 1)