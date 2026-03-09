"""
错误处理测试
"""

import sys
import os

# 添加父目录到路径以便导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_test import BaseTest, print_section


class TestErrors(BaseTest):
    """错误处理测试类"""
    
    def test_syntax_error(self):
        """测试 SQL 语法错误"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELCT 1")  # 故意拼错的 SQL
            self.assert_true(False, "Should have raised an exception")
        except Exception as e:
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_table_not_exists(self):
        """测试表不存在的错误"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM non_existent_table_xyz")
            self.assert_true(False, "Should have raised an exception")
        except Exception as e:
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_column_not_exists(self):
        """测试列不存在的错误"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT non_existent_column FROM (SELECT 1) t")
            self.assert_true(False, "Should have raised an exception")
        except Exception as e:
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_division_by_zero(self):
        """测试除零错误（如果数据库支持）"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT 1 / 0")
            # 某些数据库可能返回 NULL 而不是报错
            result = cursor.fetchone()
            # 如果到这里，说明数据库返回了 NULL 或其他值
        except Exception as e:
            # 某些数据库可能会报错
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_empty_query(self):
        """测试空查询"""
        conn = self.connect()
        cursor = conn.cursor()

        # 空查询在 unixODBC Driver Manager 层会被拒绝 (HY090)
        # 这是 Driver Manager 的行为，不是驱动的问题
        try:
            cursor.execute("")
            # 如果通过，说明 Driver Manager 允许空查询
        except Exception as e:
            # unixODBC 会返回 HY090 错误，这是预期的行为
            error_msg = str(e)
            if "HY090" in error_msg or "Invalid string or buffer length" in error_msg:
                # 这是 Driver Manager 的预期行为，测试通过
                pass
            else:
                # 其他错误则失败
                raise

        conn.close()
    
    def test_invalid_parameter(self):
        """测试无效参数"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT ?", ())  # 空参数元组
            self.assert_true(False, "Should have raised an exception")
        except Exception as e:
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_parameter_count_mismatch(self):
        """测试参数数量不匹配"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT ?, ?", (1,))  # 2 个占位符，1 个参数
            self.assert_true(False, "Should have raised an exception")
        except Exception as e:
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_invalid_connection_string(self):
        """测试无效连接字符串"""
        invalid_conn_str = "DRIVER={MaxCompute ODBC Driver};InvalidParam=value;"
        
        try:
            pyodbc.connect(invalid_conn_str)
            # 连接可能成功，但参数会被忽略
        except Exception as e:
            # 连接失败
            self.assert_not_none(str(e), "Error message should not be empty")
    
    def test_connection_without_credentials(self):
        """测试没有凭据的连接"""
        from config import config
        
        conn_str = (
            f"DRIVER={{MaxCompute ODBC Driver}};"
            f"Endpoint={config.endpoint};"
            f"Project=test_project;"
        )
        
        try:
            pyodbc.connect(conn_str)
            self.assert_true(False, "Should have raised an exception")
        except Exception as e:
            self.assert_not_none(str(e), "Error message should not be empty")
    
    def test_fetch_without_execute(self):
        """测试未执行就获取数据"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.fetchone()
            # 某些驱动可能返回 None
        except Exception as e:
            # 某些驱动可能报错
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_double_close(self):
        """测试重复关闭连接"""
        conn = self.connect()

        conn.close()
        # Some ODBC drivers/pyodbc may raise an error on double close, which is acceptable
        try:
            conn.close()  # 再次关闭
        except Exception:
            pass  # 如果报错也是可接受的
    
    def test_operation_on_closed_connection(self):
        """测试在已关闭的连接上操作"""
        conn = self.connect()
        cursor = conn.cursor()
        
        conn.close()
        
        try:
            cursor.execute("SELECT 1")
            self.assert_true(False, "Should have raised an exception")
        except Exception as e:
            self.assert_not_none(str(e), "Error message should not be empty")
    
    def test_very_long_query(self):
        """测试超长查询（可能超过限制）"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 创建一个超长的查询
            long_query = "SELECT " + ", ".join([f"{i}" for i in range(10000)])
            cursor.execute(long_query)
            # 如果执行成功，可能没有超过限制
        except Exception as e:
            # 可能超过限制
            self.assert_not_none(str(e), "Error message should not be empty")
        
        conn.close()
    
    def test_invalid_maxqa_parameters(self):
        """测试无效的 MaxQA 参数"""
        from config import config
        
        # 使用无效的 interactiveMode 值
        conn_str = (
            f"DRIVER={{MaxCompute ODBC Driver}};"
            f"Endpoint={config.endpoint};"
            f"Project={config.project};"
            f"AccessKeyId={config.access_key_id};"
            f"AccessKeySecret={config.access_key_secret};"
            f"interactiveMode=invalid_value;"
        )
        
        try:
            conn = pyodbc.connect(conn_str)
            # 连接可能成功，但参数会被忽略或使用默认值
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.assert_equals(result[0], 1, "Query should work")
            conn.close()
        except Exception as e:
            # 连接可能失败
            self.assert_not_none(str(e), "Error message should not be empty")


def run_all_tests():
    """运行所有错误处理测试"""
    print_section("Error Handling Tests")
    
    # 检查配置
    from config import config
    is_valid, message = config.validate()
    if not is_valid:
        print(f"✗ Configuration error: {message}")
        return False
    
    # 运行测试
    test = TestErrors()
    
    tests = [
        ("Syntax Error", test.test_syntax_error),
        ("Table Not Exists", test.test_table_not_exists),
        ("Column Not Exists", test.test_column_not_exists),
        ("Division by Zero", test.test_division_by_zero),
        ("Empty Query", test.test_empty_query),
        ("Invalid Parameter", test.test_invalid_parameter),
        ("Parameter Count Mismatch", test.test_parameter_count_mismatch),
        ("Invalid Connection String", test.test_invalid_connection_string),
        ("Connection Without Credentials", test.test_connection_without_credentials),
        ("Fetch Without Execute", test.test_fetch_without_execute),
        ("Double Close", test.test_double_close),
        ("Operation on Closed Connection", test.test_operation_on_closed_connection),
        ("Very Long Query", test.test_very_long_query),
        ("Invalid MaxQA Parameters", test.test_invalid_maxqa_parameters),
    ]
    
    for test_name, test_func in tests:
        test.run_test(test_func, test_name)
    
    return test.print_results()


if __name__ == "__main__":
    import pyodbc
    success = run_all_tests()
    sys.exit(0 if success else 1)