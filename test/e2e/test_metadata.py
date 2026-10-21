"""
元数据查询测试
"""

import sys
import os
import ctypes

# 添加父目录到路径以便导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_test import BaseTest, print_section


def _load_odbc():
    """加载 ODBC Driver Manager (Windows: odbc32, Linux/Unix: libodbc)"""
    if sys.platform == "win32":
        return ctypes.WinDLL("odbc32.dll")
    return ctypes.CDLL("libodbc.so.2")


# x64 下 SQLLEN/SQLULEN 均为 64 位, 显式声明签名避免缓冲区越界
_odbc = _load_odbc()
_odbc.SQLAllocHandle.restype = ctypes.c_short
_odbc.SQLAllocHandle.argtypes = [ctypes.c_short, ctypes.c_void_p,
                                 ctypes.POINTER(ctypes.c_void_p)]
_odbc.SQLSetEnvAttr.restype = ctypes.c_short
_odbc.SQLSetEnvAttr.argtypes = [ctypes.c_void_p, ctypes.c_int,
                                ctypes.c_void_p, ctypes.c_int]
_odbc.SQLDriverConnectW.restype = ctypes.c_short
_odbc.SQLDriverConnectW.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
                                    ctypes.c_wchar_p, ctypes.c_short,
                                    ctypes.c_wchar_p, ctypes.c_short,
                                    ctypes.POINTER(ctypes.c_short),
                                    ctypes.c_ushort]
_odbc.SQLExecDirectW.restype = ctypes.c_short
_odbc.SQLExecDirectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p,
                                 ctypes.c_int]
_odbc.SQLFreeStmt.restype = ctypes.c_short
_odbc.SQLFreeStmt.argtypes = [ctypes.c_void_p, ctypes.c_short]
_odbc.SQLDisconnect.restype = ctypes.c_short
_odbc.SQLDisconnect.argtypes = [ctypes.c_void_p]
_odbc.SQLFreeHandle.restype = ctypes.c_short
_odbc.SQLFreeHandle.argtypes = [ctypes.c_short, ctypes.c_void_p]
_odbc.SQLDescribeCol.restype = ctypes.c_short
_odbc.SQLDescribeCol.argtypes = [
    ctypes.c_void_p, ctypes.c_ushort, ctypes.c_char_p, ctypes.c_short,
    ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_short),
    ctypes.POINTER(ctypes.c_ulonglong), ctypes.POINTER(ctypes.c_short),
    ctypes.POINTER(ctypes.c_short),
]
_odbc.SQLColAttribute.restype = ctypes.c_short
_odbc.SQLColAttribute.argtypes = [
    ctypes.c_void_p, ctypes.c_ushort, ctypes.c_short, ctypes.c_char_p,
    ctypes.c_short, ctypes.POINTER(ctypes.c_short),
    ctypes.POINTER(ctypes.c_longlong),
]

SQL_DESC_CONCISE_TYPE = 2
SQL_HANDLE_ENV = 1
SQL_HANDLE_DBC = 2
SQL_HANDLE_STMT = 3
SQL_DRIVER_NOPROMPT = 0
SQL_NTS = -3
SQL_SUCCESS_VALUES = (0, 1)  # SQL_SUCCESS / SQL_SUCCESS_WITH_INFO


def _query_column_types(sql, ncols):
    """通过原生 ODBC (Driver Manager -> 注册的 MaxCompute 驱动) 执行 sql,
    返回每列的 (SQLDescribeCol 类型码, SQLColAttribute SQL_DESC_CONCISE_TYPE).

    pyodbc 5.x 的 cursor.description type_code 返回的是 Python 类型对象
    (str/int/float...), 不携带 ODBC SQL 类型码; 之前 SQLDescribeCol /
    SQLColAttribute 测试因此把类型断言整个剥掉了. 这里走原生路径恢复验证,
    与 Power BI 等原生 ODBC 消费者看到的行为一致.
    """
    from config import config

    h_env = ctypes.c_void_p()
    h_dbc = ctypes.c_void_p()
    h_stmt = ctypes.c_void_p()
    try:
        rc = _odbc.SQLAllocHandle(SQL_HANDLE_ENV, None, ctypes.byref(h_env))
        if rc not in SQL_SUCCESS_VALUES:
            raise RuntimeError(f"SQLAllocHandle(ENV) failed, rc={rc}")
        rc = _odbc.SQLSetEnvAttr(h_env, 200, ctypes.c_void_p(3), 0)  # ODBC3
        if rc not in SQL_SUCCESS_VALUES:
            raise RuntimeError(f"SQLSetEnvAttr failed, rc={rc}")
        rc = _odbc.SQLAllocHandle(SQL_HANDLE_DBC, h_env, ctypes.byref(h_dbc))
        if rc not in SQL_SUCCESS_VALUES:
            raise RuntimeError(f"SQLAllocHandle(DBC) failed, rc={rc}")

        conn_str_w = ctypes.create_unicode_buffer(
            config.create_connection_string())
        out = ctypes.create_unicode_buffer(1024)
        out_len = ctypes.c_short()
        rc = _odbc.SQLDriverConnectW(
            h_dbc, None, conn_str_w, SQL_NTS, out, 1024,
            ctypes.byref(out_len), SQL_DRIVER_NOPROMPT)
        if rc not in SQL_SUCCESS_VALUES:
            raise RuntimeError(f"SQLDriverConnectW failed, rc={rc}")
        rc = _odbc.SQLAllocHandle(SQL_HANDLE_STMT, h_dbc, ctypes.byref(h_stmt))
        if rc not in SQL_SUCCESS_VALUES:
            raise RuntimeError(f"SQLAllocHandle(STMT) failed, rc={rc}")

        sql_w = ctypes.create_unicode_buffer(sql)
        rc = _odbc.SQLExecDirectW(h_stmt, sql_w, SQL_NTS)
        if rc not in SQL_SUCCESS_VALUES:
            raise RuntimeError(f"SQLExecDirectW failed, rc={rc}")

        results = []
        for col in range(1, ncols + 1):
            name = ctypes.create_string_buffer(256)
            name_len = ctypes.c_short()
            data_type = ctypes.c_short()
            col_size = ctypes.c_ulonglong()
            digits = ctypes.c_short()
            nullable = ctypes.c_short()
            rc = _odbc.SQLDescribeCol(
                h_stmt, col, name, 256, ctypes.byref(name_len),
                ctypes.byref(data_type), ctypes.byref(col_size),
                ctypes.byref(digits), ctypes.byref(nullable))
            if rc not in SQL_SUCCESS_VALUES:
                raise RuntimeError(f"SQLDescribeCol failed for column {col}")
            concise = ctypes.c_longlong()
            rc = _odbc.SQLColAttribute(
                h_stmt, col, SQL_DESC_CONCISE_TYPE,
                None, 0, None, ctypes.byref(concise))
            if rc not in SQL_SUCCESS_VALUES:
                raise RuntimeError(
                    f"SQLColAttribute failed for column {col}")
            results.append((data_type.value, concise.value))
        return results
    finally:
        if h_stmt:
            _odbc.SQLFreeStmt(h_stmt, 0)  # SQL_CLOSE / unbind
            _odbc.SQLFreeHandle(SQL_HANDLE_STMT, h_stmt)
        if h_dbc:
            _odbc.SQLDisconnect(h_dbc)
            _odbc.SQLFreeHandle(SQL_HANDLE_DBC, h_dbc)
        if h_env:
            _odbc.SQLFreeHandle(SQL_HANDLE_ENV, h_env)


class TestMetadata(BaseTest):
    """元数据测试类"""
    
    def test_sqltables(self):
        """测试 SQLTables - 获取表列表"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 获取所有表
        cursor.tables(tableType='TABLE')
        tables = cursor.fetchall()
        
        self.assert_greater(len(tables), 0, "Should return at least one table")
        
        # 检查返回的列
        for table in tables:
            self.assert_not_none(table[2], "Table name should not be None")
        
        conn.close()
    
    def test_sqlcolumns(self):
        """测试 SQLColumns - 获取列信息"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 获取 information_schema.tables 的列信息
        cursor.columns(table='information_schema.tables')
        columns = cursor.fetchall()
        
        self.assert_greater(len(columns), 0, "Should return at least one column")
        
        # 检查列信息结构
        for col in columns:
            self.assert_not_none(col[3], "Column name should not be None")
        
        conn.close()
    
    def test_sqlcolumns_unicode_types(self):
        """SQLColumns 把 STRING 列报为 SQL_WVARCHAR"""
        import pyodbc

        conn = self.connect()
        cursor = conn.cursor()

        cursor.columns(table='information_schema.tables')
        columns = cursor.fetchall()

        # col[3] = COLUMN_NAME, col[4] = DATA_TYPE
        col_types = {col[3].lower(): col[4] for col in columns if col[3]}

        # STRING 列必须上报为 SQL_WVARCHAR (-9); Power BI / Power Query 据此
        # 走 SQL_C_WCHAR 取数, 避免在 zh-CN Windows 上把 UTF-8 按 GBK 解码.
        self.assert_in('table_name', col_types,
                       "information_schema.tables should expose table_name")
        self.assert_equals(col_types['table_name'], pyodbc.SQL_WVARCHAR,
                           "STRING column 'table_name' must report SQL_WVARCHAR")

        conn.close()

    def test_sqldescribecol(self):
        """测试 SQLDescribeCol - 描述列属性"""
        import pyodbc

        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT 1 as num, 'test' as str, 3.14 as pi")

        # pyodbc uses cursor.description instead of cursor.describe()
        # description returns a list of tuples: (name, type_code, display_size, internal_size, precision, scale, nullable)
        desc = cursor.description
        self.assert_not_none(desc, "Cursor description should not be None")
        self.assert_equals(len(desc), 3, "Should have 3 columns")

        # Check first column (num)
        self.assert_not_none(desc[0][0], "Column 1 name should not be None")

        # Check second column (str)
        self.assert_not_none(desc[1][0], "Column 2 name should not be None")

        # Check third column (pi)
        self.assert_not_none(desc[2][0], "Column 3 name should not be None")

        # 真实类型码走原生 SQLDescribeCol 验证 (pyodbc description 的 type_code
        # 在 5.x 是 Python 类型对象, 无法表达 SQL_WVARCHAR):
        # STRING 字面量列必须上报 SQL_WVARCHAR, 让 Power BI / Wide-API 消费者
        # 通过 SQL_C_WCHAR 直接拿 UTF-16, 避免 zh-CN Windows 上的 GBK 误解码.
        types = _query_column_types("SELECT 1 as num, 'test' as str, 3.14 as pi", 3)
        self.assert_equals(types[0][0], pyodbc.SQL_BIGINT,
                           "Integer literal column should report SQL_BIGINT")
        self.assert_equals(types[1][0], pyodbc.SQL_WVARCHAR,
                           "STRING literal column should report SQL_WVARCHAR")
        self.assert_equals(types[2][0], pyodbc.SQL_DOUBLE,
                           "Float literal column should report SQL_DOUBLE")

        # pyodbc 层保留粗粒度断言: 文本列映射为 str
        self.assert_equals(desc[1][1], str,
                           "STRING literal column should map to str in pyodbc")

        conn.close()

    def test_sqlcolattribute(self):
        """测试 SQLColAttribute - 获取列属性"""
        import pyodbc

        conn = self.connect()
        cursor = conn.cursor()

        # 同时验证 STRING / 整数两条映射, Power BI 走 SQLColAttribute 探测.
        cursor.execute("SELECT 1 as test_col, 'hello' as text_col")

        desc = cursor.description
        self.assert_not_none(desc, "Cursor description should not be None")
        self.assert_equals(len(desc), 2, "Should have 2 columns")

        # Column name is at index 0
        self.assert_not_none(desc[0][0], "Column name should not be None")
        self.assert_not_none(desc[1][0], "Column name should not be None")

        # pyodbc 没有 getcolattr 方法, 走原生 SQLColAttribute 拿
        # SQL_DESC_CONCISE_TYPE (pyodbc description 的 type_code 在 5.x
        # 是 Python 类型对象, 无法表达 SQL_WVARCHAR).
        types = _query_column_types("SELECT 1 as test_col, 'hello' as text_col", 2)
        self.assert_equals(types[0][1], pyodbc.SQL_BIGINT,
                           "Integer literal should report SQL_BIGINT")
        self.assert_equals(types[1][1], pyodbc.SQL_WVARCHAR,
                           "STRING literal should report SQL_WVARCHAR")

        conn.close()
    
    def test_sqlnumresultcols(self):
        """测试 SQLNumResultCols - 获取结果列数"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1, 2, 3")
        
        # 获取列数
        num_cols = cursor.description
        self.assert_equals(len(num_cols), 3, "Should have 3 columns")
        
        conn.close()
    
    def test_cursor_description(self):
        """测试 cursor.description"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 as col1, 'test' as col2, 3.14 as col3")
        
        # 检查描述
        description = cursor.description
        self.assert_equals(len(description), 3, "Should have 3 columns")
        
        # 检查列名
        col_names = [col[0] for col in description]
        self.assert_in('col1', col_names, "col1 should be in column names")
        self.assert_in('col2', col_names, "col2 should be in column names")
        self.assert_in('col3', col_names, "col3 should be in column names")
        
        conn.close()
    
    def test_sqlrowcount(self):
        """测试 SQLRowCount - 获取受影响的行数"""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
        rows = cursor.fetchall()

        # pyodbc's cursor.rowcount may return -1 or 0 for SELECT statements
        # We verify row count by checking the actual fetched rows
        row_count = len(rows)
        self.assert_greater(row_count, 0, "Row count should be greater than 0")

        conn.close()
    
    def test_table_types(self):
        """测试表类型"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 获取所有表类型
        cursor.tables()
        all_tables = cursor.fetchall()
        
        # 获取只有 TABLE 类型的表
        cursor.tables(tableType='TABLE')
        tables = cursor.fetchall()
        
        # 获取 VIEW 类型的表
        cursor.tables(tableType='VIEW')
        views = cursor.fetchall()
        
        self.assert_greater(len(all_tables), 0, "Should return some tables")
        
        conn.close()
    
    def test_column_info(self):
        """测试列详细信息"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 执行查询
        cursor.execute("SELECT CAST(1 AS BIGINT) as bigint_col, CAST('test' AS STRING) as str_col")
        
        # 获取列信息
        for i in range(1, 3):
            name, type_code, display_size, internal_size, precision, scale, nullable = cursor.description[i-1]
            self.assert_not_none(name, f"Column {i} name should not be None")
        
        conn.close()
    
    def test_schema_tables(self):
        """测试获取特定 schema 的表"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 获取 default 的表
        cursor.tables(catalog=None, schema='default', table=None, tableType='TABLE')
        tables = cursor.fetchall()
        
        self.assert_greater(len(tables), 0, "default schema should have tables")
        
        conn.close()

def run_all_tests():
    """运行所有元数据测试"""
    print_section("Metadata Tests")
    
    # 检查配置
    from config import config
    is_valid, message = config.validate()
    if not is_valid:
        print(f"✗ Configuration error: {message}")
        return False
    
    # 运行测试
    test = TestMetadata()
    
    tests = [
        ("SQLTables", test.test_sqltables),
        ("SQLColumns", test.test_sqlcolumns),
        ("SQLColumns Unicode Types", test.test_sqlcolumns_unicode_types),
        ("SQLDescribeCol", test.test_sqldescribecol),
        ("SQLColAttribute", test.test_sqlcolattribute),
        ("SQLNumResultCols", test.test_sqlnumresultcols),
        ("Cursor Description", test.test_cursor_description),
        ("SQLRowCount", test.test_sqlrowcount),
        ("Table Types", test.test_table_types),
        ("Column Info", test.test_column_info),
        ("Schema Tables", test.test_schema_tables),
    ]
    
    for test_name, test_func in tests:
        test.run_test(test_func, test_name)
    
    return test.print_results()


if __name__ == "__main__":
    import pyodbc
    success = run_all_tests()
    sys.exit(0 if success else 1)