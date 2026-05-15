"""
SQL_C_CHAR 编码测试 (ClientCharset 连接串参数).

pyodbc 默认走 SQL_C_WCHAR 路径取字符串列, 不会触发 driver 的 charset 转换.
这里通过 conn.setdecoding(SQL_CHAR, ctype=SQL_C_CHAR) 强制 pyodbc 调
SQLGetData(SQL_C_CHAR), 再用 latin-1 解码以拿回驱动写入的裸字节, 直接对比
预期编码下的字节序列.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pyodbc
from base_test import BaseTest, print_section
from config import config


# "你好" 在不同编码下的字节
NIHAO_UTF8 = b"\xe4\xbd\xa0\xe5\xa5\xbd"
NIHAO_GBK = b"\xc4\xe3\xba\xc3"
NIHAO_GB18030 = b"\xc4\xe3\xba\xc3"  # 与 GBK 在 BMP 上一致


class TestCharset(BaseTest):
    """ClientCharset 连接串参数 e2e 测试"""

    def _connect_with_charset(self, charset=None) -> pyodbc.Connection:
        conn_str = (
            f"DRIVER={{MaxCompute ODBC Driver}};"
            f"Endpoint={config.endpoint};"
            f"Project={config.project};"
            f"AccessKeyId={config.access_key_id};"
            f"AccessKeySecret={config.access_key_secret};"
            f"interactiveMode=false;"
        )
        if charset is not None:
            conn_str += f"ClientCharset={charset};"
        conn = pyodbc.connect(conn_str)
        # 强制 SQL_CHAR 列用 SQL_C_CHAR 取, 用 latin-1 解码 (1:1 字节映射,
        # 不会丢字节, 便于 .encode('latin-1') 回拿裸字节).
        # pyodbc 没暴露 SQL_C_CHAR; 它和 SQL_CHAR 在 ODBC 头里同值 (1).
        conn.setdecoding(pyodbc.SQL_CHAR, encoding="latin-1",
                         ctype=pyodbc.SQL_CHAR)
        return conn

    def _read_chinese_bytes(self, conn) -> bytes:
        cursor = conn.cursor()
        cursor.execute("SELECT '你好'")
        row = cursor.fetchone()
        cursor.close()
        self.assert_not_none(row, "Result row should not be None")
        s = row[0]
        self.assert_true(isinstance(s, str),
                         f"Expected str, got {type(s).__name__}")
        return s.encode("latin-1")

    def test_default_charset_is_utf8(self):
        """不传 ClientCharset → SQL_C_CHAR 默认输出 UTF-8 字节"""
        conn = self._connect_with_charset(charset=None)
        try:
            data = self._read_chinese_bytes(conn)
            print(f"  default bytes: {data.hex()}")
            self.assert_equals(data, NIHAO_UTF8,
                               "Default ClientCharset should produce UTF-8 bytes")
        finally:
            conn.close()

    def test_explicit_utf8(self):
        """ClientCharset=UTF-8 → UTF-8 字节"""
        conn = self._connect_with_charset(charset="UTF-8")
        try:
            data = self._read_chinese_bytes(conn)
            print(f"  utf-8 bytes:   {data.hex()}")
            self.assert_equals(data, NIHAO_UTF8,
                               "ClientCharset=UTF-8 should produce UTF-8 bytes")
        finally:
            conn.close()

    def test_gbk(self):
        """ClientCharset=GBK → GBK 字节 (验证修复用户报告的中文乱码)"""
        conn = self._connect_with_charset(charset="GBK")
        try:
            data = self._read_chinese_bytes(conn)
            print(f"  gbk bytes:     {data.hex()}")
            self.assert_equals(data, NIHAO_GBK,
                               "ClientCharset=GBK should produce GBK bytes")
        finally:
            conn.close()

    def test_gbk_lowercase(self):
        """ClientCharset 参数大小写不敏感"""
        conn = self._connect_with_charset(charset="gbk")
        try:
            data = self._read_chinese_bytes(conn)
            self.assert_equals(data, NIHAO_GBK,
                               "Lowercase 'gbk' should also produce GBK bytes")
        finally:
            conn.close()

    def test_gb18030(self):
        """ClientCharset=GB18030 → GB18030 字节"""
        conn = self._connect_with_charset(charset="GB18030")
        try:
            data = self._read_chinese_bytes(conn)
            print(f"  gb18030 bytes: {data.hex()}")
            self.assert_equals(data, NIHAO_GB18030,
                               "ClientCharset=GB18030 should produce GB18030 bytes")
        finally:
            conn.close()


if __name__ == "__main__":
    test = TestCharset()
    print_section("SQL_C_CHAR ClientCharset E2E Tests")
    test.run_test(test.test_default_charset_is_utf8,
                  "default ClientCharset → UTF-8 bytes")
    test.run_test(test.test_explicit_utf8, "ClientCharset=UTF-8 → UTF-8 bytes")
    test.run_test(test.test_gbk, "ClientCharset=GBK → GBK bytes")
    test.run_test(test.test_gbk_lowercase, "ClientCharset=gbk (case-insensitive)")
    test.run_test(test.test_gb18030, "ClientCharset=GB18030 → GB18030 bytes")
    success = test.print_results()
    sys.exit(0 if success else 1)
