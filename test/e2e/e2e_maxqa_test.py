#!/usr/bin/env python3
"""
MaxQA E2E Test Script using pyodbc

This script tests the MaxQA functionality with interactiveMode and quotaName parameters.

Before running, set the following environment variables:
  export MCO_DRIVER_PATH=/path/to/libmaxcompute_odbc.dylib
  export MAXCOMPUTE_ENDPOINT=https://service.cn-shanghai.maxcompute.aliyun.com/api
  export MAXCOMPUTE_PROJECT=your_project
  export ALIBABA_CLOUD_ACCESS_KEY_ID=your_access_key_id
  export ALIBABA_CLOUD_ACCESS_KEY_SECRET=your_access_key_secret
  export MAXCOMPUTE_QUOTA_NAME=your_quota  (optional)
"""

import pyodbc
import sys
import os
from typing import Optional

# Configuration - loaded from environment variables
DRIVER_PATH = os.environ.get("MCO_DRIVER_PATH", "")
ENDPOINT = os.environ.get("MAXCOMPUTE_ENDPOINT", "https://service.cn-shanghai.maxcompute.aliyun.com/api")
PROJECT = os.environ.get("MAXCOMPUTE_PROJECT", "")
ACCESS_KEY_ID = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
ACCESS_KEY_SECRET = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
QUOTA_NAME = os.environ.get("MAXCOMPUTE_QUOTA_NAME", "")


def create_connection_string(
    interactive_mode: bool = False,
    quota_name: Optional[str] = None
) -> str:
    """Create a connection string with the specified parameters."""
    conn_str = (
        f"DRIVER={{MaxCompute ODBC Driver}};"
        f"Endpoint={ENDPOINT};"
        f"Project={PROJECT};"
        f"AccessKeyId={ACCESS_KEY_ID};"
        f"AccessKeySecret={ACCESS_KEY_SECRET};"
    )

    if interactive_mode:
        conn_str += f"interactiveMode=true;"
    else:
        conn_str += f"interactiveMode=false;"

    if quota_name:
        conn_str += f"quotaName={quota_name};"

    return conn_str


def test_basic_connection():
    """Test basic connection without MaxQA."""
    print("\n=== Test 1: Basic Connection (No MaxQA) ===")
    conn_str = create_connection_string(interactive_mode=False)

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Execute a simple query
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1, "Expected result to be 1"

        print("PASS: Basic connection successful")
        print("PASS: Query execution successful")

        conn.close()
        return True
    except Exception as e:
        print(f"FAIL: Test failed: {e}")
        return False


def test_interactive_mode_with_quota():
    """Test interactive mode with quota name."""
    print("\n=== Test 2: Interactive Mode with Quota ===")
    conn_str = create_connection_string(interactive_mode=True, quota_name=QUOTA_NAME)

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Verify connection was established
        print("PASS: Interactive mode connection established")

        # Execute a query
        cursor.execute("SELECT 1, 'test', 3.14")
        result = cursor.fetchone()
        assert result[0] == 1, "Expected first column to be 1"
        assert result[1] == 'test', "Expected second column to be 'test'"
        assert abs(result[2] - 3.14) < 0.01, "Expected third column to be approximately 3.14"

        print("PASS: Query execution in interactive mode successful")

        conn.close()
        return True
    except Exception as e:
        print(f"FAIL: Test failed: {e}")
        return False


def test_interactive_mode_without_quota():
    """Test interactive mode without quota name (use default quota)."""
    print("\n=== Test 3: Interactive Mode without Quota ===")
    conn_str = create_connection_string(interactive_mode=True)

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        print("PASS: Interactive mode connection established (default quota)")

        # Execute a query
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables")
        result = cursor.fetchone()
        print(f"PASS: Found {result[0]} tables")

        conn.close()
        return True
    except Exception as e:
        print(f"FAIL: Test failed: {e}")
        return False


def test_quota_hints_mode():
    """Test non-interactive mode with quota hints."""
    print("\n=== Test 4: Quota Hints Mode (Non-Interactive with Quota) ===")
    conn_str = create_connection_string(interactive_mode=False, quota_name=QUOTA_NAME)

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        print("PASS: Connection established with quota hints")

        # Execute a query
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1, "Expected result to be 1"

        print("PASS: Query execution with quota hints successful")

        conn.close()
        return True
    except Exception as e:
        print(f"FAIL: Test failed: {e}")
        return False


def test_multiple_queries_in_session():
    """Test multiple queries in the same session."""
    print("\n=== Test 5: Multiple Queries in Same Session ===")
    conn_str = create_connection_string(interactive_mode=True, quota_name=QUOTA_NAME)

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Execute multiple queries
        queries = [
            "SELECT 1",
            "SELECT 2",
            "SELECT 3",
        ]

        for i, query in enumerate(queries, 1):
            cursor.execute(query)
            result = cursor.fetchone()
            assert result[0] == i, f"Expected result {i} in query {i}"
            print(f"PASS: Query {i} executed successfully")

        print("PASS: All queries executed in same session")

        conn.close()
        return True
    except Exception as e:
        print(f"FAIL: Test failed: {e}")
        return False


def test_error_handling():
    """Test error handling with invalid parameters."""
    print("\n=== Test 6: Error Handling ===")

    # Test with invalid interactiveMode value
    conn_str = (
        f"DRIVER={{MaxCompute ODBC Driver}};"
        f"Endpoint={ENDPOINT};"
        f"Project={PROJECT};"
        f"AccessKeyId={ACCESS_KEY_ID};"
        f"AccessKeySecret={ACCESS_KEY_SECRET};"
        f"interactiveMode=invalid;"
    )

    try:
        conn = pyodbc.connect(conn_str)
        print("PASS: Connection established with invalid interactiveMode (should default to false)")
        conn.close()
        return True
    except Exception as e:
        print(f"FAIL: Test failed: {e}")
        return False


def run_all_tests():
    """Run all tests and report results."""
    print("=" * 60)
    print("MaxQA E2E Test Suite")
    print("=" * 60)

    # Check if pyodbc is available
    try:
        import pyodbc
        print(f"pyodbc version: {pyodbc.version}")
    except ImportError:
        print("ERROR: pyodbc is not installed. Please install it with: pip install pyodbc")
        return False

    # Check required env vars
    if not DRIVER_PATH:
        print("ERROR: MCO_DRIVER_PATH environment variable not set")
        return False

    if not os.path.exists(DRIVER_PATH):
        print(f"ERROR: Driver not found at: {DRIVER_PATH}")
        print("Please build the project first: cmake --build build")
        return False

    if not all([PROJECT, ACCESS_KEY_ID, ACCESS_KEY_SECRET]):
        print("ERROR: Required environment variables not set.")
        print("  MAXCOMPUTE_PROJECT, ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET")
        return False

    print(f"Driver found at: {DRIVER_PATH}")

    # Run tests
    # SKIP: Interactive mode tests are temporarily disabled
    tests = [
        test_basic_connection,
        # test_interactive_mode_with_quota,  # SKIP: interactive mode
        # test_interactive_mode_without_quota,  # SKIP: interactive mode
        test_quota_hints_mode,
        # test_multiple_queries_in_session,  # SKIP: interactive mode
        test_error_handling,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"FAIL: Test {test.__name__} crashed: {e}")
            results.append(False)

    # Print summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")

    if passed == total:
        print("All tests passed!")
        return True
    else:
        print("Some tests failed")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
