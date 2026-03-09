#!/usr/bin/env python3
"""
Quick E2E Test for MaxQA functionality

Before running, set the following environment variables:
  export MCO_DRIVER_PATH=/path/to/libmaxcompute_odbc.dylib
  export MAXCOMPUTE_ENDPOINT=https://service.cn-shanghai.maxcompute.aliyun.com/api
  export MAXCOMPUTE_PROJECT=your_project
  export ALIBABA_CLOUD_ACCESS_KEY_ID=your_access_key_id
  export ALIBABA_CLOUD_ACCESS_KEY_SECRET=your_access_key_secret
"""

import pyodbc
import sys
import os

# Configuration - loaded from environment variables
DRIVER_PATH = os.environ.get("MCO_DRIVER_PATH", "")
ENDPOINT = os.environ.get("MAXCOMPUTE_ENDPOINT", "")
PROJECT = os.environ.get("MAXCOMPUTE_PROJECT", "")
ACCESS_KEY_ID = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
ACCESS_KEY_SECRET = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")


def test_connection(conn_str, test_name):
    """Test a connection string."""
    print(f"\nTesting: {test_name}")
    print(f"Connection string: {conn_str}")

    try:
        # Enable pyodbc tracing for debugging
        pyodbc.pooling = False  # Disable connection pooling

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Simple test query
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        if result[0] == 1:
            print(f"PASS: {test_name}")
            conn.close()
            return True
        else:
            print(f"FAIL: {test_name} - Unexpected result")
            conn.close()
            return False
    except Exception as e:
        print(f"FAIL: {test_name} - {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("=" * 60)
    print("MaxQA Quick E2E Test")
    print("=" * 60)

    # Check prerequisites
    try:
        import pyodbc
        print(f"pyodbc version: {pyodbc.version}")
    except ImportError:
        print("ERROR: pyodbc not installed. Run: pip install pyodbc")
        return False

    if not DRIVER_PATH:
        print("ERROR: MCO_DRIVER_PATH environment variable not set")
        return False

    if not os.path.exists(DRIVER_PATH):
        print(f"ERROR: Driver not found: {DRIVER_PATH}")
        return False

    if not all([ENDPOINT, PROJECT, ACCESS_KEY_ID, ACCESS_KEY_SECRET]):
        print("ERROR: Required environment variables not set.")
        print("  MAXCOMPUTE_ENDPOINT, MAXCOMPUTE_PROJECT,")
        print("  ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET")
        return False

    print(f"Driver found: {DRIVER_PATH}")

    # Build connection strings - try different formats
    test_formats = [
        # Format 1: Standard format
        f"DRIVER={{MaxCompute ODBC Driver}};SERVER={ENDPOINT};Project={PROJECT};Uid={ACCESS_KEY_ID};Pwd={ACCESS_KEY_SECRET};LOGLEVEL=DEBUG;interactiveMode=false;",

        # Format 2: With explicit endpoint
        f"DRIVER={{MaxCompute ODBC Driver}};SERVER={ENDPOINT};Endpoint={ENDPOINT};Project={PROJECT};Uid={ACCESS_KEY_ID};Pwd={ACCESS_KEY_SECRET};LOGLEVEL=DEBUG;interactiveMode=false;",

        # Format 3: With AccessKeyId/AccessKeySecret
        f"DRIVER={{MaxCompute ODBC Driver}};SERVER={ENDPOINT};Project={PROJECT};AccessKeyId={ACCESS_KEY_ID};AccessKeySecret={ACCESS_KEY_SECRET};LOGLEVEL=DEBUG;interactiveMode=false;",
    ]

    # Test cases
    tests = [
        (test_formats[0], "Format 1: Standard with Uid/Pwd"),
        (test_formats[1], "Format 2: With explicit Endpoint"),
        (test_formats[2], "Format 3: With AccessKeyId/AccessKeySecret"),
    ]

    # Run tests
    results = []
    for conn_str, name in tests:
        result = test_connection(conn_str, name)
        results.append(result)
        if not result:
            print(f"\n  Trying next format...\n")

    # Summary
    print("\n" + "=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} passed")

    if passed == total:
        print("All tests PASSED!")
        return True
    else:
        print(f"{total - passed} test(s) FAILED")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
