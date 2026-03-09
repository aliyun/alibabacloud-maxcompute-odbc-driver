#!/usr/bin/env python3
"""
MaxQA E2E Test using environment variables
This test bypasses the connection string parsing issue by using environment variables.

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

# Configuration - loaded from environment variables
DRIVER_PATH = os.environ.get("MCO_DRIVER_PATH", "")
ENDPOINT = os.environ.get("MAXCOMPUTE_ENDPOINT", "")
PROJECT = os.environ.get("MAXCOMPUTE_PROJECT", "")
ACCESS_KEY_ID = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
ACCESS_KEY_SECRET = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
QUOTA_NAME = os.environ.get("MAXCOMPUTE_QUOTA_NAME", "")


def test_connection(config, test_name):
    """Test a connection with given configuration."""
    print(f"\n{'=' * 60}")
    print(f"Testing: {test_name}")
    print(f"{'=' * 60}")

    # Set environment variables
    os.environ['ALIBABA_CLOUD_ENDPOINT'] = config.get('endpoint', ENDPOINT)
    os.environ['ALIBABA_CLOUD_PROJECT'] = config.get('project', PROJECT)
    os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'] = config.get('access_key_id', ACCESS_KEY_ID)
    os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'] = config.get('access_key_secret', ACCESS_KEY_SECRET)

    # Build connection string with MaxQA parameters
    conn_str = "DRIVER={MaxCompute ODBC Driver};LOGLEVEL=DEBUG;"

    if config.get('interactive_mode') is not None:
        if config['interactive_mode']:
            conn_str += "interactiveMode=true;"
        else:
            conn_str += "interactiveMode=false;"

    if config.get('quota_name'):
        conn_str += f"quotaName={config['quota_name']};"

    print(f"Environment variables set:")
    print(f"  ALIBABA_CLOUD_ENDPOINT = {os.environ['ALIBABA_CLOUD_ENDPOINT']}")
    print(f"  ALIBABA_CLOUD_PROJECT = {os.environ['ALIBABA_CLOUD_PROJECT']}")
    print(f"  ALIBABA_CLOUD_ACCESS_KEY_ID = {os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'][:4]}****")
    print(f"  Connection string: {conn_str}")

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Test query
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        if result[0] == 1:
            print(f"\nPASS: {test_name}")
            conn.close()
            return True
        else:
            print(f"\nFAIL: {test_name} - Unexpected result")
            conn.close()
            return False
    except Exception as e:
        print(f"\nFAIL: {test_name} - {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("=" * 60)
    print("MaxQA E2E Test Suite (Using Environment Variables)")
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

    # Test cases
    # SKIP: Interactive mode tests are temporarily disabled
    tests = [
        (
            {'interactive_mode': False},
            "Standard Mode (No MaxQA)"
        ),
        # (
        #     {'interactive_mode': True},
        #     "Interactive Mode (Default Quota)"
        # ),  # SKIP: interactive mode
        # (
        #     {'interactive_mode': True, 'quota_name': QUOTA_NAME},
        #     "Interactive Mode with Quota"
        # ),  # SKIP: interactive mode
        (
            {'interactive_mode': False, 'quota_name': QUOTA_NAME},
            "Quota Hints Mode (Non-Interactive with Quota)"
        ),
    ]

    # Run tests
    results = []
    for config, name in tests:
        result = test_connection(config, name)
        results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")

    if passed == total:
        print("All tests PASSED!")
        return True
    else:
        print(f"{total - passed} test(s) FAILED")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
