#!/usr/bin/env python3
"""
Test using environment variables for credentials

Before running, set the following environment variables:
  export ALIBABA_CLOUD_ENDPOINT=https://service.cn-shanghai.maxcompute.aliyun.com/api
  export ALIBABA_CLOUD_PROJECT=your_project
  export ALIBABA_CLOUD_ACCESS_KEY_ID=your_access_key_id
  export ALIBABA_CLOUD_ACCESS_KEY_SECRET=your_access_key_secret
"""

import pyodbc
import sys
import os

# Verify required environment variables are set
required_vars = [
    'ALIBABA_CLOUD_ENDPOINT',
    'ALIBABA_CLOUD_PROJECT',
    'ALIBABA_CLOUD_ACCESS_KEY_ID',
    'ALIBABA_CLOUD_ACCESS_KEY_SECRET',
]

missing = [v for v in required_vars if not os.environ.get(v)]
if missing:
    print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
    print("Please set them before running this test.")
    sys.exit(1)

# Minimal connection string - just driver and log level
conn_str = "DRIVER={MaxCompute ODBC Driver};LOGLEVEL=DEBUG;interactiveMode=false;"

print("=" * 60)
print("Testing with Environment Variables")
print("=" * 60)
print(f"Connection string: {conn_str}")
print(f"\nEnvironment variables set:")
print(f"  ALIBABA_CLOUD_ENDPOINT = {os.environ.get('ALIBABA_CLOUD_ENDPOINT')}")
print(f"  ALIBABA_CLOUD_PROJECT = {os.environ.get('ALIBABA_CLOUD_PROJECT')}")
print(f"  ALIBABA_CLOUD_ACCESS_KEY_ID = {os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_ID', '')[:4]}****")
print(f"  ALIBABA_CLOUD_ACCESS_KEY_SECRET = {'*' * 8}")
print()

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("SELECT 1")
    result = cursor.fetchone()

    if result[0] == 1:
        print("PASS: Connection successful with environment variables!")
        conn.close()
        sys.exit(0)
    else:
        print("FAIL: Unexpected result")
        conn.close()
        sys.exit(1)
except Exception as e:
    print(f"FAIL: Connection failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
