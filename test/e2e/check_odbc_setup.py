#!/usr/bin/env python3
"""
Minimal test to verify ODBC driver setup
"""

import sys
import os

print("=" * 60)
print("ODBC Driver Setup Verification")
print("=" * 60)

# Check pyodbc
try:
    import pyodbc
    print(f"pyodbc version: {pyodbc.version}")
except ImportError:
    print("ERROR: pyodbc not installed")
    sys.exit(1)

# List available drivers
drivers = pyodbc.drivers()
print(f"\nAvailable ODBC drivers ({len(drivers)}):")
for driver in drivers:
    print(f"  - {driver}")

# Check if MaxCompute driver is available
if "MaxCompute ODBC Driver" in drivers:
    print("\nMaxCompute ODBC Driver found!")
else:
    print("\nMaxCompute ODBC Driver NOT found!")
    print("\nTo install the driver, you may need to:")
    print("1. Copy the driver dylib/so/dll to a known location")
    print("2. Register the driver in odbcinst.ini")
    print("3. Or use a DSN-less connection with absolute path")

    # Try to find driver file via environment variable
    driver_path = os.environ.get("MCO_DRIVER_PATH", "")
    if driver_path and os.path.exists(driver_path):
        print(f"\nDriver file exists at: {driver_path}")
    elif driver_path:
        print(f"\nDriver file NOT found at: {driver_path}")
    else:
        print("\nSet MCO_DRIVER_PATH environment variable to specify driver location.")

print("\n" + "=" * 60)
