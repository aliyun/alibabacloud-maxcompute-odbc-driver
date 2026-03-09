# MaxCompute ODBC Driver - Installation Without WiX

## Manual Installation Instructions

If you don't have WiX Toolset installed, you can manually install the MaxCompute ODBC driver:

### Step 1: Copy Required Files

1. Copy the following files from `out\build\x64-Release\bin\Release\` directory:
   - `maxcompute_odbc.dll`
   - `libcrypto-3-x64.dll`
   - `libssl-3-x64.dll`
   - `libprotobuf.dll`
   - `pugixml.dll`
   - `zlib1.dll`

2. Copy these files to the Windows System32 directory:
   - `C:\Windows\System32\` (for 64-bit applications)
   - OR `C:\Windows\SysWOW64\` (for 32-bit applications)

### Step 2: Register the Driver

1. Open Command Prompt as Administrator
2. Navigate to the appropriate system directory:
   ```
   cd C:\Windows\System32
   ```
3. Register the driver:
   ```
   regsvr32 maxcompute_odbc.dll
   ```

### Step 3: Add Registry Entries

The driver needs appropriate registry entries to appear in ODBC Data Source Administrator.

Use Windows Registry Editor (regedit - run as Administrator) to add the following:

HKLM\SOFTWARE\ODBC\ODBCINST.INI\MaxCompute ODBC Driver
- Driver = C:\Windows\System32\maxcompute_odbc.dll
- Setup = C:\Windows\System32\maxcompute_odbc.dll
- APILevel = 2
- ConnectFunctions = YYN
- DriverODBCVer = 03.80
- FileUsage = 0
- SQLLevel = 1

### Step 4: Configure Data Source

1. Open ODBC Data Source Administrator (64-bit):
   - Search for "ODBC Data Sources (64-bit)" in Start Menu
   - OR run `odbcad32.exe` (on 64-bit Windows, this opens the 64-bit version)

2. Go to System DSN or User DSN tab
3. Click "Add" and select "MaxCompute ODBC Driver"
4. Configure your connection parameters:
   - Endpoint: MaxCompute service URL
   - Project: Your MaxCompute project name
   - AccessKey ID and Secret: Your Alibaba Cloud credentials

## Testing the Installation

You can test the installation by:
1. Using the "Test" button in the ODBC configuration dialog
2. Creating a simple connection from Excel, Power BI, or other applications
3. Using the connection string in an application

## Manual Uninstallation

1. Remove registry entries using regedit
2. Delete the DLL files from the system directory
3. Unregister the driver: `regsvr32 /u maxcompute_odbc.dll`

## Installing WiX Toolset for MSI Creation

To generate the MSI installer, install WiX Toolset:

1. Using Chocolatey (recommended):
   ```
   choco install wixtoolset
   ```

2. Or download from: https://wixtoolset.org/releases/

3. After installation, run the build script:
   ```
   scripts\build_installer.bat
   ```

This will create MaxCompute_ODBC_Driver.msi that can be distributed and installed on other systems.