@echo off
setlocal enabledelayedexpansion
REM ==============================================================================
REM MaxCompute ODBC Driver MSI Installer Build Script
REM Prerequisites: Build the driver first (build.bat), then run this script.
REM Dependencies: WiX Toolset (candle.exe, light.exe)
REM Usage: build_installer.bat [version]
REM   version - Product version in x.y.z format (default: 1.0.0)
REM ==============================================================================

echo ==============================================================================
echo MaxCompute ODBC Driver MSI Installer Build
echo ==============================================================================
echo.

REM --- Parse version argument ---
set "PRODUCT_VERSION=1.0.0"
if not "%~1"=="" set "PRODUCT_VERSION=%~1"
set "PRODUCT_VERSION_4=%PRODUCT_VERSION%.0"
echo --- Product version: %PRODUCT_VERSION% (MSI: %PRODUCT_VERSION_4%) ---

REM --- Locate project root ---
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
cd /d "%PROJECT_ROOT%"

REM --- Check WiX Toolset ---
where candle.exe >nul 2>nul
if errorlevel 1 (
    echo Error: WiX Toolset not found.
    echo Please install from https://wixtoolset.org/releases/
    echo Or via Chocolatey: choco install wixtoolset
    exit /b 1
)

where light.exe >nul 2>nul
if errorlevel 1 (
    echo Error: WiX Toolset not found.
    exit /b 1
)

echo --- WiX Toolset found ---

REM --- Set directory variables ---
set "BUILD_DIR=%PROJECT_ROOT%\build_win64"
set "SOURCE_DIR=%BUILD_DIR%\bin"
set "VCPKG_BIN=%BUILD_DIR%\vcpkg_installed\x64-windows\bin"
set "OUTPUT_DIR=%PROJECT_ROOT%\installer_build"
set "DLL_NAME=maxcompute_odbc.dll"

REM --- Create output directory ---
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

REM --- Check driver DLL exists ---
if not exist "%SOURCE_DIR%\%DLL_NAME%" (
    echo Error: Driver DLL not found: %SOURCE_DIR%\%DLL_NAME%
    echo Please build the project first (build.bat)
    exit /b 1
)

echo --- Found driver DLL: %SOURCE_DIR%\%DLL_NAME% ---
echo.

REM --- Copy driver DLL ---
echo --- Copying files to installer build directory... ---
copy /Y "%SOURCE_DIR%\%DLL_NAME%" "%OUTPUT_DIR%\%DLL_NAME%" >nul
if errorlevel 1 (
    echo Error: Failed to copy %DLL_NAME%
    exit /b 1
)

REM --- Copy dependency DLLs from vcpkg ---
echo --- Copying dependency DLLs... ---

REM OpenSSL
if exist "%VCPKG_BIN%\libcrypto-3-x64.dll" (
    copy /Y "%VCPKG_BIN%\libcrypto-3-x64.dll" "%OUTPUT_DIR%\" >nul
)
if exist "%VCPKG_BIN%\libssl-3-x64.dll" (
    copy /Y "%VCPKG_BIN%\libssl-3-x64.dll" "%OUTPUT_DIR%\" >nul
)

REM protobuf
if exist "%VCPKG_BIN%\libprotobuf.dll" (
    copy /Y "%VCPKG_BIN%\libprotobuf.dll" "%OUTPUT_DIR%\" >nul
)

REM pugixml
if exist "%VCPKG_BIN%\pugixml.dll" (
    copy /Y "%VCPKG_BIN%\pugixml.dll" "%OUTPUT_DIR%\" >nul
)

REM zlib
if exist "%VCPKG_BIN%\zlib1.dll" (
    copy /Y "%VCPKG_BIN%\zlib1.dll" "%OUTPUT_DIR%\" >nul
)

REM crc32c
if exist "%VCPKG_BIN%\crc32c.dll" (
    copy /Y "%VCPKG_BIN%\crc32c.dll" "%OUTPUT_DIR%\" >nul
)

REM date-tz
if exist "%VCPKG_BIN%\date-tz.dll" (
    copy /Y "%VCPKG_BIN%\date-tz.dll" "%OUTPUT_DIR%\" >nul
)

echo --- File copy complete ---
echo.

REM --- Generate License.rtf from LICENSE ---
echo --- Generating License.rtf... ---
python "%PROJECT_ROOT%\scripts\generate_license_rtf.py"
if errorlevel 1 (
    echo Error: Failed to generate License.rtf
    exit /b 1
)

REM --- Check Product.wxs ---
if not exist "%PROJECT_ROOT%\Product.wxs" (
    echo Error: Product.wxs not found
    exit /b 1
)

REM --- Compile WiX source ---
echo --- Compiling WiX source... ---
candle.exe "%PROJECT_ROOT%\Product.wxs" -dProductVersion=%PRODUCT_VERSION_4% -out "%OUTPUT_DIR%\Product.wixobj"
if errorlevel 1 (
    echo Error: WiX compilation failed
    exit /b 1
)

REM --- Link to produce MSI ---
echo --- Linking MSI installer... ---
light.exe "%OUTPUT_DIR%\Product.wixobj" -ext WixUIExtension -ext WixUtilExtension -out "%PROJECT_ROOT%\MaxCompute_ODBC_Driver.msi"
if errorlevel 1 (
    echo Error: MSI linking failed
    exit /b 1
)

echo.
echo ==============================================================================
echo Build succeeded!
echo ==============================================================================
echo.
echo MSI installer: %PROJECT_ROOT%\MaxCompute_ODBC_Driver.msi
echo.
echo Install:
echo   msiexec /i MaxCompute_ODBC_Driver.msi
echo.
echo Silent install:
echo   msiexec /i MaxCompute_ODBC_Driver.msi /quiet
echo.
echo Uninstall:
echo   msiexec /x MaxCompute_ODBC_Driver.msi
echo.

endlocal
