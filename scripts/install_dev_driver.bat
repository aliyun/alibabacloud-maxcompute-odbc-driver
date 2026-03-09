@echo off
REM ==============================================================================
REM MaxCompute ODBC Driver - 开发版驱动注册脚本
REM 将编译好的 DLL 注册到 Windows ODBC，用于 e2e 测试
REM 需要以管理员权限运行
REM ==============================================================================

setlocal enabledelayedexpansion

REM --- 检查管理员权限 ---
net session >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo 错误: 此脚本需要管理员权限
    echo 请右键选择"以管理员身份运行"
    pause
    exit /b 1
)

REM --- 定位项目根目录 ---
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"

REM --- 设置驱动路径 ---
set "BUILD_DIR=%PROJECT_ROOT%\build_win64"
set "DRIVER_DLL=%BUILD_DIR%\bin\maxcompute_odbc.dll"

REM --- 检查 DLL 是否存在 ---
if not exist "%DRIVER_DLL%" (
    echo 错误: 未找到驱动 DLL
    echo 路径: %DRIVER_DLL%
    echo.
    echo 请先运行 build.bat 编译项目
    pause
    exit /b 1
)

echo ==============================================================================
echo MaxCompute ODBC Driver - 开发版安装
echo ==============================================================================
echo 驱动路径: %DRIVER_DLL%
echo.

REM --- 复制依赖 DLL 到驱动目录 ---
echo --- 复制 vcpkg 依赖 DLL... ---
set "VCPKG_BIN=%BUILD_DIR%\vcpkg_installed\x64-windows\bin"

if exist "%VCPKG_BIN%" (
    copy /Y "%VCPKG_BIN%\libcrypto-3-x64.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    copy /Y "%VCPKG_BIN%\libssl-3-x64.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    copy /Y "%VCPKG_BIN%\libprotobuf.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    copy /Y "%VCPKG_BIN%\pugixml.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    copy /Y "%VCPKG_BIN%\zlib1.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    copy /Y "%VCPKG_BIN%\crc32c.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    copy /Y "%VCPKG_BIN%\date-tz.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    echo 依赖 DLL 已复制
) else (
    echo 警告: vcpkg bin 目录不存在，跳过依赖复制
)

REM --- 注册驱动到 ODBC ---
echo.
echo --- 注册 ODBC 驱动... ---

set "DRIVER_NAME=MaxCompute ODBC Driver"
set "REG_PATH=HKLM\SOFTWARE\ODBC\ODBCINST.INI\%DRIVER_NAME%"

REM 删除旧注册（如果存在）
reg delete "%REG_PATH%" /f >nul 2>&1

REM 添加驱动注册表项
reg add "%REG_PATH%" /v "Driver" /t REG_SZ /d "%DRIVER_DLL%" /f >nul
reg add "%REG_PATH%" /v "Setup" /t REG_SZ /d "%DRIVER_DLL%" /f >nul
reg add "%REG_PATH%" /v "APILevel" /t REG_SZ /d "2" /f >nul
reg add "%REG_PATH%" /v "ConnectFunctions" /t REG_SZ /d "YYN" /f >nul
reg add "%REG_PATH%" /v "DriverODBCVer" /t REG_SZ /d "03.80" /f >nul
reg add "%REG_PATH%" /v "FileUsage" /t REG_SZ /d "0" /f >nul
reg add "%REG_PATH%" /v "SQLLevel" /t REG_SZ /d "1" /f >nul

REM 添加到驱动列表
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "%DRIVER_NAME%" /t REG_SZ /d "Installed" /f >nul

if %ERRORLEVEL% equ 0 (
    echo.
    echo ==============================================================================
    echo 安装成功！
    echo ==============================================================================
    echo 驱动名称: %DRIVER_NAME%
    echo 驱动路径: %DRIVER_DLL%
    echo.
    echo 现在可以运行 e2e 测试:
    echo   cd test\e2e
    echo   python runner.py
    echo.
    echo 或在 ODBC 数据源管理器中配置 DSN
    echo ==============================================================================
) else (
    echo 错误: 注册失败
    exit /b 1
)

endlocal
