@echo off
REM ==============================================================================
REM Windows 构建脚本 - MaxCompute ODBC Driver
REM ==============================================================================

REM --- 检查是否需要提升管理员权限 (--test 参数) ---
echo %* | findstr /i "\-\-test" >nul
if %ERRORLEVEL% equ 0 (
    net session >nul 2>&1
    if %ERRORLEVEL% neq 0 (
        echo --- 检测到 --test 参数，需要管理员权限，正在提升... ---
        powershell -Command "Start-Process cmd -ArgumentList '/c cd /d ""%CD%"" && ""%~f0"" %*' -Verb RunAs"
        exit /b
    )
)

setlocal enabledelayedexpansion

REM 脚本配置
set "DRIVER_NAME=MaxCompute ODBC Driver"
set "TARGET_LIB_NAME=maxcompute_odbc"

REM ==============================================================================
REM --- 0. 解析命令行参数 ---
REM ==============================================================================
set "BUILD_TYPE=Release"
set "CLEAN_BUILD=0"
set "RUN_TESTS=0"
set "VCPKG_ROOT_OVERRIDE="

:parse_args
if "%~1"=="" goto :done_parsing
if /i "%~1"=="--debug" (
    set "BUILD_TYPE=Debug"
    shift
    goto :parse_args
)
if /i "%~1"=="--clean" (
    set "CLEAN_BUILD=1"
    shift
    goto :parse_args
)
if /i "%~1"=="--test" (
    set "RUN_TESTS=1"
    shift
    goto :parse_args
)
if /i "%~1"=="--vcpkg" (
    set "VCPKG_ROOT_OVERRIDE=%~2"
    shift
    shift
    goto :parse_args
)
if /i "%~1"=="--help" (
    goto :show_help
)
shift
goto :parse_args

:show_help
echo.
echo MaxCompute ODBC Driver Windows 构建脚本
echo.
echo 用法: build.bat [选项]
echo.
echo 选项:
echo   --debug       构建 Debug 版本 (默认: Release)
echo   --clean       清理后重新构建
echo   --test        构建后运行 e2e 测试
echo   --vcpkg PATH  指定 vcpkg 根目录 (覆盖环境变量)
echo   --help        显示此帮助信息
echo.
echo 环境变量:
echo   VCPKG_ROOT    vcpkg 安装目录 (如未设置，默认使用 D:\vcpkg)
echo.
echo e2e 测试环境变量 (--test 时需要):
echo   MAXCOMPUTE_PROJECT              MaxCompute 项目名
echo   ALIBABA_CLOUD_ACCESS_KEY_ID     阿里云 Access Key ID
echo   ALIBABA_CLOUD_ACCESS_KEY_SECRET 阿里云 Access Key Secret
echo   MAXCOMPUTE_ENDPOINT             (可选) 服务端点
echo   MAXCOMPUTE_QUOTA_NAME           (可选) 配额名称
echo.
echo 示例:
echo   build.bat                        构建 Release 版本
echo   build.bat --debug                构建 Debug 版本
echo   build.bat --clean                清理后重新构建
echo   build.bat --test                 构建并运行测试
echo   build.bat --vcpkg C:\vcpkg       使用指定的 vcpkg 目录
echo.
exit /b 0

:done_parsing

REM ==============================================================================
REM --- 1. 检查 VCPKG_ROOT ---
REM ==============================================================================
if not "%VCPKG_ROOT_OVERRIDE%"=="" (
    set "VCPKG_ROOT=%VCPKG_ROOT_OVERRIDE%"
    echo --- 使用命令行指定的 vcpkg 目录: %VCPKG_ROOT% ---
) else if "%VCPKG_ROOT%"=="" (
    REM 默认 vcpkg 路径
    set "VCPKG_ROOT=D:\vcpkg"
    echo --- VCPKG_ROOT 未设置，使用默认路径: %VCPKG_ROOT% ---
) else (
    echo --- 使用环境变量 VCPKG_ROOT: %VCPKG_ROOT% ---
)

REM 检查 vcpkg 目录是否存在
if not exist "%VCPKG_ROOT%" (
    echo 错误: vcpkg 目录不存在: %VCPKG_ROOT%
    echo 请确认 vcpkg 已正确安装，或使用 --vcpkg 参数指定正确路径
    exit /b 1
)

REM 检查 vcpkg toolchain 文件是否存在
set "VCPKG_TOOLCHAIN_FILE=%VCPKG_ROOT%\scripts\buildsystems\vcpkg.cmake"
if not exist "%VCPKG_TOOLCHAIN_FILE%" (
    echo 错误: 在 '%VCPKG_ROOT%' 中找不到 vcpkg.cmake 文件
    echo 请确认 VCPKG_ROOT 是一个有效的 vcpkg 安装目录
    exit /b 1
)

echo --- vcpkg toolchain 文件: %VCPKG_TOOLCHAIN_FILE% ---

REM ==============================================================================
REM --- 2. 获取项目根目录 ---
REM ==============================================================================
REM 尝试多种方式定位项目根目录（包含 CMakeLists.txt 的目录）
set "PROJECT_ROOT="

REM 方法 1: 检查当前目录
if exist "%CD%\CMakeLists.txt" (
    set "PROJECT_ROOT=%CD%"
    goto :found_root
)

REM 方法 2: 检查脚本目录的父目录
for %%I in ("%~dp0..") do (
    if exist "%%~fI\CMakeLists.txt" (
        set "PROJECT_ROOT=%%~fI"
        goto :found_root
    )
)

REM 方法 3: 用户手动指定
if not defined PROJECT_ROOT (
    echo 错误: 无法定位项目根目录
    echo 请在项目根目录下运行此脚本，或设置 PROJECT_ROOT 环境变量
    exit /b 1
)

:found_root
echo --- 项目根目录: %PROJECT_ROOT% ---

REM ==============================================================================
REM --- 3. 设置构建目录 ---
REM ==============================================================================
set "BUILD_DIR=%PROJECT_ROOT%\build_win64"
echo --- 构建目录: %BUILD_DIR% ---
echo --- 构建类型: %BUILD_TYPE% ---

REM 清理构建目录（如果请求）
if "%CLEAN_BUILD%"=="1" (
    if exist "%BUILD_DIR%" (
        echo --- 清理旧的构建目录... ---
        rmdir /s /q "%BUILD_DIR%"
    )
)

REM 创建构建目录
if not exist "%BUILD_DIR%" (
    mkdir "%BUILD_DIR%"
)

REM ==============================================================================
REM --- 4. 设置 Visual Studio 环境 (Ninja 需要) ---
REM ==============================================================================
set "VSWHERE=C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe"
set "VS_PATH="

REM 尝试使用 vswhere 查找 VS
if exist "%VSWHERE%" (
    for /f "usebackq tokens=*" %%i in (`"%VSWHERE%" -latest -property installationPath`) do set "VS_PATH=%%i"
)

REM 如果 vswhere 没找到，尝试默认路径
if "%VS_PATH%"=="" (
    if exist "C:\Program Files\Microsoft Visual Studio\18\Community" (
        set "VS_PATH=C:\Program Files\Microsoft Visual Studio\18\Community"
    )
)

if "%VS_PATH%"=="" (
    echo 错误: 未找到 Visual Studio 安装
    exit /b 1
)

echo --- Visual Studio 路径: %VS_PATH% ---

REM 设置 VS 编译环境
set "VCVARSALL=%VS_PATH%\VC\Auxiliary\Build\vcvarsall.bat"
if not exist "%VCVARSALL%" (
    echo 错误: 未找到 vcvarsall.bat
    exit /b 1
)

echo --- 正在设置 x64 编译环境... ---
call "%VCVARSALL%" x64 >nul 2>&1

REM ==============================================================================
REM --- 5. 检查 CMake 和 Ninja ---
REM ==============================================================================
where cmake >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo 错误: 未找到 cmake，请确保 VS 环境已正确设置
    exit /b 1
)

where ninja >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo 错误: 未找到 ninja，请确保 VS 环境已正确设置
    exit /b 1
)

echo --- CMake 版本: ---
cmake --version
echo --- Ninja 版本: ---
ninja --version

REM ==============================================================================
REM --- 6. 运行 CMake 配置 (使用 Ninja) ---
REM ==============================================================================
echo.
echo ==============================================================================
echo --- 正在运行 CMake 配置 (Ninja)... ---
echo ==============================================================================

cmake -B "%BUILD_DIR%" ^
      -S "%PROJECT_ROOT%" ^
      -G Ninja ^
      -DCMAKE_BUILD_TYPE=%BUILD_TYPE% ^
      -DCMAKE_TOOLCHAIN_FILE="%VCPKG_TOOLCHAIN_FILE%" ^
      -DVCPKG_TARGET_TRIPLET=x64-windows ^
      -DVCPKG_APPLOCAL_DEPS=OFF

if %ERRORLEVEL% neq 0 (
    echo 错误: CMake 配置失败
    exit /b 1
)

echo --- CMake 配置完成 ---

REM ==============================================================================
REM --- 7. 运行构建 ---
REM ==============================================================================
echo.
echo ==============================================================================
echo --- 正在构建项目 (%BUILD_TYPE%)... ---
echo ==============================================================================

cmake --build "%BUILD_DIR%" --parallel

if %ERRORLEVEL% neq 0 (
    echo 错误: 构建失败
    exit /b 1
)

echo.
echo ==============================================================================
echo --- 构建完成！---
echo ==============================================================================
echo.
echo 输出文件位于:
echo   DLL:  %BUILD_DIR%\bin\%BUILD_TYPE%\%TARGET_LIB_NAME%.dll
echo   LIB:  %BUILD_DIR%\lib\%BUILD_TYPE%\%TARGET_LIB_NAME%.lib
echo.

REM ==============================================================================
REM --- 脚本结束 ---
REM ==============================================================================

REM --- 运行 e2e 测试 (--test 参数) ---
if "%RUN_TESTS%"=="1" (
    echo.
    echo ==============================================================================
    echo --- 正在运行 e2e 测试... ---
    echo ==============================================================================
    
    REM 复制依赖 DLL
    echo --- 复制 vcpkg 依赖 DLL... ---
    set "VCPKG_BIN=%BUILD_DIR%\vcpkg_installed\x64-windows\bin"
    if exist "!VCPKG_BIN!" (
        copy /Y "!VCPKG_BIN!\libcrypto-3-x64.dll" "%BUILD_DIR%\bin\" >nul 2>&1
        copy /Y "!VCPKG_BIN!\libssl-3-x64.dll" "%BUILD_DIR%\bin\" >nul 2>&1
        copy /Y "!VCPKG_BIN!\libprotobuf.dll" "%BUILD_DIR%\bin\" >nul 2>&1
        copy /Y "!VCPKG_BIN!\pugixml.dll" "%BUILD_DIR%\bin\" >nul 2>&1
        copy /Y "!VCPKG_BIN!\zlib1.dll" "%BUILD_DIR%\bin\" >nul 2>&1
        copy /Y "!VCPKG_BIN!\crc32c.dll" "%BUILD_DIR%\bin\" >nul 2>&1
        copy /Y "!VCPKG_BIN!\date-tz.dll" "%BUILD_DIR%\bin\" >nul 2>&1
    )
    
    REM 注册 ODBC 驱动
    echo --- 注册 ODBC 驱动... ---
    set "DRIVER_DLL=%BUILD_DIR%\bin\%TARGET_LIB_NAME%.dll"
    set "REG_PATH=HKLM\SOFTWARE\ODBC\ODBCINST.INI\%DRIVER_NAME%"
    
    reg delete "!REG_PATH!" /f >nul 2>&1
    reg add "!REG_PATH!" /v "Driver" /t REG_SZ /d "!DRIVER_DLL!" /f >nul 2>&1
    reg add "!REG_PATH!" /v "Setup" /t REG_SZ /d "!DRIVER_DLL!" /f >nul 2>&1
    reg add "!REG_PATH!" /v "APILevel" /t REG_SZ /d "2" /f >nul 2>&1
    reg add "!REG_PATH!" /v "ConnectFunctions" /t REG_SZ /d "YYN" /f >nul 2>&1
    reg add "!REG_PATH!" /v "DriverODBCVer" /t REG_SZ /d "03.80" /f >nul 2>&1
    reg add "!REG_PATH!" /v "FileUsage" /t REG_SZ /d "0" /f >nul 2>&1
    reg add "!REG_PATH!" /v "SQLLevel" /t REG_SZ /d "1" /f >nul 2>&1
    reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "%DRIVER_NAME%" /t REG_SZ /d "Installed" /f >nul 2>&1
    
    if !ERRORLEVEL! neq 0 (
        echo 警告: ODBC 驱动注册失败，可能需要管理员权限
        echo 请以管理员身份运行此脚本
    ) else (
        echo 驱动已注册: !DRIVER_DLL!
    )
    
    REM 设置驱动路径环境变量
    set "MCO_DRIVER_PATH=!DRIVER_DLL!"
    
    REM 运行 Python 测试
    echo.
    echo --- 运行 e2e 测试套件... ---
    cd "%PROJECT_ROOT%\test\e2e"
    
    where python >nul 2>&1
    if !ERRORLEVEL! neq 0 (
        echo 错误: 未找到 Python，请确保 Python 已安装并添加到 PATH
        exit /b 1
    )
    
    python runner.py --connection
    set "TEST_RESULT=!ERRORLEVEL!"
    
    cd "%PROJECT_ROOT%"
    
    if !TEST_RESULT! neq 0 (
        echo.
        echo ==============================================================================
        echo --- e2e 测试失败 ---
        echo ==============================================================================
        exit /b 1
    )
    
    echo.
    echo ==============================================================================
    echo --- e2e 测试通过！---
    echo ==============================================================================
)

echo --- 所有操作完成！---

endlocal
