#!/bin/bash

# 脚本会在遇到任何错误时立即退出
set -e

# ==============================================================================
# 脚本配置 - 请根据你的项目进行调整
# ==============================================================================

# 你的 ODBC 驱动在 odbcinst.ini 中显示的名称
DRIVER_NAME="MaxCompute ODBC Driver"

# 你的 ODBC 驱动的描述
DRIVER_DESCRIPTION="ODBC Driver for Alibaba Cloud MaxCompute"

# 你的驱动共享库的名称（不含 lib 前缀和 .so/.dylib 后缀）
# 例如，如果生成的是 libmc_odbc_driver.so，这里就写 mc_odbc_driver
# CMake 通常使用 add_library(<target_name> ...) 定义，这里就是 <target_name>
TARGET_LIB_NAME="maxcompute_odbc"

# ==============================================================================
# --- 0. 解析命令行参数 (新增部分) ---
# ==============================================================================
INSTALL_REQUESTED=false
CMAKE_ARGS=() # 用于存放传递给 CMake 的参数

# 遍历所有传入脚本的参数
for arg in "$@"; do
  case "$arg" in
    --install)
      INSTALL_REQUESTED=true
      # 这个参数是给本脚本的，不是给 cmake 的，所以不添加到 CMAKE_ARGS
      ;;
    *)
      # 其他参数都认为是给 cmake 的
      CMAKE_ARGS+=("$arg")
      ;;
  esac
done

# ==============================================================================

# --- 1. 检查 VCPKG_ROOT 环境变量 ---
if [ -z "$VCPKG_ROOT" ]; then
    echo "错误: VCPKG_ROOT 环境变量未设置。"
    echo "请将其设置为你的 vcpkg 安装目录，例如: export VCPKG_ROOT=~/dev/vcpkg"
    exit 1
fi

# 检查 vcpkg toolchain 文件是否存在
VCPKG_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
if [ ! -f "$VCPKG_TOOLCHAIN_FILE" ]; then
    echo "错误: 在 '$VCPKG_ROOT' 中找不到 vcpkg.cmake 文件。"
    echo "请确认 VCPKG_ROOT 是一个有效的 vcpkg 安装目录。"
    exit 1
fi

# --- 2. 设置构建目录 ---
BUILD_DIR="build"
echo "--- 构建目录: $BUILD_DIR ---"

# 清理旧的构建目录 (可选，但在干净构建时很有用)
if [ -d "$BUILD_DIR" ]; then
    echo "--- 清理旧的构建目录... ---"
    rm -rf "$BUILD_DIR"
fi
mkdir -p "$BUILD_DIR"

# --- 3. macOS SDK 路径检测 (修复新版本 macOS 找不到头文件问题) ---
if [[ "$OSTYPE" == "darwin"* ]]; then
    # 尝试获取最新的 SDK 路径
    MACOS_SDK_PATH=$(xcrun --show-sdk-path 2>/dev/null)
    if [ -z "$MACOS_SDK_PATH" ] || [ ! -d "$MACOS_SDK_PATH" ]; then
        # 如果 xcrun 失败，手动查找最新版本的 SDK
        MACOS_SDK_PATH=$(ls -d /Library/Developer/CommandLineTools/SDKs/MacOSX*.sdk 2>/dev/null | sort -V | tail -1)
    fi
    
    if [ -n "$MACOS_SDK_PATH" ] && [ -d "$MACOS_SDK_PATH" ]; then
        echo "--- 检测到 macOS SDK: $MACOS_SDK_PATH ---"
        export CMAKE_OSX_SYSROOT="$MACOS_SDK_PATH"
        CMAKE_ARGS+=("-DCMAKE_OSX_SYSROOT=$MACOS_SDK_PATH")
    else
        echo "警告: 无法检测到 macOS SDK 路径，构建可能失败。"
    fi
fi

# --- 4. 运行 CMake 配置 ---
echo "--- 正在运行 CMake 配置... ---"
# 修改: 只将过滤后的参数传递给 cmake
cmake -B "$BUILD_DIR" \
      -S . \
      -DCMAKE_TOOLCHAIN_FILE="$VCPKG_TOOLCHAIN_FILE" \
      -DCMAKE_VERBOSE_MAKEFILE:BOOL=OFF \
      "${CMAKE_ARGS[@]}" # 将过滤后的参数传递给 cmake

# --- 5. 运行构建 ---
echo "--- 正在构建项目... ---"
cmake --build "$BUILD_DIR" --config Release --parallel # 默认构建 Release 版本
echo "--- 构建完成！---"
echo "--- 可执行文件和库位于 '$BUILD_DIR' 目录中。---"


# ==============================================================================
# --- 6. 注册 ODBC 驱动 (仅当提供了 --install 参数时执行) ---
# ==============================================================================
if [ "$INSTALL_REQUESTED" = true ]; then
    echo "--- 检测到 '--install' 参数，正在尝试注册 ODBC 驱动... ---"

    # 检查 odbcinst 命令是否存在
    if ! command -v odbcinst &> /dev/null; then
        echo "警告: 'odbcinst' 命令未找到。"
        echo "请安装 unixODBC (例如: 'sudo apt-get install unixodbc-dev' on Debian/Ubuntu, 'brew install unixodbc' on macOS)。"
        echo "驱动已编译，但无法自动注册。"
        exit 1 # 因为用户明确要求安装，所以这里以错误退出
    fi

    # 根据操作系统确定库文件后缀
    if [[ "$OSTYPE" == "darwin"* ]]; then
        LIB_SUFFIX="dylib"
    else
        LIB_SUFFIX="so"
    fi

    # 在构建目录中查找驱动库文件
    # CMake 通常将库文件放在 build/lib/ 或 build/ 目录下
    DRIVER_LIB_PATH=$(find "$BUILD_DIR" -name "lib${TARGET_LIB_NAME}.${LIB_SUFFIX}" | head -n 1)

    if [ -z "$DRIVER_LIB_PATH" ]; then
        echo "错误: 在 '$BUILD_DIR' 目录中找不到驱动库 'lib${TARGET_LIB_NAME}.${LIB_SUFFIX}'。"
        echo "请检查脚本顶部的 'TARGET_LIB_NAME' 配置是否正确，或检查 CMakeLists.txt 中的库目标名称。"
        exit 1
    fi

    # 获取驱动库的绝对路径
    DRIVER_LIB_ABSPATH=$(cd "$(dirname "$DRIVER_LIB_PATH")" && pwd)/$(basename "$DRIVER_LIB_PATH")
    echo "--- 找到驱动库: $DRIVER_LIB_ABSPATH ---"

    # 定义临时模板文件的名称
    TEMPLATE_FILE="odbcinst_template.ini"

    # --- 生成 odbcinst.ini 模板文件 ---
    echo "--- 正在生成 '$TEMPLATE_FILE'... ---"
    cat << EOF > "$TEMPLATE_FILE"
[${DRIVER_NAME}]
Description = ${DRIVER_DESCRIPTION}
Driver      = ${DRIVER_LIB_ABSPATH}
Setup       = ${DRIVER_LIB_ABSPATH}
EOF

    # 打印模板内容以供调试
    echo "--- '$TEMPLATE_FILE' 内容: ---"
    cat "$TEMPLATE_FILE"
    echo "------------------------------"

    # --- 执行注册命令 ---
    echo "--- 正在执行 'sudo odbcinst -i -d -f ${TEMPLATE_FILE}' ---"
    echo "--- 这将需要你输入管理员密码 ---"
    sudo odbcinst -i -d -f "$TEMPLATE_FILE"

    # 检查上一个命令的退出码
    if [ $? -eq 0 ]; then
        echo "--- ODBC 驱动注册成功！---"
        echo "你可以通过 'odbcinst -q -d' 命令来验证。"
    else
        echo "错误: ODBC 驱动注册失败。"
        echo "请检查 'sudo' 权限和 'odbcinst' 命令的输出。"
        # 即使注册失败，也不删除模板文件，以便用户手动调试
        exit 1
    fi

    # --- 清理临时文件 ---
    echo "--- 清理临时模板文件... ---"
    rm "$TEMPLATE_FILE"
else
    # 如果没有 --install 参数，则打印提示信息
    echo "--- 跳过 ODBC 驱动注册。如需注册，请使用 '--install' 参数重新运行脚本。 ---"
fi


# --- 脚本结束 ---
echo "--- 所有操作完成！---"
