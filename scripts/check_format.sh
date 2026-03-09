#!/bin/bash

# 检查代码格式化的脚本

# 检查 clang-format 是否可用
if ! command -v clang-format &> /dev/null
then
    echo "clang-format 未安装，请先安装 clang-format"
    exit 1
fi

# 获取项目根目录
PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

# 查找所有需要检查格式的文件
FORMAT_FILES=$(find "$PROJECT_ROOT" \( -name "*.cpp" -o -name "*.h" \) -not -path "*/build/*" -not -path "*/external/*" -not -path "*/cmake-build-*/*")

# 检查是否有文件需要格式化
NEEDS_FORMATTING=false

echo "正在检查代码格式..."

for file in $FORMAT_FILES; do
    # 创建临时文件
    TMP_FILE=$(mktemp)

    # 格式化文件到临时文件
    clang-format --style=file "$file" > "$TMP_FILE"

    # 比较原文件和格式化后的文件
    if ! cmp -s "$file" "$TMP_FILE"; then
        echo "文件需要格式化: $file"
        NEEDS_FORMATTING=true
    fi

    # 删除临时文件
    rm "$TMP_FILE"
done

if $NEEDS_FORMATTING; then
    echo "发现格式问题，请运行 'make format' 或 'cmake --build . --target format' 来格式化代码"
    exit 1
else
    echo "所有文件格式正确!"
    exit 0
fi