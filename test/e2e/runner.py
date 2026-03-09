#!/usr/bin/env python3
"""
E2E 测试运行器
"""

import sys
import os
import argparse

# 添加父目录到路径以便导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import config, print_config


def print_header():
    """打印测试标题"""
    print("=" * 60)
    print("MaxCompute ODBC Driver - E2E Test Suite")
    print("=" * 60)


def run_test_module(module_name, module_title):
    """运行单个测试模块"""
    print(f"\n[{module_name}]")
    try:
        module = __import__(module_name)
        if hasattr(module, 'run_all_tests'):
            return module.run_all_tests()
        else:
            print(f"✗ Module {module_name} does not have run_all_tests function")
            return False
    except Exception as e:
        print(f"✗ Failed to run {module_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    print_header()
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Run E2E tests for MaxCompute ODBC Driver')
    parser.add_argument('--smoke', action='store_true', help='Run only smoke tests')
    parser.add_argument('--maxqa', action='store_true', help='Run only MaxQA tests')
    parser.add_argument('--query', action='store_true', help='Run only query tests')
    parser.add_argument('--metadata', action='store_true', help='Run only metadata tests')
    parser.add_argument('--datatype', action='store_true', help='Run only data type tests')
    parser.add_argument('--error', action='store_true', help='Run only error handling tests')
    parser.add_argument('--performance', action='store_true', help='Run only performance tests')
    parser.add_argument('--all', action='store_true', help='Run all tests (default)')
    parser.add_argument('--connection', action='store_true', help='Run only connection tests')
    args = parser.parse_args()
    
    # 如果没有指定任何选项，默认运行所有测试
    if not any([args.smoke, args.maxqa, args.query, args.metadata, args.datatype,
                args.error, args.performance, args.connection]):
        args.all = True
    
    # 检查配置
    print("\nChecking configuration...")
    is_valid, message = config.validate()
    if not is_valid:
        print(f"✗ Configuration error: {message}")
        print("\nPlease set the following environment variables:")
        print("  MCO_DRIVER_PATH - Path to the ODBC driver library")
        print("  MAXCOMPUTE_ENDPOINT - MaxCompute endpoint URL")
        print("  MAXCOMPUTE_PROJECT - Project name")
        print("  ALIBABA_CLOUD_ACCESS_KEY_ID - Access Key ID")
        print("  ALIBABA_CLOUD_ACCESS_KEY_SECRET - Access Key Secret")
        print("  MAXCOMPUTE_QUOTA_NAME - Quota name (optional)")
        return False
    
    print_config()
    
    # 导入 pyodbc
    try:
        import pyodbc
        print(f"\n✓ pyodbc version: {pyodbc.version}")
    except ImportError:
        print("\n✗ pyodbc not installed. Run: pip install pyodbc")
        return False
    
    # 收集要运行的测试
    test_modules = []
    
    if args.all or args.connection:
        test_modules.append(('test_connection', 'Connection Tests'))
    
    if args.all or args.smoke:
        test_modules.append(('test_connection', 'Connection Tests (Smoke)'))
        test_modules.append(('test_query', 'Query Tests (Smoke)'))
    
    if args.all or args.maxqa:
        test_modules.append(('test_maxqa', 'MaxQA Tests'))
    
    if args.all or args.query:
        test_modules.append(('test_query', 'Query Tests'))
    
    if args.all or args.metadata:
        test_modules.append(('test_metadata', 'Metadata Tests'))
    
    if args.all or args.datatype:
        test_modules.append(('test_data_types', 'Data Types Tests'))
    
    if args.all or args.error:
        test_modules.append(('test_errors', 'Error Handling Tests'))

    # 运行测试
    results = []
    total_tests = 0
    total_passed = 0
    total_failed = 0
    total_duration = 0
    
    for module_name, module_title in test_modules:
        print(f"\n{'=' * 60}")
        print(f"Running: {module_title}")
        print(f"{'=' * 60}")
        
        try:
            module = __import__(module_name)
            if hasattr(module, 'run_all_tests'):
                result = module.run_all_tests()
                results.append((module_title, result))
            else:
                print(f"✗ Module {module_name} does not have run_all_tests function")
                results.append((module_title, False))
        except Exception as e:
            print(f"✗ Failed to run {module_name}: {e}")
            import traceback
            traceback.print_exc()
            results.append((module_title, False))
    
    # 打印汇总
    print("\n" + "=" * 60)
    print("Overall Test Summary")
    print("=" * 60)
    
    for module_title, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{module_title}: {status}")
    
    print("\n" + "=" * 60)
    
    passed_count = sum(1 for _, result in results if result)
    total_count = len(results)
    
    print(f"Test Suites: {passed_count}/{total_count} passed")
    
    if passed_count == total_count:
        print("✓ All test suites PASSED!")
        return True
    else:
        print(f"✗ {total_count - passed_count} test suite(s) FAILED")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
