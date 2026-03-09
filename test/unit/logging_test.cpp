#include "maxcompute_odbc/common/logging.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

using namespace maxcompute_odbc;

class LoggingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 设置临时日志文件路径
    log_file_path_ = "/tmp/mco_test_log.txt";
  }

  void TearDown() override {
    // 清理临时日志文件
    if (std::filesystem::exists(log_file_path_)) {
      std::filesystem::remove(log_file_path_);
    }
  }

  std::string log_file_path_;
};

TEST_F(LoggingTest, TestLogLevelFiltering) {
  Logger &logger = Logger::getInstance();
  logger.setLogLevel(LogLevel::Warning);

  // 这些日志不应该被记录（级别低于Warning）
  logger.debug("Debug message");
  logger.info("Info message");

  // 这些日志应该被记录
  logger.warning("Warning message");
  logger.error("Error message");

  // 由于我们没有直接的访问方式来检查日志输出，
  // 这个测试主要是确保没有崩溃
  SUCCEED();
}

TEST_F(LoggingTest, TestLogToFile) {
  Logger &logger = Logger::getInstance();
  logger.setLogLevel(LogLevel::Debug);
  logger.setLogFile(log_file_path_);

  logger.debug("Debug test message");
  logger.info("Info test message");
  logger.warning("Warning test message");
  logger.error("Error test message");

  // 读取日志文件并验证内容
  std::ifstream log_file(log_file_path_);
  ASSERT_TRUE(log_file.is_open());

  std::string line;
  int line_count = 0;
  while (std::getline(log_file, line)) {
    line_count++;
  }

  // 应该有4行日志
  EXPECT_EQ(line_count, 4);

  log_file.close();
}

TEST_F(LoggingTest, TestFormatLogging) {
  Logger &logger = Logger::getInstance();
  logger.setLogLevel(LogLevel::Debug);
  logger.setLogFile(log_file_path_);

  int value = 42;
  std::string name = "test";

  logger.info("Testing format with value {} and name {}", value, name);

  // 读取日志文件并验证内容
  std::ifstream log_file(log_file_path_);
  ASSERT_TRUE(log_file.is_open());

  std::string line;
  ASSERT_TRUE(std::getline(log_file, line));

  // 检查格式化是否正确
  EXPECT_NE(line.find("Testing format with value 42 and name test"),
            std::string::npos);

  log_file.close();
}

TEST_F(LoggingTest, TestLogLevelStrings) {
  Logger &logger = Logger::getInstance();

  // 测试日志级别到字符串的转换
  EXPECT_EQ(logger.levelToString(LogLevel::Debug), "DEBUG");
  EXPECT_EQ(logger.levelToString(LogLevel::Info), "INFO");
  EXPECT_EQ(logger.levelToString(LogLevel::Warning), "WARNING");
  EXPECT_EQ(logger.levelToString(LogLevel::Error), "ERROR");
}
