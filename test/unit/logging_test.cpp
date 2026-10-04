#include "maxcompute_odbc/common/logging.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

using namespace maxcompute_odbc;

class LoggingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 每个用例独占一个临时日志文件, 路径由当前平台给出:
    //   * 固定的 "/tmp/mco_test_log.txt" 在 Windows 上不存在父目录, 打不开文件,
    //     用例只能靠 ctest 排除掉才能过;
    //   * Logger::setLogFile() 以追加方式打开, 同名文件残留会把上一次运行的内容
    //     带进本次行数断言。
    auto *test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    const std::string file_name =
        std::string("mco_test_log_") + test_info->name() + ".log";
    const auto log_path = std::filesystem::temp_directory_path() / file_name;
    log_file_path_ = log_path.string();
    std::error_code ec;
    std::filesystem::remove(log_file_path_, ec);
  }

  void TearDown() override {
    // 清理本用例创建的日志文件 (不存在时忽略)
    std::error_code ec;
    std::filesystem::remove(log_file_path_, ec);
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
