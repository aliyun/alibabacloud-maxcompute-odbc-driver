#pragma once

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

namespace maxcompute_odbc {

enum class LogLevel { None = 0, Debug = 1, Info = 2, Warning = 3, Error = 4 };

class Logger {
 public:
  static Logger &getInstance();

  void setLogLevel(std::string level);
  void setLogLevel(LogLevel level);
  void setLogFile(const std::string &filepath);
  std::string levelToString(LogLevel level);

  template <typename... Args>
  void debug(const std::string &format_str, Args &&...args) {
    if (LogLevel::Debug >= current_level_) {
      log(LogLevel::Debug, format(format_str, args...));
    }
  }

  template <typename... Args>
  void info(const std::string &format_str, Args &&...args) {
    if (LogLevel::Info >= current_level_) {
      log(LogLevel::Info, format(format_str, args...));
    }
  }

  template <typename... Args>
  void warning(const std::string &format_str, Args &&...args) {
    if (LogLevel::Warning >= current_level_) {
      log(LogLevel::Warning, format(format_str, args...));
    }
  }

  template <typename... Args>
  void error(const std::string &format_str, Args &&...args) {
    if (LogLevel::Error >= current_level_) {
      log(LogLevel::Error, format(format_str, args...));
    }
  }

 private:
  Logger();
  ~Logger();

  void log(LogLevel level, const std::string &message);

  template <typename... Args>
  std::string format(const std::string &format_str, Args &&...args) {
    std::ostringstream oss;
    formatHelper(oss, format_str, args...);
    return oss.str();
  }

  template <typename T, typename... Args>
  void formatHelper(std::ostringstream &oss, const std::string &format_str,
                    T &&first, Args &&...rest) {
    size_t pos = format_str.find("{}");
    if (pos != std::string::npos) {
      oss << format_str.substr(0, pos) << first;
      formatHelper(oss, format_str.substr(pos + 2), rest...);
    } else {
      oss << format_str;
    }
  }

  void formatHelper(std::ostringstream &oss, const std::string &format_str) {
    oss << format_str;
  }

  LogLevel current_level_;
  std::unique_ptr<std::ofstream> log_file_;
  std::mutex mutex_;
};

inline LogLevel stringToLevel(std::string level_str) {
  if (level_str == "DEBUG" || level_str == "debug") {
    return LogLevel::Debug;
  } else if (level_str == "INFO" || level_str == "info") {
    return LogLevel::Info;
  } else if (level_str == "WARNING" || level_str == "warning") {
    return LogLevel::Warning;
  } else if (level_str == "ERROR" || level_str == "error") {
    return LogLevel::Error;
  } else if (level_str == "NONE" || level_str == "none") {
    return LogLevel::None;
  } else {
    return LogLevel::Info;
  }
}

// Implementation
inline Logger &Logger::getInstance() {
  // Leaky singleton: heap-allocated and never deleted on purpose.
  // Why: handle destructors (ConnHandle/StmtHandle) and HandleRegistry::get()
  // call into Logger during dylib teardown. With a function-local static, the
  // Logger could be destroyed before the last access, producing SIGSEGV at
  // process exit. Leaking is safe — OS closes fds and reclaims memory at exit.
  static Logger *instance = new Logger();
  return *instance;
}

inline Logger::Logger() : current_level_(LogLevel::Info) {
  // Check for environment variable to set log level
  const char *log_level_env = std::getenv("MCO_LOG_LEVEL");
  if (log_level_env) {
    std::string level_str(log_level_env);
    current_level_ = stringToLevel(level_str);
  }

  // Check for environment variable to set log file path
  const char *log_file_env = std::getenv("MCO_LOG_FILE");
  if (log_file_env) {
    std::string log_file_path(log_file_env);
    setLogFile(log_file_path);
  }
}

inline Logger::~Logger() {
  if (log_file_ && log_file_->is_open()) {
    log_file_->close();
  }
}

inline void Logger::setLogLevel(std::string level) {
  std::lock_guard<std::mutex> lock(mutex_);
  current_level_ = stringToLevel(level);
}

inline void Logger::setLogLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(mutex_);
  current_level_ = level;
}

inline void Logger::setLogFile(const std::string &filepath) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (log_file_) {
    log_file_->close();
  }
  // 在Windows上确保文件路径正确打开
  log_file_ = std::make_unique<std::ofstream>(filepath, std::ios::app);
}

inline void Logger::log(LogLevel level, const std::string &message) {
  if (level < current_level_) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // 获取当前时间
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
  ss << '.' << std::setfill('0') << std::setw(3) << ms.count();

  std::string log_line =
      "[" + ss.str() + "] [" + levelToString(level) + "] " + message;

  // 输出到文件（如果设置了文件）
  if (log_file_ && log_file_->is_open()) {
    *log_file_ << log_line << std::endl;
    log_file_->flush();
  } else {
    // 输出到控制台
    std::cout << log_line << std::endl;
  }
}

inline std::string Logger::levelToString(LogLevel level) {
  switch (level) {
    case LogLevel::Debug:
      return "DEBUG";
    case LogLevel::Info:
      return "INFO";
    case LogLevel::Warning:
      return "WARNING";
    case LogLevel::Error:
      return "ERROR";
    default:
      return "UNKNOWN";
  }
}

// 方便使用的宏定义
#define MCO_LOG_DEBUG(...) \
  maxcompute_odbc::Logger::getInstance().debug(__VA_ARGS__)
#define MCO_LOG_INFO(...) \
  maxcompute_odbc::Logger::getInstance().info(__VA_ARGS__)
#define MCO_LOG_WARNING(...) \
  maxcompute_odbc::Logger::getInstance().warning(__VA_ARGS__)
#define MCO_LOG_ERROR(...) \
  maxcompute_odbc::Logger::getInstance().error(__VA_ARGS__)

}  // namespace maxcompute_odbc
