#pragma once

#include <map>
#include <string>
#include <vector>

namespace maxcompute_odbc {

struct ParseResult {
  std::map<std::string, std::string> settings;
  std::string remainingQuery;
  std::vector<std::string> errors;
};

class SettingParser {
 public:
  /**
   * @brief 解析一个SQL查询，提取出开头的 'set' 语句。
   * 这是一个静态入口方法，对应 Java 中的 public static ParseResult parse(String
   * query)。
   * @param query 要解析的SQL查询字符串。
   * @return ParseResult 包含提取出的配置、剩余的查询和解析错误。
   */
  static ParseResult parse(std::string query);

 private:
  // 状态机定义
  enum class State {
    DEFAULT,
    SINGLE_LINE_COMMENT,
    MULTI_LINE_COMMENT,
    IN_SET,
    IN_KEY_VALUE,
    STOP
  };

  // 私有构造函数，确保只能通过静态方法调用
  SettingParser() = default;

  // 核心的解析逻辑
  ParseResult extractSetStatements(const std::string &s);

  // 解析 key-value 对
  bool parseKeyValue(const std::string &kv,
                     std::map<std::string, std::string> &settings,
                     std::vector<std::string> &errors);

  // 辅助函数
  static std::string toLower(std::string s);
  static std::string_view trim(std::string_view sv);
  static void replaceAll(std::string &str, const std::string &from,
                         const std::string &to);
};

}  // namespace maxcompute_odbc
