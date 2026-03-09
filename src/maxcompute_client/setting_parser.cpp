#include "maxcompute_odbc/maxcompute_client/setting_parser.h"
#include <algorithm>
#include <cctype>
#include <utility>

namespace maxcompute_odbc {

// --- 公共入口方法 ---
ParseResult SettingParser::parse(std::string query) {
  if (trim(query).empty() || trim(query).back() != ';') {
    query += ";";
  }
  SettingParser parser;
  return parser.extractSetStatements(query);
}

// --- 核心状态机实现 ---
ParseResult SettingParser::extractSetStatements(const std::string &s) {
  std::map<std::string, std::string> settings;
  std::vector<std::string> errors;
  std::vector<std::pair<size_t, size_t>> excludeRanges;
  State currentState = State::DEFAULT;

  std::string lowerS = toLower(s);
  size_t i = 0;
  size_t currentStartIndex = std::string::npos;

  while (i < s.length()) {
    switch (currentState) {
      case State::DEFAULT:
        if (s.compare(i, 2, "--") == 0) {
          currentState = State::SINGLE_LINE_COMMENT;
          i += 2;
        } else if (s.compare(i, 2, "/*") == 0) {
          currentState = State::MULTI_LINE_COMMENT;
          i += 2;
        } else if (lowerS.compare(i, 3, "set") == 0 &&
                   (i + 3 >= s.length() || std::isspace(s[i + 3]))) {
          currentState = State::IN_SET;
          currentStartIndex = i;
          i += 4;  // Skip 'set' and the following whitespace
        } else {
          if (!std::isspace(s[i])) {
            currentState = State::STOP;
          }
          i++;
        }
        break;

      case State::SINGLE_LINE_COMMENT:
        while (i < s.length() && s[i] != '\n') {
          i++;
        }
        if (i < s.length()) {
          i++;
        }
        currentState = State::DEFAULT;
        break;

      case State::MULTI_LINE_COMMENT:
        while (i < s.length()) {
          if (s.compare(i, 2, "*/") == 0) {
            i += 2;
            currentState = State::DEFAULT;
            break;
          }
          i++;
        }
        break;

      case State::IN_SET:
        while (i < s.length() && std::isspace(s[i])) {
          i++;
        }
        if (i < s.length()) {
          currentState = State::IN_KEY_VALUE;
        } else {
          errors.push_back(
              "Invalid SET statement: missing key-value after 'set'");
          currentStartIndex = std::string::npos;
          currentState = State::DEFAULT;
        }
        break;

      case State::IN_KEY_VALUE: {
        size_t keyValueStart = i;
        bool foundSemicolon = false;
        while (i < s.length()) {
          if (s[i] == ';' && (i == 0 || s[i - 1] != '\\')) {
            foundSemicolon = true;
            i++;
            break;
          }
          i++;
        }
        if (foundSemicolon) {
          std::string kv_str(s.substr(keyValueStart, i - 1 - keyValueStart));
          if (parseKeyValue(std::string(trim(kv_str)), settings, errors)) {
            excludeRanges.emplace_back(currentStartIndex, i);
          }
        } else {
          errors.push_back("Invalid SET statement: missing semicolon");
        }
        currentState = State::DEFAULT;
        currentStartIndex = std::string::npos;
        break;
      }
      case State::STOP:
        i = s.length();
        break;
    }
  }

  // 构建剩余的查询
  std::string remainingQueryStr;
  size_t currentPos = 0;
  for (const auto &range : excludeRanges) {
    if (currentPos < range.first) {
      remainingQueryStr.append(s, currentPos, range.first - currentPos);
    }
    currentPos = range.second;
  }
  if (currentPos < s.length()) {
    remainingQueryStr.append(s, currentPos, s.length() - currentPos);
  }
  return {settings, std::string(trim(remainingQueryStr)), errors};
}

// --- 私有辅助方法实现 ---
bool SettingParser::parseKeyValue(const std::string &kv,
                                  std::map<std::string, std::string> &settings,
                                  std::vector<std::string> &errors) {
  size_t eqIdx = kv.find('=');
  if (eqIdx == std::string::npos) {
    errors.push_back("Invalid key-value pair '" + kv + "': missing '='");
    return false;
  }
  std::string key = std::string(trim(kv.substr(0, eqIdx)));
  if (key.empty()) {
    errors.push_back("Invalid key-value pair '" + kv + "': empty key");
    return false;
  }
  std::string value =
      (eqIdx < kv.length() - 1) ? std::string(trim(kv.substr(eqIdx + 1))) : "";
  replaceAll(value, "\\;", ";");
  settings[key] = value;
  return true;
}

std::string SettingParser::toLower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return s;
}

std::string_view SettingParser::trim(std::string_view sv) {
  const auto pos_start = sv.find_first_not_of(" \t\n\r\f\v");
  if (pos_start == std::string_view::npos) {
    return {};  // 全是空格
  }
  const auto pos_end = sv.find_last_not_of(" \t\n\r\f\v");
  return sv.substr(pos_start, pos_end - pos_start + 1);
}

void SettingParser::replaceAll(std::string &str, const std::string &from,
                               const std::string &to) {
  size_t start_pos = 0;
  while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}

}  // namespace maxcompute_odbc
