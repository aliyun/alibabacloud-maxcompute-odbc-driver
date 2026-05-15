#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/common/utils.h"
#include "maxcompute_odbc/config/config.h"
#include <algorithm>
#include <cctype>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdexcept>

namespace maxcompute_odbc {
void Config::validate() const {
  // Validate required parameters
  if (endpoint.empty()) {
    throw std::invalid_argument("Endpoint is required");
  }

  if (project.empty()) {
    throw std::invalid_argument("Project is required");
  }

  // Either accessKeyId/accessKeySecret pair or credential client is required
  bool hasAccessKeyId = accessKeyId.has_value() && !accessKeyId->empty();
  bool hasAccessKeySecret =
      accessKeySecret.has_value() && !accessKeySecret->empty();

  if (hasAccessKeyId != hasAccessKeySecret) {
    throw std::invalid_argument(
        "Both accessKeyId and accessKeySecret must be provided together");
  }

  if (!hasAccessKeyId && !credential) {
    throw std::invalid_argument(
        "Either accessKeyId/accessKeySecret or credential client must be "
        "provided");
  }

  if (readTimeout < 0) {
    throw std::invalid_argument("Read timeout must be non-negative");
  }

  if (connectTimeout < 0) {
    throw std::invalid_argument("Connect timeout must be non-negative");
  }
}

bool Config::hasValidCredentials() const {
  // Check if we have either accessKeyId/accessKeySecret or credential client
  bool hasAccessKeyId = accessKeyId.has_value() && !accessKeyId->empty();
  bool hasAccessKeySecret =
      accessKeySecret.has_value() && !accessKeySecret->empty();
  return (hasAccessKeyId && hasAccessKeySecret) || (credential != nullptr);
}

std::shared_ptr<ICredentialsProvider> Config::getCredentialsProvider() const {
  bool hasAccessKeyId = accessKeyId.has_value() && !accessKeyId->empty();
  bool hasAccessKeySecret =
      accessKeySecret.has_value() && !accessKeySecret->empty();

  if (hasAccessKeyId && hasAccessKeySecret) {
    return std::make_shared<StaticCredentialProvider>(
        accessKeyId.value(), accessKeySecret.value(),
        securityToken.value_or(""));
  } else if (credential) {
    // Use existing credential client
    return credential;
  }
  throw std::invalid_argument("Credential config must be set.");
}

std::string Config::getFullEndpoint() const {
  // If endpoint already starts with http:// or https://, return as is
  if (endpoint.find("http://") == 0 || endpoint.find("https://") == 0) {
    return endpoint;
  }

  // Otherwise, prepend the protocol
  return protocol + "://" + endpoint;
}

// Helper function to trim whitespace from string
static std::string trim(const std::string &str) {
  const auto strBegin = str.find_first_not_of(" \t");
  if (strBegin == std::string::npos) return "";

  const auto strEnd = str.find_last_not_of(" \t");
  const auto strRange = strEnd - strBegin + 1;

  return str.substr(strBegin, strRange);
}

// Helper function to convert string to lowercase
static std::string toLower(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return str;
}

std::map<std::string, std::string>
ConnectionStringParser::parseDriverConnectString(const std::string &conn_str) {
  std::map<std::string, std::string> params;
  std::string current_pair;
  size_t start = 0;
  size_t end;

  while ((end = conn_str.find(';', start)) != std::string::npos) {
    current_pair = conn_str.substr(start, end - start);
    size_t eq_pos = current_pair.find('=');
    if (eq_pos != std::string::npos) {
      std::string key = current_pair.substr(0, eq_pos);
      // Key 转换为大写，使其不区分大小写
      std::transform(key.begin(), key.end(), key.begin(), ::toupper);
      params[key] = current_pair.substr(eq_pos + 1);
    }
    start = end + 1;
  }
  // 处理最后一个键值对
  if (start < conn_str.length()) {
    current_pair = conn_str.substr(start);
    size_t eq_pos = current_pair.find('=');
    if (eq_pos != std::string::npos) {
      std::string key = current_pair.substr(0, eq_pos);
      std::transform(key.begin(), key.end(), key.begin(), ::toupper);
      params[key] = current_pair.substr(eq_pos + 1);
    }
  }
  return params;
}

Config ConnectionStringParser::parse(const std::string &connStr) {
  MCO_LOG_INFO("DEBUG: {}", connStr);

  Config config;
  size_t start = 0;

  while (start < connStr.length()) {
    // 跳过空白字符
    while (start < connStr.length() && connStr[start] == ' ') {
      start++;
    }
    if (start >= connStr.length()) break;

    // 查找键
    size_t eqPos = connStr.find('=', start);
    if (eqPos == std::string::npos) break;

    std::string key = trim(connStr.substr(start, eqPos - start));
    std::string lowerKey = toLower(key);

    // 查找值
    size_t value_start = eqPos + 1;
    std::string value;

    // 检查值是否以双引号开头
    if (value_start < connStr.length() && connStr[value_start] == '"') {
      // 处理双引号包裹的值
      size_t quote_end = value_start + 1;
      while (quote_end < connStr.length() && connStr[quote_end] != '"') {
        // 处理转义的双引号 \"
        if (connStr[quote_end] == '\\' && quote_end + 1 < connStr.length() &&
            connStr[quote_end + 1] == '"') {
          quote_end += 2;
        } else {
          quote_end++;
        }
      }
      if (quote_end < connStr.length() && connStr[quote_end] == '"') {
        // 去掉外层的双引号，并处理转义字符
        std::string raw_value =
            connStr.substr(value_start + 1, quote_end - value_start - 1);
        for (size_t i = 0; i < raw_value.length(); ++i) {
          if (raw_value[i] == '\\' && i + 1 < raw_value.length() &&
              raw_value[i + 1] == '"') {
            value += '"';
            i++;
          } else {
            value += raw_value[i];
          }
        }
        start = quote_end + 1;
      } else {
        // 双引号不匹配，使用原始值
        value = connStr.substr(value_start);
        start = connStr.length();
      }
    } else {
      // 查找下一个分号
      size_t semicolon_pos = connStr.find(';', value_start);
      if (semicolon_pos == std::string::npos) {
        value = connStr.substr(value_start);
        start = connStr.length();
      } else {
        value = connStr.substr(value_start, semicolon_pos - value_start);
        start = semicolon_pos + 1;
      }
    }

    // 去除值两端的空白字符
    value = trim(value);

    if (lowerKey == "endpoint" || lowerKey == "server") {
      config.endpoint = value;
    } else if (lowerKey == "project") {
      config.project = value;
    } else if (lowerKey == "schema") {
      config.schema = value;
    } else if (lowerKey == "accesskeyid") {
      config.accessKeyId = value;
    } else if (lowerKey == "accesskeysecret") {
      config.accessKeySecret = value;
    } else if (lowerKey == "securitytoken") {
      config.securityToken = value;
    } else if (lowerKey == "readtimeout") {
      try {
        config.readTimeout = std::stoi(value);
      } catch (...) {
        config.readTimeout = 60;
      }
    } else if (lowerKey == "connecttimeout") {
      try {
        config.connectTimeout = std::stoi(value);
      } catch (...) {
        config.connectTimeout = 30;
      }
    } else if (lowerKey == "protocol") {
      config.protocol = toLower(value);
      if (config.protocol != "http" && config.protocol != "https") {
        config.protocol = "https";
      }
    } else if (lowerKey == "datasourcename") {
      config.dataSourceName = value;
    } else if (lowerKey == "regionid") {
      config.regionId = value;
    } else if (lowerKey == "useragent") {
      config.userAgent = value;
    } else if (lowerKey == "httpproxy") {
      config.httpProxy = value;
      auto parsed = parseProxyUrl(value);
      if (!std::get<2>(parsed).empty() && !config.proxyUser.has_value()) {
        config.proxyUser = std::get<2>(parsed);
      }
      if (!std::get<3>(parsed).empty() && !config.proxyPassword.has_value()) {
        config.proxyPassword = std::get<3>(parsed);
      }
    } else if (lowerKey == "httpsproxy") {
      config.httpsProxy = value;
      auto parsed = parseProxyUrl(value);
      if (!std::get<2>(parsed).empty() && !config.proxyUser.has_value()) {
        config.proxyUser = std::get<2>(parsed);
      }
      if (!std::get<3>(parsed).empty() && !config.proxyPassword.has_value()) {
        config.proxyPassword = std::get<3>(parsed);
      }
    } else if (lowerKey == "proxyuser") {
      config.proxyUser = value;
    } else if (lowerKey == "proxypassword" || lowerKey == "proxypwd") {
      config.proxyPassword = value;
    } else if (lowerKey == "logpath") {
      maxcompute_odbc::Logger::getInstance().setLogFile(value);
    } else if (lowerKey == "loglevel") {
      maxcompute_odbc::Logger::getInstance().setLogLevel(value);
    } else if (lowerKey == "timezone") {
      config.timezone = value;
    } else if (lowerKey == "namespaceschema") {
      config.namespaceSchema = value != "false";
    } else if (lowerKey == "charset") {
      if (!value.empty()) {
        std::string normalized = value;
        std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        if (normalized == "UTF8") normalized = "UTF-8";
        config.charset = normalized;
      }
    } else if (lowerKey == "fetchresultsplitsize") {
      try {
        config.fetchResultSplitSize = std::stoll(value);
      } catch (...) {
        config.fetchResultSplitSize = 10000;
      }
    } else if (lowerKey == "fetchresultpreloadsplitnum") {
      try {
        config.fetchResultPreloadSplitNum = std::stol(value);
      } catch (...) {
        config.fetchResultPreloadSplitNum = 5;
      }
    } else if (lowerKey == "fetchresultthreadnum") {
      try {
        config.fetchResultThreadNum = std::stol(value);
      } catch (...) {
        config.fetchResultThreadNum = 5;
      }
    } else if (lowerKey == "logviewhost") {
      config.logviewHost = value;
    } else if (lowerKey == "globalsettings") {
      try {
        auto json_obj = nlohmann::json::parse(value);
        if (json_obj.is_object()) {
          for (auto &item : json_obj.items()) {
            if (item.value().is_string()) {
              config.globalSettings[item.key()] =
                  item.value().get<std::string>();
            } else {
              config.globalSettings[item.key()] = item.value().dump();
            }
          }
        }
      } catch (const std::exception &e) {
        MCO_LOG_WARNING("Failed to parse globalSettings JSON: {}", e.what());
      }
    } else if (lowerKey == "interactivemode") {
      // Parse boolean value (case-insensitive)
      std::string lowerValue = toLower(value);
      if (lowerValue == "true" || lowerValue == "1" || lowerValue == "yes") {
        config.interactiveMode = true;
      } else if (lowerValue == "false" || lowerValue == "0" ||
                 lowerValue == "no") {
        config.interactiveMode = false;
      } else {
        // Invalid value, log warning and use default
        MCO_LOG_WARNING(
            "Invalid interactiveMode value: '{}', using default: false", value);
        config.interactiveMode = false;
      }
    } else if (lowerKey == "quotaname") {
      config.quotaName = value;
    }
  }

  return config;
}

void ConnectionStringParser::updateConfig(const std::string &connStr,
                                          Config &config) {
  size_t start = 0;

  while (start < connStr.length()) {
    // 跳过空白字符
    while (start < connStr.length() && connStr[start] == ' ') {
      start++;
    }
    if (start >= connStr.length()) break;

    // 查找键
    size_t eqPos = connStr.find('=', start);
    if (eqPos == std::string::npos) break;

    std::string key = trim(connStr.substr(start, eqPos - start));
    std::string lowerKey = toLower(key);

    // 查找值
    size_t value_start = eqPos + 1;
    std::string value;

    // 检查值是否以双引号开头
    if (value_start < connStr.length() && connStr[value_start] == '"') {
      // 处理双引号包裹的值
      size_t quote_end = value_start + 1;
      while (quote_end < connStr.length() && connStr[quote_end] != '"') {
        // 处理转义的双引号 \"
        if (connStr[quote_end] == '\\' && quote_end + 1 < connStr.length() &&
            connStr[quote_end + 1] == '"') {
          quote_end += 2;
        } else {
          quote_end++;
        }
      }
      if (quote_end < connStr.length() && connStr[quote_end] == '"') {
        // 去掉外层的双引号，并处理转义字符
        std::string raw_value =
            connStr.substr(value_start + 1, quote_end - value_start - 1);
        for (size_t i = 0; i < raw_value.length(); ++i) {
          if (raw_value[i] == '\\' && i + 1 < raw_value.length() &&
              raw_value[i + 1] == '"') {
            value += '"';
            i++;
          } else {
            value += raw_value[i];
          }
        }
        start = quote_end + 1;
      } else {
        // 双引号不匹配，使用原始值
        value = connStr.substr(value_start);
        start = connStr.length();
      }
    } else {
      // 查找下一个分号
      size_t semicolon_pos = connStr.find(';', value_start);
      if (semicolon_pos == std::string::npos) {
        value = connStr.substr(value_start);
        start = connStr.length();
      } else {
        value = connStr.substr(value_start, semicolon_pos - value_start);
        start = semicolon_pos + 1;
      }
    }

    // 去除值两端的空白字符
    value = trim(value);

    if (lowerKey == "endpoint" || lowerKey == "server") {
      config.endpoint = value;
    } else if (lowerKey == "project") {
      config.project = value;
    } else if (lowerKey == "schema") {
      config.schema = value;
    } else if (lowerKey == "accesskeyid" || lowerKey == "uid") {
      config.accessKeyId = value;
    } else if (lowerKey == "accesskeysecret" || lowerKey == "pwd") {
      config.accessKeySecret = value;
    } else if (lowerKey == "securitytoken") {
      config.securityToken = value;
    } else if (lowerKey == "readtimeout") {
      config.readTimeout = std::stoi(value);
    } else if (lowerKey == "connecttimeout") {
      config.connectTimeout = std::stoi(value);
    } else if (lowerKey == "protocol") {
      config.protocol = toLower(value);
      if (config.protocol != "http" && config.protocol != "https") {
        config.protocol = "https";
      }
    } else if (lowerKey == "datasourcename") {
      config.dataSourceName = value;
    } else if (lowerKey == "regionid") {
      config.regionId = value;
    } else if (lowerKey == "useragent") {
      config.userAgent = value;
    } else if (lowerKey == "httpproxy") {
      config.httpProxy = value;
      auto parsed = parseProxyUrl(value);
      if (!std::get<2>(parsed).empty() && !config.proxyUser.has_value()) {
        config.proxyUser = std::get<2>(parsed);
      }
      if (!std::get<3>(parsed).empty() && !config.proxyPassword.has_value()) {
        config.proxyPassword = std::get<3>(parsed);
      }
    } else if (lowerKey == "httpsproxy") {
      config.httpsProxy = value;
      auto parsed = parseProxyUrl(value);
      if (!std::get<2>(parsed).empty() && !config.proxyUser.has_value()) {
        config.proxyUser = std::get<2>(parsed);
      }
      if (!std::get<3>(parsed).empty() && !config.proxyPassword.has_value()) {
        config.proxyPassword = std::get<3>(parsed);
      }
    } else if (lowerKey == "proxyuser") {
      config.proxyUser = value;
    } else if (lowerKey == "proxypassword" || lowerKey == "proxypwd") {
      config.proxyPassword = value;
    } else if (lowerKey == "logpath") {
      maxcompute_odbc::Logger::getInstance().setLogFile(value);
    } else if (lowerKey == "loglevel") {
      maxcompute_odbc::Logger::getInstance().setLogLevel(value);
    } else if (lowerKey == "timezone") {
      config.timezone = value;
    } else if (lowerKey == "namespaceschema") {
      config.namespaceSchema = value != "false";
    } else if (lowerKey == "charset") {
      if (!value.empty()) {
        std::string normalized = value;
        std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        if (normalized == "UTF8") normalized = "UTF-8";
        config.charset = normalized;
      }
    } else if (lowerKey == "fetchresultsplitsize") {
      config.fetchResultSplitSize = std::stoll(value);
    } else if (lowerKey == "fetchresultpreloadsplitnum") {
      config.fetchResultPreloadSplitNum = std::stol(value);
    } else if (lowerKey == "fetchresultthreadnum") {
      config.fetchResultThreadNum = std::stol(value);
    } else if (lowerKey == "logviewhost") {
      config.logviewHost = value;
    } else if (lowerKey == "globalsettings") {
      try {
        auto json_obj = nlohmann::json::parse(value);
        if (json_obj.is_object()) {
          for (auto &item : json_obj.items()) {
            if (item.value().is_string()) {
              config.globalSettings[item.key()] =
                  item.value().get<std::string>();
            } else {
              config.globalSettings[item.key()] = item.value().dump();
            }
          }
        }
      } catch (const std::exception &e) {
        MCO_LOG_WARNING("Failed to parse globalSettings JSON: {}", e.what());
      }
    } else if (lowerKey == "interactivemode") {
      // Parse boolean value (case-insensitive)
      std::string lowerValue = toLower(value);
      if (lowerValue == "true" || lowerValue == "1" || lowerValue == "yes") {
        config.interactiveMode = true;
      } else if (lowerValue == "false" || lowerValue == "0" ||
                 lowerValue == "no") {
        config.interactiveMode = false;
      } else {
        // Invalid value, log warning and use default
        MCO_LOG_WARNING(
            "Invalid interactiveMode value: '{}', using default: false", value);
        config.interactiveMode = false;
      }
    } else if (lowerKey == "quotaname") {
      config.quotaName = value;
    }
  }
}

}  // namespace maxcompute_odbc
