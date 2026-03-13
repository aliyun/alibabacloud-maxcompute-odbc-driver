#pragma once

#ifdef _WIN32
// Windows ODBC requires specific include order
// windows.h must be included BEFORE sql.h to provide base type definitions
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
// clang-format off
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
// clang-format on
#include <winhttp.h>
#include <wininet.h>
#else
#include <sql.h>
#include <sqlext.h>
#endif

#include <algorithm>
#include <any>
#include <cstdlib>
#include <memory>
#include <string>
#include <tuple>

namespace maxcompute_odbc {

// ============================================================================
// ODBC String Conversion Utilities
// ============================================================================
//
// SQLWCHAR definition varies by platform:
// - Windows: SQLWCHAR is wchar_t (UTF-16LE, 2 bytes per char)
// - Unix (default): SQLWCHAR is unsigned short (UTF-16LE, 2 bytes per char)
// - Unix (SQL_WCHART_CONVERT defined): SQLWCHAR is wchar_t
//   - macOS: wchar_t is UTF-32 (4 bytes per char)
//   - Linux: wchar_t is UTF-32 (4 bytes per char)
//
// Key insight: Both Windows and Unix (default) use UTF-16LE for SQLWCHAR.

/// Convert UTF-16 code units to UTF-8 string
inline std::string Utf16ToUtf8(const uint16_t *src, size_t len) {
  std::string result;
  result.reserve(len * 3);  // UTF-8 uses up to 3 bytes per BMP char

  for (size_t i = 0; i < len; ++i) {
    uint16_t ch = src[i];

    // Handle surrogate pairs for characters > U+FFFF
    if (ch >= 0xD800 && ch <= 0xDBFF && i + 1 < len) {
      // High surrogate
      uint16_t low = src[i + 1];
      if (low >= 0xDC00 && low <= 0xDFFF) {
        // Valid surrogate pair
        uint32_t codepoint = 0x10000 + ((ch - 0xD800) << 10) + (low - 0xDC00);
        result += static_cast<char>(0xF0 | (codepoint >> 18));
        result += static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F));
        result += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
        result += static_cast<char>(0x80 | (codepoint & 0x3F));
        i++;  // Skip low surrogate
        continue;
      }
    }

    // Convert BMP character to UTF-8
    if (ch < 0x80) {
      result += static_cast<char>(ch);
    } else if (ch < 0x800) {
      result += static_cast<char>(0xC0 | (ch >> 6));
      result += static_cast<char>(0x80 | (ch & 0x3F));
    } else {
      result += static_cast<char>(0xE0 | (ch >> 12));
      result += static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (ch & 0x3F));
    }
  }

  return result;
}

/// Convert UTF-8 string to UTF-16 code units
inline size_t Utf8ToUtf16(const std::string &input, uint16_t *output,
                          size_t max_chars) {
  size_t out_pos = 0;
  const char *src = input.c_str();
  size_t src_len = input.length();

  for (size_t i = 0; i < src_len && out_pos < max_chars; ++i) {
    unsigned char c = static_cast<unsigned char>(src[i]);
    uint32_t codepoint;

    if (c < 0x80) {
      // ASCII character
      codepoint = c;
    } else if ((c & 0xE0) == 0xC0) {
      // 2-byte UTF-8
      if (i + 1 >= src_len) break;
      codepoint =
          ((c & 0x1F) << 6) | (static_cast<unsigned char>(src[i + 1]) & 0x3F);
      i += 1;
    } else if ((c & 0xF0) == 0xE0) {
      // 3-byte UTF-8
      if (i + 2 >= src_len) break;
      codepoint = ((c & 0x0F) << 12) |
                  ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(src[i + 2]) & 0x3F);
      i += 2;
    } else if ((c & 0xF8) == 0xF0) {
      // 4-byte UTF-8 (needs surrogate pair)
      if (i + 3 >= src_len || out_pos + 1 >= max_chars) break;
      codepoint = ((c & 0x07) << 18) |
                  ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 12) |
                  ((static_cast<unsigned char>(src[i + 2]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(src[i + 3]) & 0x3F);
      i += 3;

      // Encode as surrogate pair
      codepoint -= 0x10000;
      output[out_pos++] = static_cast<uint16_t>(0xD800 | (codepoint >> 10));
      output[out_pos++] = static_cast<uint16_t>(0xDC00 | (codepoint & 0x3FF));
      continue;
    } else {
      // Invalid UTF-8 byte, skip
      continue;
    }

    // BMP character (can be represented as single UTF-16 code unit)
    output[out_pos++] = static_cast<uint16_t>(codepoint);
  }

  return out_pos;
}

/// Convert ODBC narrow string (SQLCHAR*) to std::string
inline std::string OdbcStringToStdString(const SQLCHAR *odbc_str,
                                         SQLSMALLINT length) {
  if (odbc_str == nullptr) {
    return "";
  }

  if (length == SQL_NTS) {
    // SQL_NTS: use strlen to calculate length
    return std::string(reinterpret_cast<const char *>(odbc_str));
  } else if (length < 0) {
    // Other negative values are invalid, return empty string
    return "";
  } else {
    // Explicit length given
    return std::string(reinterpret_cast<const char *>(odbc_str), length);
  }
}

/// Convert ODBC wide string (SQLWCHAR*) to std::string (UTF-8)
inline std::string OdbcWStringToStdString(const SQLWCHAR *odbc_str,
                                          SQLSMALLINT length) {
  if (odbc_str == nullptr) {
    return "";
  }

  // Calculate length
  size_t len;
  if (length == SQL_NTS) {
    // Null-terminated string
    len = 0;
    while (odbc_str[len] != 0) {
      len++;
    }
  } else if (length < 0) {
    return "";
  } else {
    len = static_cast<size_t>(length);
  }

  if (len == 0) {
    return "";
  }

#if defined(_WIN32) || defined(_WIN64)
  // Windows: SQLWCHAR is wchar_t (UTF-16LE)
  const uint16_t *src = reinterpret_cast<const uint16_t *>(odbc_str);
#else
  // Unix: SQLWCHAR is unsigned short (UTF-16LE) by default
  const uint16_t *src = reinterpret_cast<const uint16_t *>(odbc_str);
#endif

  return Utf16ToUtf8(src, len);
}

/// Convert std::string (UTF-8) to ODBC wide string (SQLWCHAR*)
inline void StdStringToOdbcWString(const std::string &input_str,
                                   SQLWCHAR *output_str,
                                   SQLSMALLINT buffer_length,
                                   SQLSMALLINT *result_length) {
  if (!output_str || buffer_length <= 0) {
    if (result_length) {
      // Report required length in bytes
      *result_length =
          static_cast<SQLSMALLINT>(input_str.length() * sizeof(SQLWCHAR));
    }
    return;
  }

  // Calculate max characters we can write (leave space for null terminator)
  size_t max_chars = static_cast<size_t>(buffer_length) / sizeof(SQLWCHAR) - 1;

#if defined(_WIN32) || defined(_WIN64)
  // Windows: SQLWCHAR is wchar_t (UTF-16LE)
  uint16_t *dest = reinterpret_cast<uint16_t *>(output_str);
#else
  // Unix: SQLWCHAR is unsigned short (UTF-16LE)
  uint16_t *dest = reinterpret_cast<uint16_t *>(output_str);
#endif

  // Convert UTF-8 to UTF-16
  size_t chars_written = Utf8ToUtf16(input_str, dest, max_chars);

  // Null terminate
  output_str[chars_written] = 0;

  // Set result length in bytes
  if (result_length) {
    *result_length = static_cast<SQLSMALLINT>(chars_written * sizeof(SQLWCHAR));
  }
}

/// Convert UTF-8 string to SQLWCHAR buffer (for column names, etc.)
/// Returns the number of characters written (excluding null terminator)
inline size_t Utf8ToOdbcWChar(const std::string &input, SQLWCHAR *output,
                              size_t max_chars) {
  uint16_t *dest = reinterpret_cast<uint16_t *>(output);
  return Utf8ToUtf16(input, dest, max_chars);
}

// ============================================================================
// General Utility Functions
// ============================================================================

/**
 * @brief 检查一个 shared_ptr<string> 是否为 nullptr，
 * 或者它指向的字符串是否为空。
 */
inline bool is_empty_or_null(const std::shared_ptr<std::string> &s) {
  return !s || s->empty();
}

inline std::string toUpper(const std::string &s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return result;
}

/**
 * @brief 如果 shared_ptr 有值，则将其解引用并包装在 std::any 中。
 */
template <typename T>
std::any to_any_if_set(const std::shared_ptr<T> &ptr) {
  return ptr ? std::any(*ptr) : std::any();
}

inline std::pair<std::string, std::string> parsePrefix(
    const std::string &endpoint) {
  std::string host_part;
  std::string path_prefix;

  size_t protocol_pos = endpoint.find("://");
  size_t search_start_pos =
      (protocol_pos == std::string::npos) ? 0 : protocol_pos + 3;
  size_t path_start_pos = endpoint.find('/', search_start_pos);

  if (path_start_pos == std::string::npos) {
    host_part = endpoint;
    path_prefix = "/";
  } else {
    host_part = endpoint.substr(0, path_start_pos);
    path_prefix = endpoint.substr(path_start_pos);
  }

  return {host_part, path_prefix};
}

inline std::pair<std::string, int> parseHostAndPort(
    const std::string &endpoint) {
  std::string authority_part = endpoint;
  size_t protocol_pos = authority_part.find("://");
  if (protocol_pos != std::string::npos) {
    authority_part = authority_part.substr(protocol_pos + 3);
  }

  size_t at_pos = authority_part.find('@');
  if (at_pos != std::string::npos) {
    authority_part = authority_part.substr(at_pos + 1);
  }

  size_t port_colon_pos = authority_part.rfind(':');
  std::string host;
  std::string port_str;
  if (port_colon_pos == std::string::npos) {
    host = authority_part;
    return {host, 0};
  } else {
    host = authority_part.substr(0, port_colon_pos);
    port_str = authority_part.substr(port_colon_pos + 1);
  }
  int port = std::stoi(port_str);
  return {host, port};
}

/**
 * @brief Parses a proxy URL and extracts host, port, username, and password.
 */
inline std::tuple<std::string, int, std::string, std::string> parseProxyUrl(
    const std::string &proxyUrl) {
  std::string authority_part = proxyUrl;

  size_t protocol_pos = authority_part.find("://");
  if (protocol_pos != std::string::npos) {
    authority_part = authority_part.substr(protocol_pos + 3);
  }

  std::string username;
  std::string password;
  std::string host;
  int port = 0;

  size_t at_pos = authority_part.rfind('@');
  if (at_pos != std::string::npos) {
    std::string user_pass = authority_part.substr(0, at_pos);
    authority_part = authority_part.substr(at_pos + 1);

    size_t colon_pos = user_pass.find(':');
    if (colon_pos != std::string::npos) {
      username = user_pass.substr(0, colon_pos);
      password = user_pass.substr(colon_pos + 1);
    } else {
      username = user_pass;
    }
  }

  size_t port_colon_pos = authority_part.rfind(':');
  if (port_colon_pos == std::string::npos) {
    host = authority_part;
  } else {
    host = authority_part.substr(0, port_colon_pos);
    std::string port_str = authority_part.substr(port_colon_pos + 1);
    try {
      port = std::stoi(port_str);
    } catch (...) {
      port = 0;
    }
  }

  return {host, port, username, password};
}

/**
 * @brief Reads proxy configuration from system environment variables.
 */
inline std::tuple<std::string, std::string, std::string>
getProxyFromEnvironment(bool isHttps = false) {
  std::string proxy_url;
  std::string username;
  std::string password;

  const char *proxy_env = nullptr;
  if (isHttps) {
    proxy_env = std::getenv("HTTPS_PROXY");
    if (!proxy_env) {
      proxy_env = std::getenv("https_proxy");
    }
  } else {
    proxy_env = std::getenv("HTTP_PROXY");
    if (!proxy_env) {
      proxy_env = std::getenv("http_proxy");
    }
  }

  if (proxy_env && strlen(proxy_env) > 0) {
    proxy_url = std::string(proxy_env);

    auto parsed = parseProxyUrl(proxy_url);
    username = std::get<2>(parsed);
    password = std::get<3>(parsed);

    if (username.empty()) {
      const char *user_env = std::getenv("HTTP_PROXY_USER");
      if (!user_env) {
        user_env = std::getenv("http_proxy_user");
      }
      if (user_env) {
        username = std::string(user_env);
      }
    }

    if (password.empty() && !username.empty()) {
      const char *pass_env = std::getenv("HTTP_PROXY_PASSWORD");
      if (!pass_env) {
        pass_env = std::getenv("http_proxy_password");
      }
      if (pass_env) {
        password = std::string(pass_env);
      }
    }
  }

  return {proxy_url, username, password};
}

#ifdef _WIN32
/**
 * @brief Reads system proxy settings from Windows using WinINet/IE proxy
 * configuration.
 */
inline std::tuple<std::string, std::string, std::string> getSystemProxy(
    bool isHttps = false) {
  std::string proxy_url;
  std::string username;
  std::string password;

  WINHTTP_CURRENT_USER_IE_PROXY_CONFIG proxyConfig = {0};

  if (WinHttpGetIEProxyConfigForCurrentUser(&proxyConfig)) {
    if (proxyConfig.fAutoDetect) {
      // Auto-detect proxy settings (WPAD)
    }

    if (proxyConfig.lpszProxy != nullptr && wcslen(proxyConfig.lpszProxy) > 0) {
      int size = WideCharToMultiByte(CP_UTF8, 0, proxyConfig.lpszProxy, -1,
                                     nullptr, 0, nullptr, nullptr);
      if (size > 0) {
        std::string proxy_str(size - 1, '\0');
        WideCharToMultiByte(CP_UTF8, 0, proxyConfig.lpszProxy, -1,
                            &proxy_str[0], size, nullptr, nullptr);

        std::string target_proxy;
        if (isHttps) {
          size_t https_pos = proxy_str.find("https=");
          if (https_pos != std::string::npos) {
            size_t semicolon_pos = proxy_str.find(';', https_pos);
            if (semicolon_pos != std::string::npos) {
              target_proxy = proxy_str.substr(https_pos + 6,
                                              semicolon_pos - https_pos - 6);
            } else {
              target_proxy = proxy_str.substr(https_pos + 6);
            }
          } else {
            size_t http_pos = proxy_str.find("http=");
            if (http_pos != std::string::npos) {
              size_t semicolon_pos = proxy_str.find(';', http_pos);
              if (semicolon_pos != std::string::npos) {
                target_proxy = proxy_str.substr(http_pos + 5,
                                                semicolon_pos - http_pos - 5);
              } else {
                target_proxy = proxy_str.substr(http_pos + 5);
              }
            } else {
              size_t semicolon_pos = proxy_str.find(';');
              if (semicolon_pos != std::string::npos) {
                target_proxy = proxy_str.substr(0, semicolon_pos);
              } else {
                target_proxy = proxy_str;
              }
            }
          }
        } else {
          size_t http_pos = proxy_str.find("http=");
          if (http_pos != std::string::npos) {
            size_t semicolon_pos = proxy_str.find(';', http_pos);
            if (semicolon_pos != std::string::npos) {
              target_proxy =
                  proxy_str.substr(http_pos + 5, semicolon_pos - http_pos - 5);
            } else {
              target_proxy = proxy_str.substr(http_pos + 5);
            }
          } else {
            size_t semicolon_pos = proxy_str.find(';');
            if (semicolon_pos != std::string::npos) {
              target_proxy = proxy_str.substr(0, semicolon_pos);
            } else {
              target_proxy = proxy_str;
            }
          }
        }

        target_proxy.erase(0, target_proxy.find_first_not_of(" \t\r\n"));
        target_proxy.erase(target_proxy.find_last_not_of(" \t\r\n") + 1);

        if (!target_proxy.empty()) {
          if (target_proxy.find("://") == std::string::npos) {
            target_proxy = (isHttps ? "https://" : "http://") + target_proxy;
          }
          proxy_url = target_proxy;
        }
      }
    }

    if (proxyConfig.lpszAutoConfigUrl != nullptr) {
      GlobalFree(proxyConfig.lpszAutoConfigUrl);
    }
    if (proxyConfig.lpszProxy != nullptr) {
      GlobalFree(proxyConfig.lpszProxy);
    }
    if (proxyConfig.lpszProxyBypass != nullptr) {
      GlobalFree(proxyConfig.lpszProxyBypass);
    }
  }

  if (!proxy_url.empty()) {
    auto parsed = parseProxyUrl(proxy_url);
    username = std::get<2>(parsed);
    password = std::get<3>(parsed);
  }

  return {proxy_url, username, password};
}
#else

/**
 * @brief Reads system proxy settings from environment variables on Linux/macOS.
 */
inline std::tuple<std::string, std::string, std::string> getSystemProxy(
    bool isHttps = false) {
  return getProxyFromEnvironment(isHttps);
}
#endif

}  // namespace maxcompute_odbc