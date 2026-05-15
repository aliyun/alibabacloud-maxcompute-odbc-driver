#include "maxcompute_odbc/odbc_api/conversions.h"
#include "maxcompute_odbc/odbc_api/encoding.h"
#include "maxcompute_odbc/platform.h"
#include <algorithm>  // for std::transform
#include <cctype>     // for ::tolower
#include <chrono>
#include <cstring>  // for strncpy
#include <ctime>
#include <cwchar>  // for mbstowcs
#include <iomanip>
#include <sstream>
#include <stdexcept>  // for std::stol, etc.
#include <string>
#include <variant>

#ifndef _WIN32
// On non-Windows we can use the date library if available
#include "date/date.h"
#endif

namespace maxcompute_odbc {

// Portable conversion: assume McDate::epochDays is days since Unix epoch
// (1970-01-01)
SQL_DATE_STRUCT to_sql_date(const McDate &mc_date) {
#ifndef _WIN32
  const date::sys_days sys_days_since_epoch{date::days(mc_date.epochDays)};
  const date::year_month_day ymd{sys_days_since_epoch};

  SQL_DATE_STRUCT odbc_date{};
  odbc_date.year = static_cast<SQLSMALLINT>(static_cast<int>(ymd.year()));
  odbc_date.month =
      static_cast<SQLUSMALLINT>(static_cast<unsigned>(ymd.month()));
  odbc_date.day = static_cast<SQLUSMALLINT>(static_cast<unsigned>(ymd.day()));
  return odbc_date;
#else
  time_t seconds = static_cast<time_t>(mc_date.epochDays) * 86400;
  tm t;
#if defined(_MSC_VER)
  gmtime_s(&t, &seconds);
#else
  gmtime_r(&seconds, &t);
#endif
  SQL_DATE_STRUCT odbc_date{};
  odbc_date.year = static_cast<SQLSMALLINT>(t.tm_year + 1900);
  odbc_date.month = static_cast<SQLUSMALLINT>(t.tm_mon + 1);
  odbc_date.day = static_cast<SQLUSMALLINT>(t.tm_mday);
  return odbc_date;
#endif
}

SQL_TIMESTAMP_STRUCT to_sql_timestamp(const McTimestamp &mc_ts) {
#ifndef _WIN32
  const auto duration_ns = std::chrono::seconds(mc_ts.epochSeconds) +
                           std::chrono::nanoseconds(mc_ts.nanoseconds);
  const date::sys_time<std::chrono::nanoseconds> utc_tp{duration_ns};

  const auto dp = date::floor<date::days>(utc_tp);
  const date::year_month_day ymd{dp};
  const date::hh_mm_ss hms{utc_tp - dp};

  SQL_TIMESTAMP_STRUCT odbc_ts{};
  odbc_ts.year = static_cast<SQLSMALLINT>(static_cast<int>(ymd.year()));
  odbc_ts.month = static_cast<SQLUSMALLINT>(static_cast<unsigned>(ymd.month()));
  odbc_ts.day = static_cast<SQLUSMALLINT>(static_cast<unsigned>(ymd.day()));
  odbc_ts.hour = static_cast<SQLUSMALLINT>(hms.hours().count());
  odbc_ts.minute = static_cast<SQLUSMALLINT>(hms.minutes().count());
  odbc_ts.second = static_cast<SQLUSMALLINT>(hms.seconds().count());
  odbc_ts.fraction = static_cast<SQLUINTEGER>(hms.subseconds().count());
  return odbc_ts;
#else
  // epochSeconds is seconds since Unix epoch
  time_t sec = static_cast<time_t>(mc_ts.epochSeconds);
  tm t;
#if defined(_MSC_VER)
  gmtime_s(&t, &sec);
#else
  gmtime_r(&sec, &t);
#endif
  SQL_TIMESTAMP_STRUCT odbc_ts{};
  odbc_ts.year = static_cast<SQLSMALLINT>(t.tm_year + 1900);
  odbc_ts.month = static_cast<SQLUSMALLINT>(t.tm_mon + 1);
  odbc_ts.day = static_cast<SQLUSMALLINT>(t.tm_mday);
  odbc_ts.hour = static_cast<SQLUSMALLINT>(t.tm_hour);
  odbc_ts.minute = static_cast<SQLUSMALLINT>(t.tm_min);
  odbc_ts.second = static_cast<SQLUSMALLINT>(t.tm_sec);
  // fraction is nanoseconds; SQL_TIMESTAMP_STRUCT.fraction holds fractional
  // seconds
  odbc_ts.fraction = static_cast<SQLUINTEGER>(mc_ts.nanoseconds);
  return odbc_ts;
#endif
}

SQLRETURN convertAndWrite(const ColumnData &data,
                          const StmtHandle::ColumnBinding &binding,
                          const std::string &charset) {
  // 步骤 1: 处理 NULL 值 (这部分逻辑保持不变)
  if (std::holds_alternative<std::monostate>(data)) {
    if (binding.indicator_ptr) {
      *(binding.indicator_ptr) = SQL_NULL_DATA;
    }
    return SQL_SUCCESS;
  }

  MCO_LOG_DEBUG("Call convert and write, target type {}",
                std::to_string(binding.target_type));

  // 步骤 2: 使用 std::visit 进行类型安全的直接转换
  return std::visit(
      [&](auto &&arg) -> SQLRETURN {
        using T = std::decay_t<decltype(arg)>;

        // 根据绑定的目标类型进行转换
        switch (binding.target_type) {
          // --- 文本类型 ---
          case SQL_C_CHAR:  // SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR
          {
            std::string str_val;
            // 将各种源类型转换为字符串
            if constexpr (std::is_same_v<T, std::string>) {
              str_val = arg;
            } else if constexpr (std::is_same_v<T, int64_t> ||
                                 std::is_same_v<T, double> ||
                                 std::is_same_v<T, bool>) {
              str_val = std::to_string(arg);
            } else if constexpr (std::is_same_v<T, McDate>) {
          // Format YYYY-MM-DD
#ifndef _WIN32
              const auto odbc_date = to_sql_date(arg);
              str_val =
                  date::format("%Y-%m-%d", date::year(odbc_date.year) /
                                               odbc_date.month / odbc_date.day);
#else
              const auto d = to_sql_date(arg);
              std::ostringstream ss;
              ss << std::setw(4) << std::setfill('0') << d.year << "-"
                 << std::setw(2) << std::setfill('0') << d.month << "-"
                 << std::setw(2) << std::setfill('0') << d.day;
              str_val = ss.str();
#endif
            } else if constexpr (std::is_same_v<T, McTimestamp>) {
#ifndef _WIN32
              const auto duration_ns =
                  std::chrono::seconds(arg.epochSeconds) +
                  std::chrono::nanoseconds(arg.nanoseconds);
              const date::sys_time<std::chrono::nanoseconds> utc_tp{
                  duration_ns};
              auto sec_tp = date::floor<std::chrono::seconds>(utc_tp);
              auto subseconds =
                  std::chrono::duration_cast<std::chrono::nanoseconds>(utc_tp -
                                                                       sec_tp)
                      .count();
              str_val = date::format("%Y-%m-%d %H:%M:%S", sec_tp);
              if (subseconds > 0) {
                char buf[32];
                int len = snprintf(buf, sizeof(buf), ".%09lld",
                                   static_cast<long long>(subseconds));
                int trim = 0;
                for (int i = len - 1; i >= 0; --i) {
                  if (buf[i] == '0')
                    ++trim;
                  else
                    break;
                }
                if (trim > 0) buf[len - trim] = '\0';
                str_val += buf;
              }
#else
              // Format UTC timestamp
              auto sec = static_cast<time_t>(arg.epochSeconds);
              tm t;
#if defined(_MSC_VER)
              gmtime_s(&t, &sec);
#else
              gmtime_r(&sec, &t);
#endif
              char buf[64];
              int n =
                  snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                           t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                           t.tm_min, t.tm_sec);
              if (arg.nanoseconds > 0) {
                int len = snprintf(buf + n, sizeof(buf) - n, ".%09lld",
                                   static_cast<long long>(arg.nanoseconds));
                // trim trailing zeros
                int trim = 0;
                for (int i = n + len - 1; i >= n; --i) {
                  if (buf[i] == '0')
                    ++trim;
                  else
                    break;
                }
                if (trim > 0) buf[n + len - trim] = '\0';
              }
              str_val = buf;
#endif
            } else {
              str_val = "<unsupported_type_to_string>";
            }

            // 通过 encoding helper 完成 UTF-8 -> 目标 charset 转换、
            // 字符边界截断与 NUL 终止. 返回值是转换后的总字节长度,
            // 用于报告 StrLen_or_IndPtr (供应用程序判断截断或继续读取).
            char *out_buf = static_cast<char *>(binding.target_buffer);
            size_t out_size = binding.buffer_length > 0
                                  ? static_cast<size_t>(binding.buffer_length)
                                  : 0;
            size_t total_bytes = encoding::WriteUtf8AsCharset(
                str_val, charset, out_buf, out_size);
            if (binding.indicator_ptr)
              *binding.indicator_ptr = static_cast<SQLLEN>(total_bytes);

            MCO_LOG_DEBUG("format str {}", str_val);
            bool truncated =
                out_size == 0 ? total_bytes > 0 : total_bytes > out_size - 1;
            return truncated ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
          }
          // 更多宽字符处理...

          // --- 精确数值类型 ---
          case SQL_C_SSHORT:  // SQL_SMALLINT
          {
            if constexpr (std::is_same_v<T, signed short int>) {
              // Direct assignment from same type
              *static_cast<SQLSMALLINT *>(binding.target_buffer) = arg;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLSMALLINT);
            } else if constexpr (std::is_same_v<T, int>) {
              // Convert from int to SQLSMALLINT with range checking
              if (arg >= std::numeric_limits<SQLSMALLINT>::min() &&
                  arg <= std::numeric_limits<SQLSMALLINT>::max()) {
                *static_cast<SQLSMALLINT *>(binding.target_buffer) =
                    static_cast<SQLSMALLINT>(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLSMALLINT);
              } else {
                return SQL_ERROR;  // Overflow
              }
            } else if constexpr (std::is_same_v<T, int64_t>) {
              // Convert from int64_t to SQLSMALLINT with range checking
              if (arg >= std::numeric_limits<SQLSMALLINT>::min() &&
                  arg <= std::numeric_limits<SQLSMALLINT>::max()) {
                *static_cast<SQLSMALLINT *>(binding.target_buffer) =
                    static_cast<SQLSMALLINT>(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLSMALLINT);
              } else {
                return SQL_ERROR;  // Overflow
              }
            } else if constexpr (std::is_same_v<T, double>) {
              // Convert from double to SQLSMALLINT with range checking
              if (arg >= std::numeric_limits<SQLSMALLINT>::min() &&
                  arg <= std::numeric_limits<SQLSMALLINT>::max()) {
                *static_cast<SQLSMALLINT *>(binding.target_buffer) =
                    static_cast<SQLSMALLINT>(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLSMALLINT);
              } else {
                return SQL_ERROR;  // Overflow
              }
            } else if constexpr (std::is_same_v<T, bool>) {
              // Convert from bool to SQLSMALLINT (0 or 1)
              *static_cast<SQLSMALLINT *>(binding.target_buffer) = arg ? 1 : 0;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLSMALLINT);
            } else if constexpr (std::is_same_v<T, std::string>) {
              try {
                long val = std::stol(arg);
                if (val >= std::numeric_limits<SQLSMALLINT>::min() &&
                    val <= std::numeric_limits<SQLSMALLINT>::max()) {
                  *static_cast<SQLSMALLINT *>(binding.target_buffer) =
                      static_cast<SQLSMALLINT>(val);
                  if (binding.indicator_ptr)
                    *binding.indicator_ptr = sizeof(SQLSMALLINT);
                } else {
                  return SQL_ERROR;  // Overflow
                }
              } catch (...) {
                return SQL_ERROR;
              }
            } else { /* 不支持的转换 */
              return SQL_ERROR;
            }
            return SQL_SUCCESS;
          }
          case SQL_C_SLONG:  // SQL_INTEGER
          {
            if constexpr (std::is_same_v<T, int>) {
              // Direct assignment from same type
              *static_cast<SQLINTEGER *>(binding.target_buffer) = arg;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLINTEGER);
            } else if constexpr (std::is_same_v<T, signed short int>) {
              // Convert from signed short int to SQLINTEGER
              *static_cast<SQLINTEGER *>(binding.target_buffer) =
                  static_cast<SQLINTEGER>(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLINTEGER);
            } else if constexpr (std::is_same_v<T, int64_t>) {
              // Convert from int64_t to SQLINTEGER with range checking
              if (arg >= std::numeric_limits<SQLINTEGER>::min() &&
                  arg <= std::numeric_limits<SQLINTEGER>::max()) {
                *static_cast<SQLINTEGER *>(binding.target_buffer) =
                    static_cast<SQLINTEGER>(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLINTEGER);
              } else {
                return SQL_ERROR;  // Overflow
              }
            } else if constexpr (std::is_same_v<T, double>) {
              // Convert from double to SQLINTEGER with range checking
              if (arg >= std::numeric_limits<SQLINTEGER>::min() &&
                  arg <= std::numeric_limits<SQLINTEGER>::max()) {
                *static_cast<SQLINTEGER *>(binding.target_buffer) =
                    static_cast<SQLINTEGER>(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLINTEGER);
              } else {
                return SQL_ERROR;  // Overflow
              }
            } else if constexpr (std::is_same_v<T, bool>) {
              // Convert from bool to SQLINTEGER (0 or 1)
              *static_cast<SQLINTEGER *>(binding.target_buffer) = arg ? 1 : 0;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLINTEGER);
            } else if constexpr (std::is_same_v<T, std::string>) {
              try {
                long val = std::stol(arg);
                if (val >= std::numeric_limits<SQLINTEGER>::min() &&
                    val <= std::numeric_limits<SQLINTEGER>::max()) {
                  *static_cast<SQLINTEGER *>(binding.target_buffer) =
                      static_cast<SQLINTEGER>(val);
                  if (binding.indicator_ptr)
                    *binding.indicator_ptr = sizeof(SQLINTEGER);
                } else {
                  return SQL_ERROR;  // Overflow
                }
              } catch (...) {
                return SQL_ERROR;
              }
            } else { /* 不支持的转换 */
              return SQL_ERROR;
            }
            return SQL_SUCCESS;
          }
          case SQL_C_SBIGINT:  // SQL_BIGINT
          {
            if constexpr (std::is_same_v<T, int64_t>) {
              // Direct assignment from same type
              *static_cast<SQLBIGINT *>(binding.target_buffer) = arg;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLBIGINT);
            } else if constexpr (std::is_same_v<T, int>) {
              // Convert from int to SQLBIGINT
              *static_cast<SQLBIGINT *>(binding.target_buffer) =
                  static_cast<SQLBIGINT>(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLBIGINT);
            } else if constexpr (std::is_same_v<T, signed short int>) {
              // Convert from signed short int to SQLBIGINT
              *static_cast<SQLBIGINT *>(binding.target_buffer) =
                  static_cast<SQLBIGINT>(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLBIGINT);
            } else if constexpr (std::is_same_v<T, double>) {
              // Convert from double to SQLBIGINT with range checking
              if (arg >= std::numeric_limits<SQLBIGINT>::min() &&
                  arg <= std::numeric_limits<SQLBIGINT>::max()) {
                *static_cast<SQLBIGINT *>(binding.target_buffer) =
                    static_cast<SQLBIGINT>(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLBIGINT);
              } else {
                return SQL_ERROR;  // Overflow
              }
            } else if constexpr (std::is_same_v<T, bool>) {
              // Convert from bool to SQLBIGINT (0 or 1)
              *static_cast<SQLBIGINT *>(binding.target_buffer) = arg ? 1 : 0;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLBIGINT);
            } else if constexpr (std::is_same_v<T, std::string>) {
              try {
                *static_cast<SQLBIGINT *>(binding.target_buffer) =
                    std::stoll(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLBIGINT);
              } catch (...) {
                return SQL_ERROR;
              }
            } else { /* 不支持的转换 */
              return SQL_ERROR;
            }
            return SQL_SUCCESS;
          }

          // --- 浮点数值类型 ---
          case SQL_C_DOUBLE:  // SQL_DOUBLE, SQL_DECIMAL, etc.
          {
            if constexpr (std::is_same_v<T, double>) {
              // Direct assignment from same type
              *static_cast<SQLDOUBLE *>(binding.target_buffer) = arg;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLDOUBLE);
            } else if constexpr (std::is_same_v<T, int>) {
              // Convert from int to SQLDOUBLE
              *static_cast<SQLDOUBLE *>(binding.target_buffer) =
                  static_cast<double>(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLDOUBLE);
            } else if constexpr (std::is_same_v<T, signed short int>) {
              // Convert from signed short int to SQLDOUBLE
              *static_cast<SQLDOUBLE *>(binding.target_buffer) =
                  static_cast<double>(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLDOUBLE);
            } else if constexpr (std::is_same_v<T, int64_t>) {
              // Convert from int64_t to SQLDOUBLE
              *static_cast<SQLDOUBLE *>(binding.target_buffer) =
                  static_cast<double>(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLDOUBLE);
            } else if constexpr (std::is_same_v<T, bool>) {
              // Convert from bool to SQLDOUBLE (0.0 or 1.0)
              *static_cast<SQLDOUBLE *>(binding.target_buffer) =
                  arg ? 1.0 : 0.0;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQLDOUBLE);
            } else if constexpr (std::is_same_v<T, std::string>) {
              try {
                *static_cast<SQLDOUBLE *>(binding.target_buffer) =
                    std::stod(arg);
                if (binding.indicator_ptr)
                  *binding.indicator_ptr = sizeof(SQLDOUBLE);
              } catch (...) {
                return SQL_ERROR;
              }
            } else { /* 不支持的转换 */
              return SQL_ERROR;
            }
            return SQL_SUCCESS;
          }

          // --- 日期和时间戳类型 ---
          case SQL_C_TYPE_DATE:  // SQL_DATE
          {
            if constexpr (std::is_same_v<T, McDate>) {
              *static_cast<SQL_DATE_STRUCT *>(binding.target_buffer) =
                  to_sql_date(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQL_DATE_STRUCT);
            } else { /* 不支持的转换 */
              return SQL_ERROR;
            }
            return SQL_SUCCESS;
          }
          case SQL_C_TYPE_TIMESTAMP:  // SQL_DATETIME, SQL_TIMESTAMP
          {
            if constexpr (std::is_same_v<T, McTimestamp>) {
              *static_cast<SQL_TIMESTAMP_STRUCT *>(binding.target_buffer) =
                  to_sql_timestamp(arg);
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQL_TIMESTAMP_STRUCT);
            } else if constexpr (std::is_same_v<T, McDate>) {
              // Convert McDate to SQL_TIMESTAMP_STRUCT with time set to
              // midnight
              SQL_TIMESTAMP_STRUCT ts;
              const auto sql_date = to_sql_date(arg);
              ts.year = sql_date.year;
              ts.month = sql_date.month;
              ts.day = sql_date.day;
              ts.hour = 0;
              ts.minute = 0;
              ts.second = 0;
              ts.fraction = 0;
              *static_cast<SQL_TIMESTAMP_STRUCT *>(binding.target_buffer) = ts;
              if (binding.indicator_ptr)
                *binding.indicator_ptr = sizeof(SQL_TIMESTAMP_STRUCT);
            } else { /* 不支持的转换 */
              return SQL_ERROR;
            }
            return SQL_SUCCESS;
          }

          // --- 其他类型 ---
          case SQL_C_BIT:  // SQL_BIT
          {
            if constexpr (std::is_same_v<T, bool>) {
              *static_cast<unsigned char *>(binding.target_buffer) =
                  arg ? 1 : 0;
              if (binding.indicator_ptr) *binding.indicator_ptr = 1;
            } else if constexpr (std::is_same_v<T, int>) {
              *static_cast<unsigned char *>(binding.target_buffer) =
                  (arg != 0) ? 1 : 0;
              if (binding.indicator_ptr) *binding.indicator_ptr = 1;
            } else if constexpr (std::is_same_v<T, signed short int>) {
              *static_cast<unsigned char *>(binding.target_buffer) =
                  (arg != 0) ? 1 : 0;
              if (binding.indicator_ptr) *binding.indicator_ptr = 1;
            } else if constexpr (std::is_same_v<T, int64_t>) {
              *static_cast<unsigned char *>(binding.target_buffer) =
                  (arg != 0) ? 1 : 0;
              if (binding.indicator_ptr) *binding.indicator_ptr = 1;
            } else if constexpr (std::is_same_v<T, double>) {
              *static_cast<unsigned char *>(binding.target_buffer) =
                  (arg != 0.0) ? 1 : 0;
              if (binding.indicator_ptr) *binding.indicator_ptr = 1;
            } else if constexpr (std::is_same_v<T, std::string>) {
              try {
                // Parse string to boolean: "true", "1", "yes", "y" = true;
                // others = false
                std::string lower_str = arg;
                std::transform(lower_str.begin(), lower_str.end(),
                               lower_str.begin(), ::tolower);

                bool value = false;
                if (lower_str == "true" || lower_str == "1" ||
                    lower_str == "yes" || lower_str == "y") {
                  value = true;
                } else {
                  // Try to convert to number and check if non-zero
                  try {
                    value = (std::stod(lower_str) != 0.0);
                  } catch (...) {
                    // If conversion fails, treat as false
                    value = false;
                  }
                }

                *static_cast<unsigned char *>(binding.target_buffer) =
                    value ? 1 : 0;
                if (binding.indicator_ptr) *binding.indicator_ptr = 1;
              } catch (...) {
                return SQL_ERROR;
              }
            } else { /* 不支持的转换 */
              return SQL_ERROR;
            }
            return SQL_SUCCESS;
          }

          // --- Wide character type (SQL_C_WCHAR = -8) ---
          // pyodbc on Unix requests data as SQL_C_WCHAR (UTF-16)
          case SQL_C_WCHAR:  // -8
          {
            std::string str_val;
            // Convert various source types to string (same as SQL_C_CHAR)
            if constexpr (std::is_same_v<T, std::string>) {
              str_val = arg;
            } else if constexpr (std::is_same_v<T, int64_t>) {
              str_val = std::to_string(arg);
            } else if constexpr (std::is_same_v<T, int>) {
              str_val = std::to_string(arg);
            } else if constexpr (std::is_same_v<T, signed short int>) {
              str_val = std::to_string(arg);
            } else if constexpr (std::is_same_v<T, double>) {
              str_val = std::to_string(arg);
            } else if constexpr (std::is_same_v<T, bool>) {
              str_val = arg ? "1" : "0";
            } else if constexpr (std::is_same_v<T, McDate>) {
#ifndef _WIN32
              const auto odbc_date = to_sql_date(arg);
              str_val =
                  date::format("%Y-%m-%d", date::year(odbc_date.year) /
                                               odbc_date.month / odbc_date.day);
#else
              const auto d = to_sql_date(arg);
              std::ostringstream ss;
              ss << std::setw(4) << std::setfill('0') << d.year << "-"
                 << std::setw(2) << std::setfill('0') << d.month << "-"
                 << std::setw(2) << std::setfill('0') << d.day;
              str_val = ss.str();
#endif
            } else if constexpr (std::is_same_v<T, McTimestamp>) {
#ifndef _WIN32
              const auto duration_ns =
                  std::chrono::seconds(arg.epochSeconds) +
                  std::chrono::nanoseconds(arg.nanoseconds);
              const date::sys_time<std::chrono::nanoseconds> utc_tp{
                  duration_ns};
              auto sec_tp = date::floor<std::chrono::seconds>(utc_tp);
              auto subseconds =
                  std::chrono::duration_cast<std::chrono::nanoseconds>(utc_tp -
                                                                       sec_tp)
                      .count();
              str_val = date::format("%Y-%m-%d %H:%M:%S", sec_tp);
              if (subseconds > 0) {
                char buf[32];
                int len = snprintf(buf, sizeof(buf), ".%09lld",
                                   static_cast<long long>(subseconds));
                int trim = 0;
                for (int i = len - 1; i >= 0; --i) {
                  if (buf[i] == '0')
                    ++trim;
                  else
                    break;
                }
                if (trim > 0) buf[len - trim] = '\0';
                str_val += buf;
              }
#else
              auto sec = static_cast<time_t>(arg.epochSeconds);
              tm t;
#if defined(_MSC_VER)
              gmtime_s(&t, &sec);
#else
              gmtime_r(&sec, &t);
#endif
              char buf[64];
              int n =
                  snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                           t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                           t.tm_min, t.tm_sec);
              if (arg.nanoseconds > 0) {
                int len = snprintf(buf + n, sizeof(buf) - n, ".%09lld",
                                   static_cast<long long>(arg.nanoseconds));
                int trim = 0;
                for (int i = n + len - 1; i >= n; --i) {
                  if (buf[i] == '0')
                    ++trim;
                  else
                    break;
                }
                if (trim > 0) buf[n + len - trim] = '\0';
              }
              str_val = buf;
#endif
            } else {
              str_val = "<unsupported_type>";
            }

            // Convert UTF-8 string to UTF-16 (SQLWCHAR)
            // Buffer length is in bytes, calculate max UTF-16 chars
            size_t max_chars =
                static_cast<size_t>(binding.buffer_length) / sizeof(SQLWCHAR) -
                1;  // Leave space for null terminator

            // UTF-8 to UTF-16 conversion
            size_t out_pos = 0;
            uint16_t *dest =
                reinterpret_cast<uint16_t *>(binding.target_buffer);
            const char *src = str_val.c_str();
            size_t src_len = str_val.length();

            for (size_t i = 0; i < src_len && out_pos < max_chars; ++i) {
              unsigned char c = static_cast<unsigned char>(src[i]);
              uint32_t codepoint;

              if (c < 0x80) {
                codepoint = c;
              } else if ((c & 0xE0) == 0xC0) {
                if (i + 1 >= src_len) break;
                codepoint = ((c & 0x1F) << 6) |
                            (static_cast<unsigned char>(src[i + 1]) & 0x3F);
                i += 1;
              } else if ((c & 0xF0) == 0xE0) {
                if (i + 2 >= src_len) break;
                codepoint =
                    ((c & 0x0F) << 12) |
                    ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 6) |
                    (static_cast<unsigned char>(src[i + 2]) & 0x3F);
                i += 2;
              } else if ((c & 0xF8) == 0xF0) {
                if (i + 3 >= src_len || out_pos + 1 >= max_chars) break;
                codepoint =
                    ((c & 0x07) << 18) |
                    ((static_cast<unsigned char>(src[i + 1]) & 0x3F) << 12) |
                    ((static_cast<unsigned char>(src[i + 2]) & 0x3F) << 6) |
                    (static_cast<unsigned char>(src[i + 3]) & 0x3F);
                i += 3;
                // Encode as surrogate pair
                codepoint -= 0x10000;
                dest[out_pos++] =
                    static_cast<uint16_t>(0xD800 | (codepoint >> 10));
                dest[out_pos++] =
                    static_cast<uint16_t>(0xDC00 | (codepoint & 0x3FF));
                continue;
              } else {
                continue;
              }
              dest[out_pos++] = static_cast<uint16_t>(codepoint);
            }

            // Null terminate
            dest[out_pos] = 0;

            if (binding.indicator_ptr) {
              *binding.indicator_ptr =
                  static_cast<SQLLEN>(out_pos * sizeof(SQLWCHAR));
            }

            MCO_LOG_DEBUG("SQL_C_WCHAR: converted {} bytes to {} UTF-16 chars",
                          str_val.length(), out_pos);
            // Always return SQL_SUCCESS since we successfully converted the
            // data
            return SQL_SUCCESS;
          }

          default:
            // 对于未知的目标类型，可以返回错误或执行一个默认行为
            return SQL_ERROR;
        }
      },
      data);
}

Result<std::unique_ptr<OdbcColumn>> convertColumn(const Column &column) {
  auto col = std::make_unique<OdbcColumn>();
  col->name = column.name;

  const typeinfo &type_info_ref = *column.type_info;
  switch (type_info_ref.getTypeID()) {
    // === 整数类型 ===
    case OdpsType::TINYINT:
      col->sql_type = SQL_BIGINT;
      col->column_size = 3;                  // -128 to 127
      col->octet_length = sizeof(SQLSCHAR);  // 1 byte for SQL_TINYINT
      break;
    case OdpsType::SMALLINT:
      col->sql_type = SQL_BIGINT;
      col->column_size = 5;
      col->octet_length = sizeof(SQLSMALLINT);  // 2 bytes for SQL_SMALLINT
      break;
    case OdpsType::INT:
      col->sql_type = SQL_BIGINT;
      col->column_size = 10;
      col->octet_length = sizeof(SQLINTEGER);  // 4 bytes for SQL_INTEGER
      break;
    case OdpsType::BIGINT:
      col->sql_type = SQL_BIGINT;
      col->column_size = 19;
      col->octet_length = sizeof(SQLBIGINT);  // 8 bytes for SQL_BIGINT
      break;

    // === 浮点/定点 ===
    case OdpsType::FLOAT:
      col->sql_type = SQL_DOUBLE;
      col->column_size = 7;
      col->octet_length = sizeof(SQLREAL);  // 4 bytes for SQL_REAL
      break;
    case OdpsType::DOUBLE:
      col->sql_type = SQL_DOUBLE;
      col->column_size = 15;
      col->octet_length = sizeof(SQLDOUBLE);  // 8 bytes for SQL_DOUBLE
      break;
    // case OdpsType::DECIMAL: {
    //   const auto *decimal_info =
    //       dynamic_cast<const DecimalTypeInfo *>(&type_info_ref);
    //   col->sql_type = SQL_DECIMAL;
    //   col->column_size = decimal_info->getPrecision();
    //   col->decimal_digits = decimal_info->getScale();
    //   col->octet_length =
    //       decimal_info
    //           ->getPrecision();  // For character representation of DECIMAL
    //   break;
    // }
    //  === 字符串 ===
    case OdpsType::VARCHAR: {
      const auto *varchar_info =
          dynamic_cast<const VarcharTypeInfo *>(&type_info_ref);
      col->sql_type = SQL_VARCHAR;
      col->column_size = varchar_info->getLength();
      col->octet_length =
          varchar_info->getLength();  // Same as column_size for VARCHAR
      break;
    }
    case OdpsType::CHAR: {
      const auto *char_info =
          dynamic_cast<const CharTypeInfo *>(&type_info_ref);
      col->sql_type = SQL_CHAR;
      col->column_size = char_info->getLength();
      col->octet_length =
          char_info->getLength();  // Same as column_size for CHAR
      break;
    }
    case OdpsType::STRING:
    case OdpsType::JSON:
    case OdpsType::BINARY:
    case OdpsType::DECIMAL:
      col->sql_type = SQL_VARCHAR;
      col->column_size = 65535;
      col->octet_length = 65535;  // Same as column_size for STRING
      break;

    case OdpsType::BOOLEAN:
      col->sql_type = SQL_BIT;
      col->column_size = 1;
      col->octet_length = sizeof(SQLCHAR);  // 1 byte for SQL_BIT
      break;

    case OdpsType::DATE:
      col->sql_type = SQL_TYPE_DATE;
      col->column_size = 10;                        // "YYYY-MM-DD"
      col->octet_length = sizeof(SQL_DATE_STRUCT);  // Size of SQL_DATE_STRUCT
      break;

    case OdpsType::DATETIME:
      col->sql_type = SQL_TYPE_TIMESTAMP;
      col->column_size = 29;  // "YYYY-MM-DD HH:MM:SS.fffffffff"
      col->decimal_digits = 0;
      col->octet_length =
          sizeof(SQL_TIMESTAMP_STRUCT);  // Size of SQL_TIMESTAMP_STRUCT
      break;

    case OdpsType::TIMESTAMP:
      col->sql_type = SQL_TYPE_TIMESTAMP;
      col->column_size = 29;  // 包含纳秒
      col->decimal_digits = 9;
      col->octet_length =
          sizeof(SQL_TIMESTAMP_STRUCT);  // Size of SQL_TIMESTAMP_STRUCT
      break;

    case OdpsType::TIMESTAMP_NTZ:
      col->sql_type = SQL_TYPE_TIMESTAMP;
      col->column_size = 29;  // 包含纳秒
      col->decimal_digits = 9;
      col->octet_length =
          sizeof(SQL_TIMESTAMP_STRUCT);  // Size of SQL_TIMESTAMP_STRUCT
      break;

    // === 复杂类型（暂不支持，fallback）===
    case OdpsType::ARRAY:
    case OdpsType::MAP:
    case OdpsType::STRUCT:
    default:
      // Fallback to VARCHAR to avoid breaking
      col->sql_type = SQL_VARCHAR;
      col->column_size = 65535;
      col->octet_length = 65535;  // Same as column_size for fallback
      MCO_LOG_WARNING("Unsupported MaxCompute type '{}', mapped to SQL_VARCHAR",
                      type_info_ref.toString());
      break;
  }
  return makeSuccess<std::unique_ptr<OdbcColumn>>(std::move(col));
}

Result<std::unique_ptr<ResultStream>> convertTable(
    const Table &table, const std::string &column_pattern) {
  try {
    auto builder = std::make_unique<StaticResultSetBuilder>();
    builder
        ->addColumn("TABLE_CAT", PrimitiveTypeInfo(OdpsType::STRING))   // 1
        .addColumn("TABLE_SCHEM", PrimitiveTypeInfo(OdpsType::STRING))  // 2
        .addColumn("TABLE_NAME", PrimitiveTypeInfo(OdpsType::STRING))   // 3
        .addColumn("COLUMN_NAME", PrimitiveTypeInfo(OdpsType::STRING))  // 4
        .addColumn("DATA_TYPE", PrimitiveTypeInfo(OdpsType::SMALLINT))  // 5
        .addColumn("TYPE_NAME", PrimitiveTypeInfo(OdpsType::STRING))    // 6
        .addColumn("COLUMN_SIZE", PrimitiveTypeInfo(OdpsType::INT))     // 7
        .addColumn("BUFFER_LENGTH", PrimitiveTypeInfo(OdpsType::INT))   // 8
        .addColumn("DECIMAL_DIGITS",
                   PrimitiveTypeInfo(OdpsType::SMALLINT))  // 9
        .addColumn("NUM_PREC_RADIX",
                   PrimitiveTypeInfo(OdpsType::SMALLINT))              // 10
        .addColumn("NULLABLE", PrimitiveTypeInfo(OdpsType::SMALLINT))  // 11
        .addColumn("REMARKS", PrimitiveTypeInfo(OdpsType::STRING))     // 12
        .addColumn("COLUMN_DEF", PrimitiveTypeInfo(OdpsType::STRING))  // 13
        .addColumn("SQL_DATA_TYPE",
                   PrimitiveTypeInfo(OdpsType::SMALLINT))  // 14
        .addColumn("SQL_DATETIME_SUB",
                   PrimitiveTypeInfo(OdpsType::SMALLINT))                  // 15
        .addColumn("CHAR_OCTET_LENGTH", PrimitiveTypeInfo(OdpsType::INT))  // 16
        .addColumn("ORDINAL_POSITION", PrimitiveTypeInfo(OdpsType::INT))   // 17
        .addColumn("IS_NULLABLE", PrimitiveTypeInfo(OdpsType::STRING));    // 18

    bool match_all = column_pattern.empty() || column_pattern == "%";

    int columnIndex = 1;

    table.forEachColumn([&](const Column &col) {
      if (!match_all && col.name != column_pattern) {
        return;
      }
      auto odbcColumnRes = convertColumn(col);
      if (!odbcColumnRes.has_value()) {
        throw std::runtime_error(odbcColumnRes.error().message);
      }
      auto &odbcColumn = odbcColumnRes.value();

      builder->addRowValues(table.project_name,          // TABLE_CAT
                            table.schema_name,           // TABLE_SCHEM
                            table.name,                  // TABLE_NAME
                            col.name,                    // COLUMN_NAME
                            odbcColumn->sql_type,        // DATA_TYPE
                            col.type_info->toString(),   // TYPE_NAME
                            odbcColumn->column_size,     // COLUMN_SIZE
                            SQL_C_DEFAULT,               // BUFFER_LENGTH
                            odbcColumn->decimal_digits,  // DECIMAL_DIGITS
                            10,                          // NUM_PREC_RADIX
                            SQL_NULLABLE_UNKNOWN,        // NULLABLE
                            std::monostate{},            // REMARKS (null)
                            std::monostate{},            // COLUMN_DEF(null)
                            odbcColumn->sql_type,        // SQL_DATA_TYPE
                            std::monostate{},  // SQL_DATETIME_SUB (null)
                            16 * 1024 * 1024,  // CHAR_OCTET_LENGTH (16M)
                            columnIndex,       // ORDINAL_POSITION
                            ""                 // IS_NULLABLE
      );
      columnIndex++;
    });

    std::unique_ptr<ResultStream> stream = builder->build();

    return makeSuccess(std::move(stream));

  } catch (const std::exception &e) {
    return makeError<std::unique_ptr<ResultStream>>(ErrorCode::UnknownError,
                                                    e.what());
  }
}

Result<std::unique_ptr<ResultStream>> convertTables(
    const std::vector<TableId> &tables) {
  try {
    auto builder = std::make_unique<StaticResultSetBuilder>();
    builder
        ->addColumn("TABLE_CAT", PrimitiveTypeInfo(OdpsType::STRING))   // 1
        .addColumn("TABLE_SCHEM", PrimitiveTypeInfo(OdpsType::STRING))  // 2
        .addColumn("TABLE_NAME", PrimitiveTypeInfo(OdpsType::STRING))   // 3
        .addColumn("TABLE_TYPE", PrimitiveTypeInfo(OdpsType::STRING))   // 4
        .addColumn("REMARKS", PrimitiveTypeInfo(OdpsType::STRING));     // 5

    for (const auto &tableId : tables) {
      builder->addRowValues(
          tableId.project_name,  // TABLE_CAT
          tableId.schema_name,   // TABLE_SCHEM
          tableId.name,          // TABLE_NAME
          "TABLE",  // TABLE_TYPE, currently all table seem as 'table'
          ""        // REMARKS
      );
    }
    std::unique_ptr<ResultStream> stream = builder->build();

    return makeSuccess(std::move(stream));

  } catch (const std::exception &e) {
    return makeError<std::unique_ptr<ResultStream>>(ErrorCode::UnknownError,
                                                    e.what());
  }
}

}  // namespace maxcompute_odbc
