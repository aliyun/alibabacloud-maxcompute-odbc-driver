#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/common/utils.h"
#include "maxcompute_odbc/odbc_api/encoding.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <set>
#include <string>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <iconv.h>
#endif

namespace maxcompute_odbc::encoding {

namespace {

std::string toUpperCopy(const std::string &s) {
  std::string r;
  r.reserve(s.size());
  for (unsigned char c : s) r.push_back(static_cast<char>(std::toupper(c)));
  return r;
}

bool isUtf8Charset(const std::string &charset) {
  if (charset.empty()) return true;
  std::string upper = toUpperCopy(charset);
  return upper == "UTF-8" || upper == "UTF8";
}

// Walk back from `max_bytes` to a UTF-8 character boundary. Returns the number
// of leading bytes that form a complete sequence of UTF-8 codepoints.
size_t SafeUtf8TruncatePoint(const std::string &s, size_t max_bytes) {
  if (max_bytes >= s.size()) return s.size();
  size_t pos = max_bytes;
  while (pos > 0 && (static_cast<unsigned char>(s[pos]) & 0xC0) == 0x80) {
    pos--;
  }
  return pos;
}

void WarnUnsupportedOnce(const std::string &charset, const char *context) {
  static std::mutex mu;
  static std::set<std::string> seen;
  std::lock_guard<std::mutex> lock(mu);
  if (seen.insert(charset).second) {
    MCO_LOG_WARNING(
        "Unsupported charset '{}' ({}); falling back to UTF-8 output", charset,
        context);
  }
}

size_t WriteUtf8Direct(const std::string &utf8, char *buffer,
                       size_t buffer_size) {
  if (buffer == nullptr || buffer_size == 0) return utf8.size();
  size_t copy = SafeUtf8TruncatePoint(utf8, buffer_size - 1);
  if (copy > 0) std::memcpy(buffer, utf8.data(), copy);
  buffer[copy] = '\0';
  return utf8.size();
}

#ifdef _WIN32

UINT CharsetToCodePage(const std::string &charset_upper) {
  if (charset_upper == "GBK" || charset_upper == "CP936" ||
      charset_upper == "GB2312") {
    return 936;
  }
  if (charset_upper == "GB18030") return 54936;
  if (charset_upper == "BIG5" || charset_upper == "CP950") return 950;
  if (charset_upper == "SHIFT_JIS" || charset_upper == "SHIFT-JIS" ||
      charset_upper == "SJIS" || charset_upper == "CP932") {
    return 932;
  }
  if (charset_upper == "EUC-KR" || charset_upper == "EUCKR" ||
      charset_upper == "CP949") {
    return 949;
  }
  if (charset_upper == "UTF-8" || charset_upper == "UTF8") return CP_UTF8;
  // Numeric code page like "936"
  if (!charset_upper.empty() &&
      std::all_of(charset_upper.begin(), charset_upper.end(), [](char c) {
        return std::isdigit(static_cast<unsigned char>(c));
      })) {
    try {
      return static_cast<UINT>(std::stoul(charset_upper));
    } catch (...) {
      return 0;
    }
  }
  return 0;  // unsupported
}

size_t WriteUtf8AsCharsetWin(const std::string &utf8, UINT cp, char *buffer,
                             size_t buffer_size) {
  if (utf8.empty()) {
    if (buffer && buffer_size > 0) buffer[0] = '\0';
    return 0;
  }

  // UTF-8 -> UTF-16
  int wlen = MultiByteToWideChar(CP_UTF8, 0, utf8.data(),
                                 static_cast<int>(utf8.size()), nullptr, 0);
  if (wlen <= 0) {
    return WriteUtf8Direct(utf8, buffer, buffer_size);
  }
  std::wstring wbuf(static_cast<size_t>(wlen), L'\0');
  MultiByteToWideChar(CP_UTF8, 0, utf8.data(), static_cast<int>(utf8.size()),
                      &wbuf[0], wlen);

  // Total bytes needed in the target codepage.
  int total_bytes = WideCharToMultiByte(cp, 0, wbuf.data(), wlen, nullptr, 0,
                                        nullptr, nullptr);
  if (total_bytes <= 0) {
    return WriteUtf8Direct(utf8, buffer, buffer_size);
  }

  if (buffer == nullptr || buffer_size == 0) {
    return static_cast<size_t>(total_bytes);
  }

  size_t out_capacity = buffer_size - 1;
  if (static_cast<size_t>(total_bytes) <= out_capacity) {
    WideCharToMultiByte(cp, 0, wbuf.data(), wlen, buffer, total_bytes, nullptr,
                        nullptr);
    buffer[total_bytes] = '\0';
    return static_cast<size_t>(total_bytes);
  }

  // Truncation needed: convert wide chars chunk-by-chunk, respecting surrogate
  // pairs, until the next chunk would overflow.
  size_t bytes_written = 0;
  size_t i = 0;
  while (i < wbuf.size()) {
    int chunk = 1;
    wchar_t hi = wbuf[i];
    if (hi >= 0xD800 && hi <= 0xDBFF && i + 1 < wbuf.size()) {
      wchar_t lo = wbuf[i + 1];
      if (lo >= 0xDC00 && lo <= 0xDFFF) chunk = 2;
    }
    int needed = WideCharToMultiByte(cp, 0, &wbuf[i], chunk, nullptr, 0,
                                     nullptr, nullptr);
    if (needed <= 0) {
      // Skip this codepoint silently (cannot represent in target charset).
      i += chunk;
      continue;
    }
    if (bytes_written + static_cast<size_t>(needed) > out_capacity) break;
    WideCharToMultiByte(cp, 0, &wbuf[i], chunk, buffer + bytes_written, needed,
                        nullptr, nullptr);
    bytes_written += static_cast<size_t>(needed);
    i += chunk;
  }
  buffer[bytes_written] = '\0';
  return static_cast<size_t>(total_bytes);
}

#else  // POSIX iconv

// Run iconv with input `(in, in_left)`, writing to `(out, out_left)`. Returns
// the bytes written into `out`. On exit, `*in` and `*in_left` reflect the
// unconsumed input (so a follow-up call can count the rest).
size_t IconvStep(iconv_t cd, char **in, size_t *in_left, char *out,
                 size_t out_capacity) {
  char *out_p = out;
  size_t out_left = out_capacity;
  // Loop on EILSEQ/EINVAL to skip a single byte and continue, so a malformed
  // sequence doesn't abort the whole conversion.
  while (*in_left > 0 && out_left > 0) {
    size_t r = iconv(cd, in, in_left, &out_p, &out_left);
    if (r != static_cast<size_t>(-1)) break;
    if (errno == E2BIG) break;
    if (errno == EILSEQ || errno == EINVAL) {
      // Skip one input byte and try again.
      if (*in_left > 0) {
        (*in)++;
        (*in_left)--;
      } else {
        break;
      }
    } else {
      break;
    }
  }
  return out_capacity - out_left;
}

size_t WriteUtf8AsCharsetIconv(const std::string &utf8,
                               const std::string &charset_upper, char *buffer,
                               size_t buffer_size) {
  iconv_t cd = iconv_open(charset_upper.c_str(), "UTF-8");
  if (cd == reinterpret_cast<iconv_t>(-1)) {
    WarnUnsupportedOnce(charset_upper, "iconv_open failed");
    return WriteUtf8Direct(utf8, buffer, buffer_size);
  }

  std::string in_copy = utf8;  // iconv wants non-const input pointer
  char *in = in_copy.data();
  size_t in_left = in_copy.size();

  size_t bytes_written = 0;
  if (buffer != nullptr && buffer_size > 0) {
    bytes_written = IconvStep(cd, &in, &in_left, buffer, buffer_size - 1);
    buffer[bytes_written] = '\0';
  } else {
    // Caller wants only the length (passes null buffer or zero size).
  }

  // Count remaining bytes via a scratch buffer.
  size_t remainder_bytes = 0;
  char tmp[1024];
  while (in_left > 0) {
    size_t step = IconvStep(cd, &in, &in_left, tmp, sizeof(tmp));
    if (step == 0 && in_left > 0) {
      // Defensive: avoid infinite loop if iconv made no progress and
      // didn't error out cleanly.
      break;
    }
    remainder_bytes += step;
  }

  iconv_close(cd);
  return bytes_written + remainder_bytes;
}

#endif

}  // namespace

size_t WriteUtf8AsCharset(const std::string &utf8, const std::string &charset,
                          char *buffer, size_t buffer_size) {
  if (isUtf8Charset(charset)) {
    return WriteUtf8Direct(utf8, buffer, buffer_size);
  }

  std::string upper = toUpperCopy(charset);

#ifdef _WIN32
  UINT cp = CharsetToCodePage(upper);
  if (cp == 0) {
    WarnUnsupportedOnce(charset, "unknown Windows code page");
    return WriteUtf8Direct(utf8, buffer, buffer_size);
  }
  return WriteUtf8AsCharsetWin(utf8, cp, buffer, buffer_size);
#else
  return WriteUtf8AsCharsetIconv(utf8, upper, buffer, buffer_size);
#endif
}

}  // namespace maxcompute_odbc::encoding
