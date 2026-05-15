#pragma once

#include <cstddef>
#include <string>

namespace maxcompute_odbc::encoding {

/**
 * Convert a UTF-8 string into a target charset and write it to a SQL_C_CHAR
 * buffer.
 *
 * Behavior:
 * - Truncates at character boundaries (never writes a partial multi-byte
 *   character; the next byte after the last complete character is the NUL).
 * - Always NUL-terminates the buffer when buffer_size > 0.
 * - Returns the *total* converted byte length (excluding NUL), so callers
 *   can fill SQL_LEN_or_Ind correctly. If the return value is greater than
 *   buffer_size - 1, the data was truncated.
 *
 * Recognized charset values (case-insensitive, normalized to UPPER):
 *   "UTF-8" / "UTF8"  -> no conversion (UTF-8 byte truncation only)
 *   "GBK" / "CP936" / "GB2312" -> Windows code page 936
 *   "GB18030"
 *   "BIG5" / "CP950"
 *   "SHIFT_JIS" / "SJIS" / "CP932"
 *   "EUC-KR" / "CP949"
 * Unknown values fall through to iconv (Unix) / numeric code-page parsing
 * (Windows). On unsupported charset or conversion error, the function falls
 * back to writing UTF-8 bytes (with character-boundary truncation) and logs a
 * warning at most once per process per unsupported charset.
 *
 * @param utf8        Source UTF-8 string.
 * @param charset     Target charset name.
 * @param buffer      Destination buffer (may be nullptr if buffer_size == 0).
 * @param buffer_size Size of the destination buffer in bytes.
 * @return Total byte length the converted output requires (excluding NUL).
 */
size_t WriteUtf8AsCharset(const std::string &utf8, const std::string &charset,
                          char *buffer, size_t buffer_size);

}  // namespace maxcompute_odbc::encoding
