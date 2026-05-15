#include "maxcompute_odbc/odbc_api/encoding.h"
#include <cstring>
#include <gtest/gtest.h>
#include <string>

using maxcompute_odbc::encoding::WriteUtf8AsCharset;

namespace {

// "你好" in UTF-8 is E4 BD A0  E5 A5 BD (6 bytes).
// In GBK it is C4 E3  BA C3 (4 bytes).
const std::string kNiHaoUtf8 = "\xE4\xBD\xA0\xE5\xA5\xBD";
const std::string kNiHaoGbk = "\xC4\xE3\xBA\xC3";

}  // namespace

TEST(EncodingTest, Utf8PassThrough) {
  char buf[16] = {};
  size_t total = WriteUtf8AsCharset("hello", "UTF-8", buf, sizeof(buf));
  EXPECT_EQ(total, 5u);
  EXPECT_STREQ(buf, "hello");
}

TEST(EncodingTest, EmptyCharsetTreatedAsUtf8) {
  char buf[16] = {};
  size_t total = WriteUtf8AsCharset(kNiHaoUtf8, "", buf, sizeof(buf));
  EXPECT_EQ(total, kNiHaoUtf8.size());
  EXPECT_EQ(0, std::memcmp(buf, kNiHaoUtf8.data(), kNiHaoUtf8.size()));
  EXPECT_EQ('\0', buf[kNiHaoUtf8.size()]);
}

TEST(EncodingTest, Utf8ByteBoundaryTruncation) {
  // "你好" is 6 bytes UTF-8. Buffer can hold 5 bytes (4 chars + NUL).
  // Truncating after 4 bytes would split 好 (3-byte sequence E5 A5 BD), so
  // helper should stop after 你 (3 bytes) at the last complete char boundary.
  char buf[5] = {};
  size_t total = WriteUtf8AsCharset(kNiHaoUtf8, "UTF-8", buf, sizeof(buf));
  EXPECT_EQ(total, 6u);  // total length unaffected by buffer
  // bytes written: 3 ("你"), then NUL
  EXPECT_EQ(buf[3], '\0');
  EXPECT_EQ(0, std::memcmp(buf, kNiHaoUtf8.data(), 3));
}

TEST(EncodingTest, Utf8NoTruncationWhenFits) {
  char buf[16] = {};
  size_t total = WriteUtf8AsCharset(kNiHaoUtf8, "UTF-8", buf, sizeof(buf));
  EXPECT_EQ(total, kNiHaoUtf8.size());
  EXPECT_EQ('\0', buf[kNiHaoUtf8.size()]);
}

TEST(EncodingTest, GbkConversionRoundTrip) {
  // Skip on platforms where GBK is unavailable: helper falls back to UTF-8 and
  // returns the UTF-8 byte length, so we can detect that case.
  char buf[16] = {};
  size_t total = WriteUtf8AsCharset(kNiHaoUtf8, "GBK", buf, sizeof(buf));
  if (total == kNiHaoUtf8.size()) {
    GTEST_SKIP() << "GBK not available on this platform; helper fell back to "
                    "UTF-8.";
  }
  EXPECT_EQ(total, kNiHaoGbk.size());
  EXPECT_EQ(0, std::memcmp(buf, kNiHaoGbk.data(), kNiHaoGbk.size()));
  EXPECT_EQ('\0', buf[kNiHaoGbk.size()]);
}

TEST(EncodingTest, GbkCharsetCaseInsensitive) {
  char a[16] = {};
  char b[16] = {};
  size_t ta = WriteUtf8AsCharset(kNiHaoUtf8, "GBK", a, sizeof(a));
  size_t tb = WriteUtf8AsCharset(kNiHaoUtf8, "gbk", b, sizeof(b));
  EXPECT_EQ(ta, tb);
  if (ta == kNiHaoGbk.size()) {
    EXPECT_EQ(0, std::memcmp(a, b, ta));
  }
}

TEST(EncodingTest, GbkBoundaryTruncation) {
  // "你好" → 4 bytes in GBK. Buffer holds 3 bytes (room for 1 char + NUL).
  // The helper must stop after 1 char (2 bytes) and not split a multi-byte
  // sequence.
  char buf[3] = {};
  size_t total = WriteUtf8AsCharset(kNiHaoUtf8, "GBK", buf, sizeof(buf));
  if (total == kNiHaoUtf8.size()) {
    GTEST_SKIP() << "GBK not available on this platform.";
  }
  EXPECT_EQ(total, kNiHaoGbk.size());  // total reported, not capped
  // Buffer should contain "你" (2 bytes) + NUL — never the partial 3 bytes.
  EXPECT_EQ('\0', buf[2]);
  EXPECT_EQ(0, std::memcmp(buf, kNiHaoGbk.data(), 2));
}

TEST(EncodingTest, EmptyInputProducesEmptyOutput) {
  char buf[8];
  std::memset(buf, 'X', sizeof(buf));
  size_t total = WriteUtf8AsCharset("", "GBK", buf, sizeof(buf));
  EXPECT_EQ(total, 0u);
  EXPECT_EQ('\0', buf[0]);
}

TEST(EncodingTest, UnsupportedCharsetFallsBackToUtf8) {
  char buf[16] = {};
  size_t total = WriteUtf8AsCharset(kNiHaoUtf8, "DEFINITELY-NOT-A-REAL-CHARSET",
                                    buf, sizeof(buf));
  // Fallback writes UTF-8 bytes verbatim.
  EXPECT_EQ(total, kNiHaoUtf8.size());
  EXPECT_EQ(0, std::memcmp(buf, kNiHaoUtf8.data(), kNiHaoUtf8.size()));
}

TEST(EncodingTest, ZeroSizedBufferReportsLengthOnly) {
  // Passing buffer_size == 0 must not write anything but should still return
  // the converted length so callers can size a buffer.
  size_t total = WriteUtf8AsCharset(kNiHaoUtf8, "UTF-8", nullptr, 0);
  EXPECT_EQ(total, kNiHaoUtf8.size());
}
