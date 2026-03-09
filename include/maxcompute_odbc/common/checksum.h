#pragma once

#include <crc32c/crc32c.h>
#include <cstdint>  // For uint8_t, int32_t, etc.
#include <string>   // For std::string
#include <vector>   // For std::vector

// 编译时检查平台字节序，如果不是小端序，则会编译失败。
// 这确保了我们的字节转换行为和 Java 的 ByteOrder.LITTLE_ENDIAN 一致。
#if !defined(__LITTLE_ENDIAN__) && !defined(_WIN32) && \
        !defined(__BYTE_ORDER__) ||                    \
    (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#if !defined(_MSC_VER)  // MSVC and most modern compilers on x86/ARM are
                        // little-endian by default
// #warning "This code assumes a little-endian architecture for direct
// type-to-byte conversion."
#endif
#endif

/**
 * @brief A C++ utility for calculating CRC32-C checksums, mirroring the Java
 * counterpart.
 *
 * This class uses Google's high-performance crc32c library, which leverages
 * hardware acceleration (SSE4.2, ARM CRC) when available.
 */
class Checksum {
 public:
  Checksum() : crc_(0) {}

  /**
   * @brief Updates the checksum with the little-endian byte representation of
   * an int32_t.
   */
  void update(int32_t v) {
    // 直接将 int32_t 的内存内容作为字节序列更新
    crc_ =
        crc32c::Extend(crc_, reinterpret_cast<const uint8_t *>(&v), sizeof(v));
  }

  /**
   * @brief Updates the checksum with the little-endian byte representation of
   * an uint32_t.
   */
  void update(uint32_t v) {
    // 直接将 uint32_t 的内存内容作为字节序列更新
    crc_ =
        crc32c::Extend(crc_, reinterpret_cast<const uint8_t *>(&v), sizeof(v));
  }

  /**
   * @brief Updates the checksum with the little-endian byte representation of
   * an int64_t.
   */
  void update(int64_t v) {
    crc_ =
        crc32c::Extend(crc_, reinterpret_cast<const uint8_t *>(&v), sizeof(v));
  }

  /**
   * @brief Updates the checksum with the little-endian byte representation of a
   * double.
   */
  void update(double v) {
    crc_ =
        crc32c::Extend(crc_, reinterpret_cast<const uint8_t *>(&v), sizeof(v));
  }

  /**
   * @brief Updates the checksum with the little-endian byte representation of a
   * float.
   */
  void update(float v) {
    crc_ =
        crc32c::Extend(crc_, reinterpret_cast<const uint8_t *>(&v), sizeof(v));
  }

  /**
   * @brief Updates the checksum with a single byte representing a boolean.
   */
  void update(bool v) {
    // 与 Java 代码行为一致：true 为 1，false 为 0
    static constexpr uint8_t TRUE_BYTE = 1;
    static constexpr uint8_t FALSE_BYTE = 0;
    crc_ = crc32c::Extend(crc_, v ? &TRUE_BYTE : &FALSE_BYTE, 1);
  }

  /**
   * @brief Updates the checksum with a buffer of bytes.
   * @param b Pointer to the start of the buffer.
   * @param len Number of bytes to process.
   */
  void update(const uint8_t *b, size_t len) {
    if (b && len > 0) {
      crc_ = crc32c::Extend(crc_, b, len);
    }
  }

  /**
   * @brief Updates the checksum with a C-style string.
   */
  void update(const char *s, size_t len) {
    update(reinterpret_cast<const uint8_t *>(s), len);
  }

  /**
   * @brief Updates the checksum with a std::string.
   */
  void update(const std::string &s) {
    update(reinterpret_cast<const uint8_t *>(s.data()), s.length());
  }

  /**
   * @brief Updates the checksum with a std::vector<uint8_t>.
   */
  void update(const std::vector<uint8_t> &vec) {
    update(vec.data(), vec.size());
  }

  /**
   * @brief Gets the current 32-bit CRC value.
   * @return The calculated checksum.
   */
  uint32_t getValue() const { return crc_; }

  /**
   * @brief Resets the checksum calculation to its initial state.
   */
  void reset() { crc_ = 0; }

 private:
  uint32_t crc_;
};
