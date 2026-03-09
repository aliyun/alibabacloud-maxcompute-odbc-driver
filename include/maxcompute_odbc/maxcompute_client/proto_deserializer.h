#pragma once

#include "maxcompute_odbc/common/checksum.h"
#include "maxcompute_odbc/common/errors.h"
#include "maxcompute_odbc/maxcompute_client/models.h"  // Assuming Record, TypeInfo, etc. are here.
#include <bitset>
#include <memory>
#include <optional>
#include <stdexcept>
#include <variant>
#include <vector>
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream.h"

// Forward declaration if needed, to reduce include dependencies.
namespace maxcompute_odbc {
class ResultSetSchema;
}

namespace maxcompute_odbc {

// The maximum number of columns supported by the ODPS tunnel protocol.
// This is useful for fixed-size containers like std::bitset.
constexpr size_t MAX_ODPS_COLUMNS = 4096;

/**
 * @class ProtoDeserializer
 * @brief Deserializes a stream of MaxCompute Tunnel V6 Protobuf data into
 * Records.
 *
 * This class is responsible for parsing the low-level Protobuf wire format,
 * handling records, metadata, and checksums according to the tunnel protocol.
 * It is designed to be used in a streaming fashion.
 *
 * @note This class assumes the input stream is ready for parsing (e.g., already
 * decompressed if the original source was compressed). The responsibility of
 * setting up the stream pipeline (network -> decompression) lies with the
 * caller.
 */
class ProtoDeserializer {
 public:
  /**
   * @brief Constructs a ProtoDeserializer.
   *
   * @param in The input stream providing Protobuf data. The deserializer takes
   * ownership.
   * @param schema The schema corresponding to the data in the stream. Must
   * outlive the deserializer.
   * @param doStreamChecksum If true, calculates checksums in stream mode.
   * @param protobufTotalBytesLimit The size limit for a single message chunk in
   * Protobuf's CodedInputStream.
   * @param timezone The timezone for timestamp handling, defaults to "UTC".
   */
  ProtoDeserializer(
      std::unique_ptr<google::protobuf::io::ZeroCopyInputStream> in,
      const ResultSetSchema *schema, bool doStreamChecksum = false,
      int protobufTotalBytesLimit = -1,  // Default to Protobuf's own default.
      const std::string &timezone = "UTC");

  // Disable copy and move semantics for simplicity and to prevent misuse.
  ProtoDeserializer(const ProtoDeserializer &) = delete;
  ProtoDeserializer &operator=(const ProtoDeserializer &) = delete;

  /**
   * @brief Deserializes the next record from the stream.
   *
   * This method will read from the input stream until a complete record is
   * parsed or the end of the stream is reached. It may block if the underlying
   * stream blocks.
   *
   * @return A Result containing an optional Record:
   *         - Success, optional has value: A record was successfully
   * deserialized.
   *         - Success, optional is empty: The end of the stream was reached
   * cleanly.
   *         - Failure: An error occurred during deserialization (e.g., data
   * corruption, checksum mismatch).
   */
  Result<std::optional<Record>> deserializeNext();

 private:
  // Enum to represent the outcome of processing a meta tag.
  enum class MetaTagResult { Continue, EndOfStream };

  // Helper methods for internal logic
  Result<std::optional<Record>> buildAndReturnRecord();
  Result<void> processColumnData(uint32_t tag, uint32_t fieldNum);
  Result<MetaTagResult> processMetaTag();

  // Reworked helpers for complex and nested types
  Result<ColumnData> readComplexValue(const typeinfo &type);
  Result<ColumnData> readComplexValueImpl(const typeinfo &type);
  Result<ColumnData> readNestedValue(const typeinfo &type);
  // Core components
  std::unique_ptr<google::protobuf::io::ZeroCopyInputStream> mInputStream;
  std::unique_ptr<google::protobuf::io::CodedInputStream> mCodedInputStream;
  const ResultSetSchema *mSchema;  // Non-owning pointer

  // State for the current record being built
  std::optional<Record> mCurrentRecord;
  std::bitset<MAX_ODPS_COLUMNS> mNullMap;

  // Deserialization state
  bool mIsStreamComplete = false;
  int64_t mReceivedTotalRecords = 0;
  int mProtobufTotalBytesLimit;
  int64_t mBytesReadAtLastReset = 0;

  // Checksum related state
  bool mDoStreamChecksum;
  Checksum mRecordCrc;      // Per-record CRC
  Checksum mCrcOfCrc;       // CRC of per-record CRCs
  Checksum mCodedChecksum;  // Checksum for stream mode

  // Timezone configuration
  std::string mTimezone;
};
}  // namespace maxcompute_odbc
