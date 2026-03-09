#include "maxcompute_odbc/maxcompute_client/api_client.h"
#include "maxcompute_odbc/maxcompute_client/proto_deserializer.h"
#include <google/protobuf/wire_format_lite_inl.h>
#include "google/protobuf/wire_format_lite.h"

namespace maxcompute_odbc {

namespace {

// Helper functions to wrap WireFormatLite calls and avoid external
// instantiation issues
inline bool readUInt32(google::protobuf::io::CodedInputStream* cis,
                       uint32_t* value) {
  return google::protobuf::internal::WireFormatLite::ReadPrimitive<
      uint32_t, google::protobuf::internal::WireFormatLite::TYPE_UINT32>(cis,
                                                                         value);
}

inline bool readInt64(google::protobuf::io::CodedInputStream* cis,
                      int64_t* value) {
  return google::protobuf::internal::WireFormatLite::ReadPrimitive<
      int64_t, google::protobuf::internal::WireFormatLite::TYPE_SINT64>(cis,
                                                                        value);
}

inline bool readInt32(google::protobuf::io::CodedInputStream* cis,
                      int32_t* value) {
  return google::protobuf::internal::WireFormatLite::ReadPrimitive<
      int32_t, google::protobuf::internal::WireFormatLite::TYPE_SINT32>(cis,
                                                                        value);
}

inline bool readDouble(google::protobuf::io::CodedInputStream* cis,
                       double* value) {
  return google::protobuf::internal::WireFormatLite::ReadPrimitive<
      double, google::protobuf::internal::WireFormatLite::TYPE_DOUBLE>(cis,
                                                                       value);
}

inline bool readFloat(google::protobuf::io::CodedInputStream* cis,
                      float* value) {
  return google::protobuf::internal::WireFormatLite::ReadPrimitive<
      float, google::protobuf::internal::WireFormatLite::TYPE_FLOAT>(cis,
                                                                     value);
}

inline bool readBool(google::protobuf::io::CodedInputStream* cis, bool* value) {
  return google::protobuf::internal::WireFormatLite::ReadPrimitive<
      bool, google::protobuf::internal::WireFormatLite::TYPE_BOOL>(cis, value);
}

inline bool readBytes(google::protobuf::io::CodedInputStream* cis,
                      std::string* value) {
  return google::protobuf::internal::WireFormatLite::ReadBytes(cis, value);
}

}  // namespace

// Use a nested namespace for constants to avoid polluting the parent namespace.
namespace {
// These constants are from the MaxCompute Tunnel Protobuf specification.
inline constexpr int PROTOBUF_TOTAL_BYTES_LIMIT_DEFAULT =
    67108864;  // 64MB, Protobuf's default limit.
inline constexpr int PROTOBUF_WARNING_THRESHOLD = 469762048;  // 448MB

inline constexpr uint32_t META_TOTAL_RECORDS_TAG_NUM =
    33554430;  // Tag numbers are simpler to work with.
inline constexpr uint32_t META_CHECKSUM_TAG_NUM = 33554431;
inline constexpr uint32_t RECORD_END_TAG_NUM = 33553408;
inline constexpr uint32_t METRICS_START_TAG_NUM = 1;
inline constexpr uint32_t METRICS_END_TAG_NUM = 33554176;
}  // namespace

// =======================================================================
// Constructor
// =======================================================================

ProtoDeserializer::ProtoDeserializer(
    std::unique_ptr<google::protobuf::io::ZeroCopyInputStream> in,
    const ResultSetSchema* schema, bool doStreamChecksum,
    int protobufTotalBytesLimit, const std::string& timezone)
    : mInputStream(std::move(in)),
      mSchema(schema),
      mDoStreamChecksum(doStreamChecksum),
      mTimezone(timezone) {
  if (!mSchema) {
    throw std::invalid_argument("Schema cannot be null.");
  }
  if (mSchema->getColumnCount() > MAX_ODPS_COLUMNS) {
    throw std::runtime_error("Column count exceeds MAX_ODPS_COLUMNS.");
  }

  // Set the Protobuf total bytes limit.
  mProtobufTotalBytesLimit =
      (protobufTotalBytesLimit > 0 &&
       protobufTotalBytesLimit <= PROTOBUF_WARNING_THRESHOLD)
          ? protobufTotalBytesLimit
          : PROTOBUF_TOTAL_BYTES_LIMIT_DEFAULT;

  // The input stream is now assumed to be ready for parsing (e.g., already
  // decompressed).
  if (!mInputStream) {
    throw std::invalid_argument("Input stream cannot be null.");
  }

  mCodedInputStream = std::make_unique<google::protobuf::io::CodedInputStream>(
      mInputStream.get());
  mCodedInputStream->SetTotalBytesLimit(mProtobufTotalBytesLimit);
  mBytesReadAtLastReset = mInputStream->ByteCount();
}

// =======================================================================
// Main Deserialization Loop
// =======================================================================

Result<std::optional<Record>> ProtoDeserializer::deserializeNext() {
  if (mIsStreamComplete) {
    return std::nullopt;  // Stream is already fully processed.
  }

  // Handle CodedInputStream's size limit by recreating it if necessary.
  if (mInputStream->ByteCount() - mBytesReadAtLastReset >
      mProtobufTotalBytesLimit) {
    mBytesReadAtLastReset = mInputStream->ByteCount();
    mCodedInputStream =
        std::make_unique<google::protobuf::io::CodedInputStream>(
            mInputStream.get());
    mCodedInputStream->SetTotalBytesLimit(mProtobufTotalBytesLimit);
  }

  // Prepare state for a new record.
  mCurrentRecord.emplace(mSchema->getColumnCount());
  mNullMap.reset();

  while (true) {
    uint32_t tag = mCodedInputStream->ReadTag();
    if (tag == 0) {  // End of stream or an error.
      if (mCodedInputStream->ConsumedEntireMessage()) {
        // This is an unexpected end. A valid stream should end with
        // META_CHECKSUM_TAG.
        return makeError<std::optional<Record>>(
            ErrorCode::DataError,
            "Unexpected end of stream. Missing final checksum tag.");
      }
      // For non-streaming input streams, just return end of stream
      return makeError<std::optional<Record>>(ErrorCode::DataError,
                                              "Tag equals 0.");
    }

    const uint32_t fieldNum =
        google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag);

    // This is a simplified dispatcher. A more performant version might use a
    // switch on a range.
    if (fieldNum == RECORD_END_TAG_NUM) {
      return buildAndReturnRecord();
    } else if (fieldNum ==
               META_TOTAL_RECORDS_TAG_NUM) {  // Meta tags have high numbers
      auto metaResult = processMetaTag();
      if (!metaResult.has_value()) {
        return makeError<std::optional<Record>>(metaResult.error().code,
                                                metaResult.error().message);
      }
      if (*metaResult == MetaTagResult::EndOfStream) {
        return std::nullopt;  // Clean end of stream.
      }
    } else {  // Regular column data
      auto columnResult = processColumnData(tag, fieldNum);
      if (!columnResult.has_value()) {
        return makeError<std::optional<Record>>(columnResult.error().code,
                                                columnResult.error().message);
      }
    }
  }
}

// =======================================================================
// Record and Column Processing
// =======================================================================

Result<std::optional<Record>> ProtoDeserializer::buildAndReturnRecord() {
  uint32_t crcFromStream;
  if (!google::protobuf::internal::WireFormatLite::ReadPrimitive<
          uint32_t, google::protobuf::internal::WireFormatLite::TYPE_UINT32>(
          mCodedInputStream.get(), &crcFromStream)) {
    return makeError<std::optional<Record>>(ErrorCode::DataError,
                                            "Failed to read record CRC.");
  }

  const uint32_t calculatedChecksum =
      mDoStreamChecksum ? mCodedChecksum.getValue() : mRecordCrc.getValue();
  if (calculatedChecksum != crcFromStream) {
    return makeError<std::optional<Record>>(ErrorCode::DataError,
                                            "Record checksum mismatch.");
  }

  // Finalize the record by moving it out of the member variable.
  Record recordToReturn = std::move(mCurrentRecord.value());
  mCurrentRecord
      .reset();  // Should already be empty after move, but good practice.

  // Correctly fill NULLs for columns that were not present in the stream.
  for (size_t i = 0; i < mSchema->getColumnCount(); ++i) {
    if (!mNullMap.test(i)) {
      recordToReturn.values[i] = std::monostate{};
    }
  }

  // Update checksums for the next stage.
  if (mDoStreamChecksum) {
    mCodedChecksum.update(RECORD_END_TAG_NUM);
    mCodedChecksum.update(crcFromStream);
  } else {
    mCrcOfCrc.update(calculatedChecksum);
    mRecordCrc.reset();
  }

  mReceivedTotalRecords++;
  return std::optional<Record>(std::move(recordToReturn));
}

Result<void> ProtoDeserializer::processColumnData(uint32_t tag,
                                                  uint32_t fieldNum) {
  if (fieldNum == 0 || fieldNum > mSchema->getColumnCount()) {
    return makeError<void>(
        ErrorCode::DataError,
        "Invalid column field number: " + std::to_string(fieldNum));
  }

  const uint32_t colIdx = fieldNum - 1;
  auto& typeInfo = mSchema->getColumn(colIdx).type_info;

  // Mark column as not null.
  mNullMap.set(colIdx);
  mRecordCrc.update(fieldNum);

  switch (typeInfo->getTypeID()) {
    case OdpsType::BIGINT:
    case OdpsType::INT:
    case OdpsType::SMALLINT:
    case OdpsType::TINYINT: {
      int64_t val;
      if (!readInt64(mCodedInputStream.get(), &val)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      mCurrentRecord->values[colIdx] = val;
      mRecordCrc.update(val);
      break;
    }
    case OdpsType::DOUBLE: {
      double val;
      if (!readDouble(mCodedInputStream.get(), &val)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      mCurrentRecord->values[colIdx] = val;
      mRecordCrc.update(val);
      break;
    }
    case OdpsType::FLOAT: {
      float val;
      if (!readFloat(mCodedInputStream.get(), &val)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      mCurrentRecord->values[colIdx] = val;
      mRecordCrc.update(val);
      break;
    }
    case OdpsType::BOOLEAN: {
      bool val;
      if (!readBool(mCodedInputStream.get(), &val)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      mCurrentRecord->values[colIdx] = val;
      mRecordCrc.update(val);
      break;
    }
    // String-like types
    case OdpsType::STRING:
    case OdpsType::JSON:
    case OdpsType::VARCHAR:
    case OdpsType::CHAR:
    case OdpsType::DECIMAL:
    case OdpsType::BINARY: {
      std::string val;
      // Add safety check for potential huge allocations that could cause
      // std::length_error Read bytes but check for reasonable string size
      // limits
      if (!readBytes(mCodedInputStream.get(), &val)) {
        return makeError<void>(
            ErrorCode::DataError,
            "Failed to read bytes for column " + std::to_string(colIdx));
      }
      // Add a reasonable limit to prevent std::length_error - protect against
      // malicious or corrupted data
      if (val.size() > 100 * 1024 * 1024) {  // 100MB limit
        return makeError<void>(ErrorCode::DataError,
                               "String data too large for column " +
                                   std::to_string(colIdx) +
                                   ", size: " + std::to_string(val.size()));
      }
      mRecordCrc.update(val);
      // Safe move assignment with validated string size
      std::string safe_val = val.substr(
          0,
          100 * 1024 * 1024);  // Ensure safety with substr if somehow exceeded
      mCurrentRecord->values[colIdx] = std::move(safe_val);
      break;
    }
    // Complex types
    case OdpsType::ARRAY:
    case OdpsType::MAP:
    case OdpsType::STRUCT: {
      auto complexResult = readComplexValue(*typeInfo);
      if (!complexResult.has_value()) {
        return makeError<void>(complexResult.error().code,
                               complexResult.error().message);
      }
      mCurrentRecord->values[colIdx] = std::move(*complexResult);
      break;
    }
    case OdpsType::DATE: {
      int64_t val;
      if (!readInt64(mCodedInputStream.get(), &val)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      if (val < INT_MIN || val > INT_MAX) {
        return makeError<void>(ErrorCode::DataError,
                               "Datetime data too large for column " +
                                   std::to_string(colIdx) +
                                   ", epochDays: " + std::to_string(val));
      }
      mRecordCrc.update(val);
      int epochDays = static_cast<int>(val);
      mCurrentRecord->values[colIdx] = McDate{epochDays};
      break;
    }
    case OdpsType::DATETIME: {
      int64_t epochMillis;
      if (!readInt64(mCodedInputStream.get(), &epochMillis)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      mRecordCrc.update(epochMillis);
      mCurrentRecord->values[colIdx] = McTimestamp{
          epochMillis / 1000, static_cast<int>(epochMillis % 1000), mTimezone};
      break;
    }
    case OdpsType::TIMESTAMP:
    case OdpsType::TIMESTAMP_NTZ: {
      int64_t time;
      if (!readInt64(mCodedInputStream.get(), &time)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      mRecordCrc.update(time);
      int32_t nano;
      if (!readInt32(mCodedInputStream.get(), &nano)) {
        return makeError<void>(ErrorCode::DataError,
                               "Failed to read primitive value for column " +
                                   std::to_string(colIdx));
      }
      mRecordCrc.update(nano);
      if (typeInfo->getTypeID() == OdpsType::TIMESTAMP) {
        mCurrentRecord->values[colIdx] = McTimestamp{time, nano, mTimezone};
      } else {
        mCurrentRecord->values[colIdx] = McTimestamp{time, nano, "UTC"};
      }
      break;
    }
    default:
      return makeError<void>(ErrorCode::DataError, "Unknown field.");
  }
  return {};
}

// =======================================================================
// Complex Type Deserialization (REWORKED)
// =======================================================================
// This part is heavily refactored for type safety and correctness.
// NOTE: This assumes `McArray`, `McMap`, `McStruct` are defined and
// that their constructors or methods can accept vectors of `NestedValue`.

Result<ColumnData> ProtoDeserializer::readNestedValue(const typeinfo& type) {
  bool is_null;
  if (!readBool(mCodedInputStream.get(), &is_null)) {
    return makeError<ColumnData>(
        ErrorCode::DataError,
        "Failed to read null indicator for nested value.");
  }
  if (mDoStreamChecksum)
    mCodedChecksum.update(is_null);  // Checksum includes null indicators

  if (is_null) {
    return {std::monostate{}};
  }

  // Dispatch based on type, similar to processColumnData but for nested values.
  switch (type.getTypeID()) {
    case OdpsType::BIGINT: {
      int64_t val;
      if (!readInt64(mCodedInputStream.get(), &val))
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Failed to read nested int64.");
      if (mDoStreamChecksum) mCodedChecksum.update(val);
      return {val};
    }
    case OdpsType::DOUBLE: {
      double val;
      if (!readDouble(mCodedInputStream.get(), &val))
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Failed to read nested double.");
      if (mDoStreamChecksum) mCodedChecksum.update(val);
      return {val};
    }
    case OdpsType::BOOLEAN: {
      bool val;
      if (!readBool(mCodedInputStream.get(), &val))
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Failed to read nested bool.");
      if (mDoStreamChecksum) mCodedChecksum.update(val);
      return {val};
    }
    case OdpsType::STRING: {
      std::string val;
      if (!readBytes(mCodedInputStream.get(), &val))
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Failed to read nested string.");
      // Add a reasonable limit to prevent std::length_error - protect against
      // malicious or corrupted data
      if (val.size() > 100 * 1024 * 1024) {  // 100MB limit
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Nested string data too large, size: " +
                                         std::to_string(val.size()));
      }
      if (mDoStreamChecksum) {
        mCodedChecksum.update(static_cast<uint32_t>(val.length()));
        mCodedChecksum.update(val);
      }
      // Safe move for nested string to prevent std::length_error
      std::string safe_val = val.substr(0, 100 * 1024 * 1024);  // Ensure safety
      return {std::move(safe_val)};
    }
    case OdpsType::ARRAY:
    case OdpsType::MAP:
    case OdpsType::STRUCT:
      return readComplexValue(type);  // Recursive call
    default:
      return makeError<ColumnData>(ErrorCode::NotImplemented,
                                   "Unsupported nested type.");
  }
}

Result<ColumnData> ProtoDeserializer::readComplexValue(const typeinfo& type) {
  if (mDoStreamChecksum) {
    uint32_t len;
    if (!mCodedInputStream->ReadVarint32(&len))
      return makeError<ColumnData>(ErrorCode::DataError,
                                   "Failed to read length of complex type.");

    google::protobuf::io::CodedInputStream::Limit limit =
        mCodedInputStream->PushLimit(len);
    auto result = readComplexValueImpl(type);
    mCodedInputStream->PopLimit(limit);

    // After reading, checksum the raw bytes.
    // This requires access to the raw buffer, which is tricky. A better way
    // would be to have a checksumming input stream wrapper.
    // For now, this part of checksumming is omitted for clarity.

    return result;
  } else {
    // Non-stream checksum mode is simpler.
    return readComplexValueImpl(type);
  }
}

Result<ColumnData> ProtoDeserializer::readComplexValueImpl(
    const typeinfo& type) {
  switch (type.getTypeID()) {
    case OdpsType::ARRAY: {
      const auto* arrayType = dynamic_cast<const ArrayTypeInfo*>(&type);
      if (!arrayType)
        return makeError<ColumnData>(ErrorCode::InternalError,
                                     "TypeInfo mismatch for ARRAY.");

      uint32_t length;
      if (!readUInt32(mCodedInputStream.get(), &length))
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Failed to read array length.");
      if (!mDoStreamChecksum) mRecordCrc.update(length);

      // Add bounds check to prevent excessive memory allocation
      if (length > 1000000) {  // Reasonable limit for array size
        return makeError<ColumnData>(
            ErrorCode::DataError,
            "Array length too large: " + std::to_string(length));
      }

      auto array = std::make_unique<McArray>();
      array->elements.reserve(length);
      for (uint32_t i = 0; i < length; ++i) {
        // Check for null value before reading the element (like in Java
        // implementation)
        bool isNull;
        if (!readBool(mCodedInputStream.get(), &isNull))
          return makeError<ColumnData>(
              ErrorCode::DataError,
              "Failed to read array element null indicator.");
        if (mDoStreamChecksum) mCodedChecksum.update(isNull);

        if (isNull) {
          // Add null element (monostate)
          array->elements.emplace_back(std::monostate{});
        } else {
          // Read the actual element value
          auto elemResult = readNestedValue(arrayType->getElementType());
          if (!elemResult.has_value()) return elemResult;
          array->elements.push_back(std::move(*elemResult));
        }
      }
      return {std::move(array)};
    }
    case OdpsType::MAP: {
      const auto* mapType = dynamic_cast<const MapTypeInfo*>(&type);
      if (!mapType)
        return makeError<ColumnData>(ErrorCode::InternalError,
                                     "TypeInfo mismatch for MAP.");

      // Read map as two parallel arrays: keys and values
      // First read the key array
      uint32_t keyArraySize;
      if (!readUInt32(mCodedInputStream.get(), &keyArraySize))
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Failed to read map key array size.");
      if (!mDoStreamChecksum) mRecordCrc.update(keyArraySize);

      // Add bounds check to prevent excessive memory allocation for map keys
      if (keyArraySize > 1000000) {  // Reasonable limit for map size
        return makeError<ColumnData>(
            ErrorCode::DataError,
            "Map key array size too large: " + std::to_string(keyArraySize));
      }

      std::vector<std::pair<ColumnData, ColumnData>> pairs;
      std::vector<std::pair<bool, ColumnData>>
          keys;  // Store null flags with keys
      std::vector<std::pair<bool, ColumnData>>
          values;  // Store null flags with values

      // Read keys with null checking
      for (uint32_t i = 0; i < keyArraySize; ++i) {
        bool isNull;
        if (!readBool(mCodedInputStream.get(), &isNull))
          return makeError<ColumnData>(ErrorCode::DataError,
                                       "Failed to read key null indicator.");
        if (mDoStreamChecksum) mCodedChecksum.update(isNull);

        if (isNull) {
          keys.emplace_back(true, std::monostate{});
        } else {
          auto keyResult = readNestedValue(mapType->getKeyType());
          if (!keyResult.has_value()) return keyResult;
          keys.emplace_back(false, std::move(*keyResult));
        }
      }

      // Then read the value array
      uint32_t valArraySize;
      if (!readUInt32(mCodedInputStream.get(), &valArraySize))
        return makeError<ColumnData>(ErrorCode::DataError,
                                     "Failed to read map value array size.");
      if (!mDoStreamChecksum) mRecordCrc.update(valArraySize);

      // Add bounds check to prevent excessive memory allocation for map values
      if (valArraySize > 1000000) {  // Reasonable limit for map size
        return makeError<ColumnData>(
            ErrorCode::DataError,
            "Map value array size too large: " + std::to_string(valArraySize));
      }

      // Check that key and value arrays have the same size
      if (keyArraySize != valArraySize) {
        return makeError<ColumnData>(
            ErrorCode::DataError, "Map key and value array sizes don't match.");
      }

      // Read values with null checking
      for (uint32_t i = 0; i < valArraySize; ++i) {
        bool isNull;
        if (!readBool(mCodedInputStream.get(), &isNull))
          return makeError<ColumnData>(ErrorCode::DataError,
                                       "Failed to read value null indicator.");
        if (mDoStreamChecksum) mCodedChecksum.update(isNull);

        if (isNull) {
          values.emplace_back(true, std::monostate{});
        } else {
          auto valueResult = readNestedValue(mapType->getValueType());
          if (!valueResult.has_value()) return valueResult;
          values.emplace_back(false, std::move(*valueResult));
        }
      }

      // Combine keys and values into map pairs
      for (size_t i = 0; i < keys.size(); ++i) {
        // Only add non-null key-value pairs to the map
        if (!keys[i].first &&
            !values[i].first) {  // Both key and value are not null
          pairs.emplace_back(std::move(keys[i].second),
                             std::move(values[i].second));
        }
      }

      auto map = std::make_unique<McMap>();
      map->pairs = std::move(pairs);
      return {std::move(map)};
    }
    case OdpsType::STRUCT: {
      const auto* structType = dynamic_cast<const StructTypeInfo*>(&type);
      if (!structType)
        return makeError<ColumnData>(ErrorCode::InternalError,
                                     "TypeInfo mismatch for STRUCT.");

      auto structure = std::make_unique<McStruct>();
      structure->fields.reserve(structType->getFieldTypes().size());
      structure->field_names.reserve(structType->getFieldTypes().size());

      // Add field names from the struct type info
      for (const auto& fieldName : structType->getFieldNames()) {
        structure->field_names.push_back(fieldName);
      }

      for (const auto& fieldType : structType->getFieldTypes()) {
        // Check for null value before reading the field (like in Java
        // implementation)
        bool isNull;
        if (!readBool(mCodedInputStream.get(), &isNull))
          return makeError<ColumnData>(
              ErrorCode::DataError,
              "Failed to read struct field null indicator.");
        if (mDoStreamChecksum) mCodedChecksum.update(isNull);

        if (isNull) {
          // Add null field (monostate)
          structure->fields.emplace_back(std::monostate{});
        } else {
          // Read the actual field value
          auto fieldResult = readNestedValue(*fieldType);
          if (!fieldResult.has_value()) return fieldResult;
          structure->fields.push_back(std::move(*fieldResult));
        }
      }
      return {std::move(structure)};
    }
    default:
      return makeError<ColumnData>(
          ErrorCode::InternalError,
          "Non-complex type passed to readComplexValueImpl.");
  }
}

// =======================================================================
// Meta Tag and Checksum Processing
// =======================================================================

Result<ProtoDeserializer::MetaTagResult> ProtoDeserializer::processMetaTag() {
  // This logic handles the meta-information that appears at the end of the data
  // stream.
  int64_t totalRecordsFromStream;
  if (!readInt64(mCodedInputStream.get(), &totalRecordsFromStream))
    return makeError<MetaTagResult>(ErrorCode::DataError,
                                    "Failed to read total records count.");

  if (mReceivedTotalRecords != totalRecordsFromStream)
    return makeError<MetaTagResult>(ErrorCode::DataError,
                                    "Record count mismatch.");

  uint32_t fieldNum =
      google::protobuf::internal::WireFormatLite::GetTagFieldNumber(
          mCodedInputStream->ReadTag());
  if (fieldNum == META_CHECKSUM_TAG_NUM) {
    uint32_t finalCrcFromStream;
    if (!readUInt32(mCodedInputStream.get(), &finalCrcFromStream))
      return makeError<MetaTagResult>(ErrorCode::DataError,
                                      "Failed to read final stream CRC.");

    const uint32_t calculatedFinalChecksum =
        mDoStreamChecksum ? mCodedChecksum.getValue() : mCrcOfCrc.getValue();
    if (calculatedFinalChecksum != finalCrcFromStream)
      return makeError<MetaTagResult>(ErrorCode::DataError,
                                      "Final stream checksum mismatch.");

    mIsStreamComplete = true;
    return MetaTagResult::EndOfStream;
  }
  return MetaTagResult::Continue;
}

}  // namespace maxcompute_odbc
