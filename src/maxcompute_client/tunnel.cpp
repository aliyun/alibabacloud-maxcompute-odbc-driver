#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/maxcompute_client/api_client.h"
#include "maxcompute_odbc/maxcompute_client/proto_deserializer.h"
#include "maxcompute_odbc/maxcompute_client/tunnel.h"
#include <google/protobuf/io/gzip_stream.h>
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <stdexcept>

// For decompression
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <sstream>
#include <vector>
#include <zlib.h>

namespace maxcompute_odbc {

static DownloadStatus StringToDownloadStatus(const std::string &status) {
  if (status == "INITIATED") return DownloadStatus::INITIATED;
  if (status == "CLOSED") return DownloadStatus::CLOSED;
  if (status == "EXPIRED") return DownloadStatus::EXPIRED;
  if (status == "INITIATING") return DownloadStatus::INITIATING;
  return DownloadStatus::UNKNOWN;
}

// =======================================================================
// DownloadSession implementation
// =======================================================================

DownloadSession::DownloadSession(const Config &conf,
                                 const std::string &instanceId)
    : mConf(conf), mInstanceId(instanceId) {
  // First, create a temporary client to get the tunnel endpoint
  auto tempClient = std::make_unique<ApiClient>(mConf);

  // Get the specific tunnel endpoint for this session
  Result<std::string> tunnelEndpointResult = GetTunnelEndpoint();
  if (!tunnelEndpointResult.has_value()) {
    throw std::runtime_error("Failed to get tunnel endpoint: " +
                             tunnelEndpointResult.error().message);
  }

  // Now, update our session's config with the correct tunnel endpoint
  mConf.endpoint = tunnelEndpointResult.value();

  // Finally, create the permanent client with the correct endpoint
  mClient = std::make_unique<ApiClient>(mConf);

  // Initiate the download session on the tunnel server
  Initiate();
}

Result<std::string> DownloadSession::GetTunnelEndpoint() {
  if (mConf.tunnelEndpoint.has_value() &&
      !mConf.tunnelEndpoint.value().empty()) {
    return mConf.tunnelEndpoint.value();
  }

  // Use a temporary client with the service endpoint to ask for the tunnel
  // endpoint
  ApiClient tempClient(mConf);

  auto path = "/projects/" + mConf.project + "/tunnel";
  httplib::Params params;
  params.emplace("service", "");
  if (mConf.tunnelQuotaName.has_value()) {
    params.emplace("quotaName", mConf.tunnelQuotaName.value());
  }

  auto responseResult = tempClient.Get(path, params, {});
  if (!responseResult.has_value()) {
    return makeError<std::string>(
        ErrorCode::NetworkError,
        "Failed to get tunnel endpoint: " + responseResult.error().message);
  }

  const std::string &serviceEndpoint = mConf.endpoint;
  std::string protocol =
      (serviceEndpoint.rfind("https://", 0) == 0) ? "https://" : "http://";
  return protocol + responseResult->body;
}

void DownloadSession::Initiate() {
  httplib::Params params;
  params.emplace("downloads", "");
  if (mConf.tunnelQuotaName.has_value()) {
    params.emplace("quotaName", mConf.tunnelQuotaName.value());
  }

  httplib::Headers headers;
  headers.emplace("x-odps-tunnel-version", "5");

  auto path = "/projects/" + mConf.project + "/instances/" + mInstanceId;

  auto response = mClient->Post(path, params, headers, "");
  if (!response.has_value()) {
    throw std::runtime_error("Failed to initiate download session: " +
                             response.error().message);
  }

  FromJson(response->body);
}

void DownloadSession::FromJson(const std::string &jsonStr) {
  try {
    nlohmann::json j = nlohmann::json::parse(jsonStr);

    mDownloadId = j.value("DownloadID", "");
    mRecordCount = j.value("RecordCount", 0);
    mStatus = StringToDownloadStatus(j.value("Status", ""));
    mQuotaName = j.value("QuotaName", "");

    if (j.contains("Schema") && j["Schema"].is_object()) {
      // 直接将 nlohmann::json 对象传递给新的 FromJson 方法
      auto schemaResult = ResultSetSchema::FromJson(j["Schema"]);

      if (schemaResult.has_value()) {
        // 假设你的 DownloadSession 中有一个 mSchema 成员
        mSchema =
            std::make_shared<ResultSetSchema>(std::move(schemaResult.value()));
      } else {
        // 如果 Schema 解析失败，可以记录一个警告或抛出异常
        // 这里我们选择抛出异常，因为一个没有 Schema 的 session 通常是无用的
        throw std::runtime_error(
            "Failed to parse schema from session response: " +
            schemaResult.error().message);
      }
    } else {
      // 如果 JSON 中根本没有 Schema 字段
      throw std::runtime_error("Session response is missing 'Schema' object.");
    }

  } catch (const nlohmann::json::parse_error &e) {
    throw std::runtime_error(
        "Failed to parse JSON response from session initiation: " +
        std::string(e.what()));
  }
}

std::unique_ptr<BufferedRecordReader> DownloadSession::OpenBufferedRecordReader(
    uint64_t batchSize) {
  return std::make_unique<BufferedRecordReader>(*this, *mClient, batchSize);
}

std::unique_ptr<ConcurrentBufferedRecordReader>
DownloadSession::OpenConcurrentBufferedRecordReader(uint64_t batchSize,
                                                    size_t numWorkers,
                                                    size_t prefetchLimit) {
  return std::make_unique<ConcurrentBufferedRecordReader>(
      *this, *mClient, batchSize, numWorkers, prefetchLimit);
}

ConcurrentBufferedRecordReader::ConcurrentBufferedRecordReader(
    DownloadSession &session, ApiClient &client, uint64_t batchSize,
    size_t numWorkers, size_t prefetchLimit)
    : mSession(session),
      mClient(client),
      mBatchSize(batchSize),
      mNumWorkers(numWorkers),
      mPrefetchLimit(std::max(
          size_t(1), prefetchLimit)),  // Prefetch limit must be at least 1.
      mTotalRecordCount(0),
      mTotalRecordsRead(0),
      mMaxPrefetchedPosition(0),
      mCurrentBatchIndex(0) {
  mTotalRecordCount = mSession.GetRecordCount();
  MCO_LOG_INFO(
      "ConcurrentBufferedRecordReader: Initializing with batch size {}, {} "
      "workers, "
      "prefetch limit {}, for {} total records",
      mBatchSize, mNumWorkers, mPrefetchLimit, mTotalRecordCount);

  // Initial scheduling of tasks
  scheduleMoreTasksIfNeeded();
  startWorkerThreads();
}

ConcurrentBufferedRecordReader::~ConcurrentBufferedRecordReader() {
  Close();  // Ensure proper cleanup via RAII.
}

Result<void> ConcurrentBufferedRecordReader::Close() {
  if (mIsClosed.exchange(true)) {
    return makeSuccess();
  }

  mShutdown = true;

  // Wake up all waiting threads so they can exit gracefully.
  mTaskCV.notify_all();
  mCacheCV.notify_all();

  stopWorkerThreads();

  // No locks needed here as all worker threads have been joined.
  mPrefetchCache.clear();
  while (!mDownloadTasks.empty()) {
    mDownloadTasks.pop();
  }

  MCO_LOG_DEBUG("ConcurrentBufferedRecordReader: Closed successfully.");
  return makeSuccess();
}

void ConcurrentBufferedRecordReader::startWorkerThreads() {
  mWorkers.reserve(mNumWorkers);
  for (size_t i = 0; i < mNumWorkers; ++i) {
    mWorkers.emplace_back(&ConcurrentBufferedRecordReader::workerThreadFunc,
                          this);
  }
}

void ConcurrentBufferedRecordReader::stopWorkerThreads() {
  for (auto &worker : mWorkers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  mWorkers.clear();
}

void ConcurrentBufferedRecordReader::scheduleMoreTasksIfNeeded() {
  std::lock_guard<std::mutex> lock(mTaskMutex);

  while (mDownloadTasks.size() < mPrefetchLimit &&
         mMaxPrefetchedPosition < mTotalRecordCount) {
    uint64_t count =
        std::min(mBatchSize, mTotalRecordCount - mMaxPrefetchedPosition);
    mDownloadTasks.push({mMaxPrefetchedPosition, count});
    mMaxPrefetchedPosition += count;
  }

  if (!mDownloadTasks.empty()) {
    // Notify workers in case they were idle.
    mTaskCV.notify_all();
  }
}

Result<std::vector<Record>> ConcurrentBufferedRecordReader::downloadDataTask(
    uint64_t start, uint64_t count) {
  auto path = "/projects/" + mSession.GetConf().project + "/instances/" +
              mSession.GetInstanceId();
  auto rowRange =
      "(" + std::to_string(start) + "," + std::to_string(count) + ")";
  auto params = httplib::Params{};
  params.emplace("downloadid", mSession.GetDownloadId());
  params.emplace("data", "");
  params.emplace("rowrange", rowRange);

  if (mSession.GetConf().tunnelQuotaName.has_value()) {
    params.emplace("quotaName", mSession.GetConf().tunnelQuotaName.value());
  }
  auto headers = httplib::Headers{};
  headers.emplace("x-odps-tunnel-version", "5");

  MCO_LOG_INFO(
      "BufferedRecordReader: Starting download from path: {}, rowRange: {}",
      path, rowRange);

  auto res = mClient.Get(path, params, headers);
  if (!res.has_value()) {
    return makeError<std::vector<Record>>(
        ErrorCode::NetworkError,
        "Failed to download data: " + res.error().message);
  }

  auto inputStream = std::make_unique<google::protobuf::io::ArrayInputStream>(
      res->body.data(), res->body.size());
  maxcompute_odbc::ProtoDeserializer deserializer(
      std::move(inputStream), mSession.GetSchema().get(), false, -1,
      mSession.GetConf().timezone);

  std::vector<Record> records;
  while (true) {
    auto result = deserializer.deserializeNext();

    if (!result.has_value()) {
      return makeError<std::vector<Record>>(
          result.error().code,
          "Failed to deserialize data: " + result.error().message);
    } else if (result == std::nullopt) {
      MCO_LOG_INFO("Concurrent downloadData succeeded, loaded {} records.",
                   records.size());
      return makeSuccess(std::move(records));  // Return Result object
    } else {
      records.push_back(std::move(result->value()));
    }
  }
}

void ConcurrentBufferedRecordReader::workerThreadFunc() {
  while (!mShutdown) {
    std::pair<uint64_t, uint64_t> task;

    // 1. Get a download task.
    {
      std::unique_lock<std::mutex> lock(mTaskMutex);
      mTaskCV.wait(lock,
                   [this] { return !mDownloadTasks.empty() || mShutdown; });

      if (mShutdown) break;

      task = mDownloadTasks.front();
      mDownloadTasks.pop();
    }

    // 2. Execute the download outside the lock.
    Result<std::vector<Record>> result;
    try {
      result = downloadDataTask(task.first, task.second);
    } catch (const std::exception &e) {
      MCO_LOG_ERROR("Unhandled exception in download task: {}", e.what());
      result = makeError<std::vector<Record>>(
          ErrorCode::UnknownError,
          "Unhandled exception in worker thread: " + std::string(e.what()));
    } catch (...) {
      MCO_LOG_ERROR("Unknown unhandled exception in download task");
      result = makeError<std::vector<Record>>(
          ErrorCode::UnknownError,
          "Unknown unhandled exception in worker thread.");
    }
    // 3. Store the result (or error) in the cache.
    {
      // **FIX**: Use the correct mutex (mCacheMutex) that protects the cache.
      std::lock_guard<std::mutex> lock(mCacheMutex);
      mPrefetchCache[task.first] = std::move(result);
    }

    // 4. Notify the consumer that new data might be available.
    mCacheCV.notify_one();
  }
}

Result<std::optional<Record>> ConcurrentBufferedRecordReader::Next() {
  if (mIsClosed) {
    return makeError<std::optional<Record>>(ErrorCode::ExecutionError,
                                            "Reader is closed.");
  }

  // Check if the current local batch is consumed. If so, fetch the next one
  // from the cache.
  if (mCurrentBatchIndex >= mCurrentBatch.size()) {
    // All records read, end of file.
    if (mTotalRecordsRead >= mTotalRecordCount) {
      return std::nullopt;
    }

    const uint64_t nextBatchStartPos = mTotalRecordsRead;

    {
      std::unique_lock<std::mutex> lock(mCacheMutex);

      // **FIX**: Wait using the correct mutex/CV pair for the correct
      // condition.
      mCacheCV.wait(lock, [this, nextBatchStartPos] {
        return mPrefetchCache.count(nextBatchStartPos) > 0 || mShutdown;
      });

      if (mShutdown && mPrefetchCache.count(nextBatchStartPos) == 0) {
        return makeError<std::optional<Record>>(
            ErrorCode::ExecutionError,
            "Reader was shut down while waiting for data.");
      }

      auto it = mPrefetchCache.find(nextBatchStartPos);
      // This should not happen if logic is correct, but as a safeguard:
      if (it == mPrefetchCache.end()) {
        return makeError<std::optional<Record>>(
            ErrorCode::InternalError,
            "Internal error: Prefetched data disappeared from cache.");
      }

      // **FIX**: Handle download errors stored in the cache.
      Result<std::vector<Record>> &batchResult = it->second;
      if (!batchResult.has_value()) {
        // The download for this batch failed. Propagate the error.
        auto error = batchResult.error();
        mPrefetchCache.erase(it);  // Clean up the failed entry.
        return makeError<std::optional<Record>>(error.code, error.message);
      }

      // Move the successfully downloaded batch to the local consumer cache.
      mCurrentBatch = std::move(batchResult.value());
      mCurrentBatchIndex = 0;
      mPrefetchCache.erase(it);
    }

    // Now that a batch has been consumed, schedule more tasks if needed.
    scheduleMoreTasksIfNeeded();
  }

  // **FIX**: This part of the logic is now correct because we handle batches.
  if (mCurrentBatchIndex >= mCurrentBatch.size()) {
    // This indicates an empty batch was successfully downloaded, which might be
    // an edge case or an error. For robustness, treat it as EOF for this batch
    // and try fetching the next one on the subsequent call. Or, more strictly,
    // return an error.
    return makeError<std::optional<Record>>(
        ErrorCode::InternalError, "An empty record batch was processed.");
  }

  // Return the next record from the local batch.
  Record record = std::move(mCurrentBatch[mCurrentBatchIndex]);
  mCurrentBatchIndex++;
  mTotalRecordsRead++;

  return std::make_optional(std::move(record));
}

// =======================================================================
// BufferedRecordReader implementation
// =======================================================================

BufferedRecordReader::BufferedRecordReader(DownloadSession &session,
                                           ApiClient &client,
                                           uint64_t batchSize)
    : mSession(session),
      mClient(client),
      mBatchSize(batchSize),
      mCurrentPosition(0),
      mTotalRecordCount(0),
      mBufferIndex(0),
      mIsClosed(false),
      mDummySchema() {
  mTotalRecordCount = mSession.GetRecordCount();
  MCO_LOG_DEBUG(
      "BufferedRecordReader: Initializing with batch size {} for {} total "
      "records",
      mBatchSize, mTotalRecordCount);
}

BufferedRecordReader::~BufferedRecordReader() { (void)Close(); }

Result<void> BufferedRecordReader::DownloadData(uint64_t start,
                                                uint64_t count) {
  auto startTime = std::chrono::steady_clock::now();

  auto path = "/projects/" + mSession.GetConf().project + "/instances/" +
              mSession.GetInstanceId();
  auto rowRange =
      "(" + std::to_string(start) + "," + std::to_string(count) + ")";
  auto params = httplib::Params{};
  params.emplace("downloadid", mSession.GetDownloadId());
  params.emplace("data", "");
  params.emplace("rowrange", rowRange);

  if (mSession.GetConf().tunnelQuotaName.has_value()) {
    params.emplace("quotaName", mSession.GetConf().tunnelQuotaName.value());
  }
  auto headers = httplib::Headers{};
  headers.emplace("x-odps-tunnel-version", "5");

  MCO_LOG_INFO(
      "BufferedRecordReader: Starting download from path: {}, rowRange: {}",
      path, rowRange);

  auto res = mClient.Get(path, params, headers);
  if (!res.has_value()) {
    return makeError<void>(ErrorCode::NetworkError,
                           "Failed to download data: " + res.error().message);
  }

  auto inputStream = std::make_unique<google::protobuf::io::ArrayInputStream>(
      res->body.data(), res->body.size());
  maxcompute_odbc::ProtoDeserializer deserializer(
      std::move(inputStream), mSession.GetSchema().get(), false, -1,
      mSession.GetConf().timezone);

  while (true) {
    auto result = deserializer.deserializeNext();

    if (!result.has_value()) {
      // Clear any partially filled buffer on error
      mBuffer.clear();
      mBufferIndex = 0;
      return makeError<void>(
          result.error().code,
          "Failed to deserialize data: " + result.error().message);
    } else if (result == std::nullopt) {
      auto endTime = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                          endTime - startTime)
                          .count();
      MCO_LOG_INFO("DownloadData succeeded in {} ms, loaded {} records.",
                   duration, mBuffer.size());
      return makeSuccess();
    } else {
      mBuffer.push_back(std::move(result->value()));
    }
  }
}

Result<void> BufferedRecordReader::fillBuffer() {
  if (mIsClosed) {
    return makeSuccess();
  }
  // Clear the current buffer
  mBuffer.clear();
  mBufferIndex = 0;
  // Calculate how many records to fetch in this batch
  uint64_t recordsToFetch =
      std::min(mBatchSize, mTotalRecordCount - mCurrentPosition);

  MCO_LOG_DEBUG(
      "BufferedRecordReader: Fetching {} records starting at position {}",
      recordsToFetch, mCurrentPosition);

  if (recordsToFetch == 0) {
    return makeSuccess();
  }

  // Open a record reader for this batch
  auto result = DownloadData(mCurrentPosition, recordsToFetch);
  if (!result.has_value()) {
    // If download failed, we should throw an exception to be caught by Next()
    return result;
  }

  // Update current position after successful download
  mCurrentPosition += mBuffer.size();

  return makeSuccess();
}

Result<std::optional<Record>> BufferedRecordReader::Next() {
  if (mIsClosed) {
    return makeError<std::optional<Record>>(ErrorCode::ExecutionError,
                                            "BufferedRecordReader is closed.");
  }

  // Check if we need to fill the buffer
  if (mBufferIndex >= mBuffer.size()) {
    auto result = fillBuffer();
    if (!result.has_value()) {
      return makeError<std::optional<Record>>(result.error().code,
                                              result.error().message);
    }
    // After filling, check if we're at EOF with no records
    if (mBuffer.empty() || mBufferIndex >= mBuffer.size()) {
      // Check if we've read all records
      if (mCurrentPosition >= mTotalRecordCount) {
        return std::optional<Record>();  // Return empty optional to indicate
                                         // EOF
      }
      // If buffer is empty but we haven't read all records, it's an error
      if (mBuffer.empty()) {
        return makeError<std::optional<Record>>(
            ErrorCode::ExecutionError, "Buffer is empty but not at EOF.");
      }
    }
  }
  // Double-check bounds before accessing
  if (mBufferIndex >= mBuffer.size()) {
    // Check if we've read all records
    if (mCurrentPosition >= mTotalRecordCount) {
      return std::optional<Record>();  // Return empty optional to indicate EOF
    }
    // This shouldn't happen, but let's handle it gracefully
    return makeError<std::optional<Record>>(ErrorCode::ExecutionError,
                                            "Buffer index out of bounds.");
  }

  // Return the next record from the buffer
  Record record = std::move(mBuffer[mBufferIndex]);
  mBufferIndex++;

  return std::optional<Record>(std::move(record));
}

Result<void> BufferedRecordReader::Close() {
  if (mIsClosed) {
    return {};
  }

  mIsClosed = true;
  mBuffer.clear();
  mBufferIndex = 0;
  mCurrentPosition = 0;

  MCO_LOG_DEBUG("BufferedRecordReader: Closed successfully.");
  return {};
}

}  // namespace maxcompute_odbc
