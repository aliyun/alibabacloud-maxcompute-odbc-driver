#pragma once

#include "maxcompute_odbc/common/errors.h"
#include "maxcompute_odbc/config/config.h"
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include "api_client.h"
#include "models.h"

namespace maxcompute_odbc {

// Forward declarations
class RecordReader;
class BufferedRecordReader;
class ConcurrentBufferedRecordReader;

// Status enum for download session
enum class DownloadStatus {
  UNKNOWN = 0,
  INITIATED = 1,
  CLOSED = 2,
  EXPIRED = 3,
  INITIATING = 4
};

// DownloadSession class
// Manages the lifecycle of a download from an instance.
class DownloadSession {
 public:
  /**
   * @brief Constructs and initiates a download session.
   * @param conf The configuration, which will be copied.
   * @param instanceId The ID of the instance to download from.
   * @throws std::runtime_error if initiation fails.
   */
  DownloadSession(const Config &conf, const std::string &instanceId);

  virtual ~DownloadSession() = default;

  // Disable copy and move semantics
  DownloadSession(const DownloadSession &) = delete;
  DownloadSession &operator=(const DownloadSession &) = delete;
  DownloadSession(DownloadSession &&) = delete;
  DownloadSession &operator=(DownloadSession &&) = delete;

  // Getters
  const std::string &GetDownloadId() const { return mDownloadId; }
  const std::string &GetInstanceId() const { return mInstanceId; }
  const Config &GetConf() const { return mConf; }
  const std::string &GetQuotaName() const { return mQuotaName; }
  std::shared_ptr<ResultSetSchema> GetSchema() const { return mSchema; }
  uint64_t GetRecordCount() const { return mRecordCount; }
  DownloadStatus GetDownloadStatus() const { return mStatus; }

  /**
   * @brief Opens a buffered reader for records with a specified batch size.
   * @note The returned BufferedRecordReader is dependent on this
   * DownloadSession. The DownloadSession instance must outlive any
   * BufferedRecordReader it creates.
   * @param batchSize The number of records to fetch in each batch (default:
   * 1000).
   * @return A unique_ptr to a BufferedRecordReader.
   */
  std::unique_ptr<BufferedRecordReader> OpenBufferedRecordReader(
      uint64_t batchSize = 1000);

  /**
   * @brief Opens a concurrent buffered reader for records with specified
   * parameters.
   * @note The returned ConcurrentBufferedRecordReader is dependent on this
   * DownloadSession. The DownloadSession instance must outlive any
   * ConcurrentBufferedRecordReader it creates.
   * @param batchSize The number of records to fetch in each batch (default:
   * 10000).
   * @param numWorkers The number of worker threads to use (default: 4).
   * @param prefetchLimit The maximum number of pending prefetch batches
   * (default: 8).
   * @return A unique_ptr to a ConcurrentBufferedRecordReader.
   */
  std::unique_ptr<ConcurrentBufferedRecordReader>
  OpenConcurrentBufferedRecordReader(uint64_t batchSize = 10000,
                                     size_t numWorkers = 4,
                                     size_t prefetchLimit = 8);

 private:
  void Initiate();
  Result<std::string> GetTunnelEndpoint();
  void FromJson(const std::string &json);

  Config mConf;
  std::string mInstanceId;
  std::string mDownloadId;
  uint64_t mRecordCount = 0;
  DownloadStatus mStatus = DownloadStatus::UNKNOWN;

  std::shared_ptr<ResultSetSchema> mSchema;
  std::string mQuotaName;

  std::unique_ptr<ApiClient> mClient;
};

class ConcurrentBufferedRecordReader {
 public:
  ConcurrentBufferedRecordReader(DownloadSession &session, ApiClient &client,
                                 uint64_t batchSize, size_t numWorkers,
                                 size_t prefetchLimit);
  ~ConcurrentBufferedRecordReader();

  Result<void> Close();
  Result<std::optional<Record>> Next();

 private:
  void startWorkerThreads();
  void stopWorkerThreads();
  void workerThreadFunc();
  Result<std::vector<Record>> downloadDataTask(uint64_t start, uint64_t count);
  void scheduleMoreTasksIfNeeded();

  DownloadSession &mSession;
  ApiClient &mClient;
  const uint64_t mBatchSize;
  const size_t mNumWorkers;
  const size_t mPrefetchLimit;

  // --- State and Synchronization Primitives ---

  // Total records to be read, set on initialization.
  uint64_t mTotalRecordCount;
  // Total records successfully returned by Next(). Used to determine the next
  // batch to fetch.
  uint64_t mTotalRecordsRead;

  // Guards access to mPrefetchCache and is paired with mCacheCV.
  std::mutex mCacheMutex;
  // Notifies the consumer (Next()) that a new batch is available in the cache.
  std::condition_variable mCacheCV;
  // Caches downloaded batches (or download errors). Key is the start position
  // of the batch.
  std::map<uint64_t, Result<std::vector<Record>>> mPrefetchCache;

  // Guards access to mDownloadTasks and mMaxPrefetchedPosition. Paired with
  // mTaskCV.
  std::mutex mTaskMutex;
  // Notifies worker threads that new download tasks are available.
  std::condition_variable mTaskCV;
  // Queue of download tasks {start_position, count}.
  std::queue<std::pair<uint64_t, uint64_t>> mDownloadTasks;
  // The start position of the next batch that will be scheduled for download.
  uint64_t mMaxPrefetchedPosition;

  // --- Local Batch Cache for Consumer ---
  // The batch currently being consumed by the Next() method.
  std::vector<Record> mCurrentBatch;
  // The index of the next record to be read from mCurrentBatch.
  size_t mCurrentBatchIndex;

  // --- Thread and Shutdown Management ---
  std::vector<std::thread> mWorkers;
  std::atomic<bool> mShutdown{false};
  std::atomic<bool> mIsClosed{false};
};

// BufferedRecordReader class
// Reads records in batches, closing connections between batches to avoid
// long-lived connections and reduce memory usage by not loading all data into
// memory at once.
class BufferedRecordReader {
 public:
  /**
   * @brief Constructs a BufferedRecordReader.
   *
   * @param session The DownloadSession to use for downloads.
   * @param batchSize The number of records to fetch in each batch.
   */
  BufferedRecordReader(DownloadSession &session, ApiClient &client,
                       uint64_t batchSize = 10000);

  ~BufferedRecordReader();

  // Disable copy and move semantics
  BufferedRecordReader(const BufferedRecordReader &) = delete;
  BufferedRecordReader &operator=(const BufferedRecordReader &) = delete;
  BufferedRecordReader(BufferedRecordReader &&) = delete;
  BufferedRecordReader &operator=(BufferedRecordReader &&) = delete;

  /**
   * @brief Reads the next record from the buffer.
   * @return A Result containing an optional Record.
   *         - Success, optional has value: A record was read.
   *         - Success, optional is empty: End of stream reached.
   *         - Failure: An error occurred (e.g., network, deserialization).
   */
  Result<std::optional<Record>> Next();

  /**
   * @brief Closes the reader and releases resources.
   * It is safe to call this method multiple times.
   * The destructor calls this automatically.
   * @return A success Result, or an error Result if closing failed.
   */
  Result<void> Close();

  const ResultSetSchema &GetSchema() const {
    auto schema = mSession.GetSchema();
    if (schema) {
      return *schema;
    } else {
      // Return a reference to the dummy schema if the session doesn't have one
      return mDummySchema;
    }
  }

 private:
  Result<void> fillBuffer();
  Result<void> DownloadData(uint64_t start, uint64_t count);

  DownloadSession &mSession;
  ApiClient &mClient;

  uint64_t mBatchSize;
  uint64_t mCurrentPosition;
  uint64_t mTotalRecordCount;

  std::vector<Record> mBuffer;
  size_t mBufferIndex;

  bool mIsClosed = false;

  // Dummy schema for fallback
  ResultSetSchema mDummySchema;
};
}  // namespace maxcompute_odbc
