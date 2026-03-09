#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/maxcompute_client/api_client.h"
#include "maxcompute_odbc/maxcompute_client/client.h"
#include "maxcompute_odbc/maxcompute_client/models.h"
#include "maxcompute_odbc/maxcompute_client/proto_deserializer.h"
#include "maxcompute_odbc/maxcompute_client/setting_parser.h"
#include "maxcompute_odbc/maxcompute_client/tunnel.h"
#include "maxcompute_odbc/maxcompute_client/typeinfo.h"

// Undefine BOOL macro to avoid conflict with Arrow
#ifdef BOOL
#undef BOOL
#endif

#include <chrono>
#include <mutex>
#include <nlohmann/json.hpp>
#include <sstream>
#include <thread>

// Protobuf headers for ProtoDeserializer
#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace maxcompute_odbc {
namespace internal {
// ===================================================================
// ==  TinyMaxComputeSDKImpl (PIMPL) 的完整定义
// ===================================================================
class MaxComputeClientImpl {
 public:
  explicit MaxComputeClientImpl(const Config &config)
      : config_(config), client_(std::make_unique<ApiClient>(config)) {
    MCO_LOG_DEBUG("MaxComputeClientImpl created");

    // Fetch project information to check if schema model is enabled
    auto project_result = getProject();
    if (project_result.has_value()) {
      const Project &project = project_result.value();
      // Check if the schema model is enabled and update the config accordingly
      bool schema_model_enabled =
          project.isPropertyEnabled("odps.schema.model.enabled", true);
      config_.namespaceSchema = schema_model_enabled;

      if (config_.timezone == "unknown") {
        std::string timezone = project.getProperty("odps.sql.timezone", "UTC");
        MCO_LOG_INFO("Use project timezone: {}", timezone);
        config_.timezone = timezone;
      }
      config_.regionId = project.region;
      MCO_LOG_INFO(
          "Project schema model enabled: {}, timezone: {}, regionId: {}",
          schema_model_enabled, config_.timezone, config_.regionId);
    } else {
      MCO_LOG_WARNING("Failed to retrieve project information: {}",
                      project_result.error().message);
      // Keep the default namespaceSchema value if project fetch fails
    }
  }
  const Config &getConfig() const { return config_; }

  // --- 低级API方法，直接与ApiClient交互 ---

  Result<Instance> submitQuery(const ExecuteSQLRequest &request) {
    MCO_LOG_DEBUG("Submitting SQL: {}", request.query);
    Request req = request.toRequest();
    auto response_result = client_->execute(req);

    if (!response_result.has_value()) {
      return makeError<Instance>(
          ErrorCode::NetworkError,
          "Failed to submit SQL: " + response_result.error().message);
    }

    auto response = response_result.value();
    if (response.status / 100 != 2) {
      return makeError<Instance>(
          ErrorCode::NetworkError,
          "Failed to submit query. Status: " + std::to_string(response.status) +
              ", Body: " + response.body);
    }
    Instance instance;
    if (response.status == 200) {
      instance.status = InstanceStatus::TERMINATED;
    } else if (response.status == 201) {
      instance.status = InstanceStatus::RUNNING;
    }

    // 提取 MaxQA Query Cookie（如果存在）
    auto cookie_it = response.headers.find("x-odps-mcqa-query-cookie");
    if (cookie_it != response.headers.end()) {
      instance.maxQAInfo.queryCookie = cookie_it->second;
      instance.maxQAInfo.isMaxQA = true;
      MCO_LOG_DEBUG("Received MaxQA Query Cookie: {}",
                    instance.maxQAInfo.queryCookie);
    }

    // 从请求中获取 MaxQA Session ID（如果存在）
    auto session_id_it = req.headers.find("x-odps-mcqa-conn");
    if (session_id_it != req.headers.end()) {
      instance.maxQAInfo.sessionID = session_id_it->second;
      instance.maxQAInfo.isMaxQA = true;
      MCO_LOG_DEBUG("MaxQA Session ID: {}", instance.maxQAInfo.sessionID);
    }

    auto location_it = response.headers.find("Location");
    if (location_it != response.headers.end()) {
      std::string location = location_it->second;
      size_t last_slash = location.find_last_of('/');
      if (last_slash != std::string::npos &&
          last_slash < location.length() - 1) {
        instance.id = location.substr(last_slash + 1);
        MCO_LOG_DEBUG("SQL submitted successfully. Instance ID: {}",
                      instance.id);
      }
      return makeSuccess(instance);
    }
    return makeError<Instance>(ErrorCode::UnknownError,
                               "Cannot parse instanceId from Location header.");
  }

  Result<InstanceStatus> getQueryStatus(const std::string &instanceId,
                                        const MaxQASessionInfo *maxQAInfo) {
    std::string path =
        "/projects/" + config_.project + "/instances/" + instanceId;

    httplib::Headers headers;
    // 如果是 MaxQA 查询，添加 /mcqa 前缀和必要的请求头
    if (maxQAInfo && maxQAInfo->isMaxQA) {
      path = "/mcqa" + path;
      if (!maxQAInfo->sessionID.empty()) {
        headers.emplace("x-odps-mcqa-conn", maxQAInfo->sessionID);
      }
      if (!maxQAInfo->queryCookie.empty()) {
        headers.emplace("x-odps-mcqa-query-cookie", maxQAInfo->queryCookie);
      }
      MCO_LOG_DEBUG("Querying MaxQA instance status with session: {}",
                    maxQAInfo->sessionID);
    }

    auto response_result = client_->Get(path, {}, headers);

    if (!response_result.has_value()) {
      return makeError<InstanceStatus>(
          ErrorCode::NetworkError,
          "Failed to get instance status: " + response_result.error().message);
    }

    auto response = response_result.value();
    if (response.status != 200) {
      return makeError<InstanceStatus>(
          ErrorCode::NetworkError,
          "Failed to get status. Status: " + std::to_string(response.status) +
              ", Body: " + response.body);
    }
    // body like :<?xml version="1.0" encoding="UTF-8"?>
    // <Instance><Status>Terminated</Status></Instance>
    return Instance::parseInstanceStatus(response.body);
  }

  Result<void> getTaskStatus(const std::string &instanceId,
                             const MaxQASessionInfo *maxQAInfo) {
    std::string path =
        "/projects/" + config_.project + "/instances/" + instanceId;
    httplib::Params params;
    params.emplace("taskstatus", "");

    httplib::Headers headers;
    // 如果是 MaxQA 查询，添加 /mcqa 前缀和必要的请求头
    if (maxQAInfo && maxQAInfo->isMaxQA) {
      path = "/mcqa" + path;
      if (!maxQAInfo->sessionID.empty()) {
        headers.emplace("x-odps-mcqa-conn", maxQAInfo->sessionID);
      }
      if (!maxQAInfo->queryCookie.empty()) {
        headers.emplace("x-odps-mcqa-query-cookie", maxQAInfo->queryCookie);
      }
      MCO_LOG_DEBUG("Querying MaxQA task status with session: {}",
                    maxQAInfo->sessionID);
    }

    auto response_result = client_->Get(path, params, headers);

    if (!response_result.has_value()) {
      return makeError<void>(
          ErrorCode::NetworkError,
          "Failed to get task status: " + response_result.error().message);
    }

    auto response = response_result.value();
    if (response.status != 200) {
      return makeError<void>(ErrorCode::NetworkError,
                             "Failed to get task status. Status: " +
                                 std::to_string(response.status) +
                                 ", Body: " + response.body);
    }

    return Instance::checkTaskStatus(response.body);
  }

  Result<std::string> getRawResult(std::string &instanceId,
                                   const MaxQASessionInfo *maxQAInfo) {
    std::string path =
        "/projects/" + config_.project + "/instances/" + instanceId;
    httplib::Params params;
    params.emplace("result", "");

    httplib::Headers headers;
    // 如果是 MaxQA 查询，添加 /mcqa 前缀和必要的请求头
    if (maxQAInfo && maxQAInfo->isMaxQA) {
      path = "/mcqa" + path;
      if (!maxQAInfo->sessionID.empty()) {
        headers.emplace("x-odps-mcqa-conn", maxQAInfo->sessionID);
      }
      if (!maxQAInfo->queryCookie.empty()) {
        headers.emplace("x-odps-mcqa-query-cookie", maxQAInfo->queryCookie);
      }
      MCO_LOG_DEBUG("Fetching MaxQA raw result with session: {}",
                    maxQAInfo->sessionID);
    }

    auto response_result = client_->Get(path, params, headers);

    if (!response_result.has_value()) {
      return makeError<std::string>(
          ErrorCode::NetworkError,
          "Failed to fetch raw result: " + response_result.error().message);
    }

    auto response = response_result.value();
    if (response.status != 200) {
      return makeError<std::string>(ErrorCode::NetworkError,
                                    "Failed to get raw result. Status: " +
                                        std::to_string(response.status) +
                                        ", Body: " + response.body);
    }

    MCO_LOG_DEBUG("Raw result fetched ({} bytes)", response.body.size());
    return Instance::parseRawResult(response.body);
  }

  Result<void> waitForSuccess(Instance &instance) {
    // 获取 MaxQA 信息指针（如果存在）
    const MaxQASessionInfo *maxQAInfo =
        instance.maxQAInfo.isMaxQA ? &instance.maxQAInfo : nullptr;

    if (instance.status == InstanceStatus::TERMINATED) {
      MCO_LOG_DEBUG("Query already terminated with status: TERMINATED");
      auto task_status_result = getTaskStatus(instance.id, maxQAInfo);
      if (!task_status_result.has_value()) {
        auto raw_result = getRawResult(instance.id, maxQAInfo);
        if (raw_result.has_value()) {
          return makeError<void>(task_status_result.error().code,
                                 raw_result.value());
        }
        return makeError<void>(task_status_result.error().code,
                               task_status_result.error().message);
      }
      return makeSuccess();
    }
    // continue get status till status is terminated
    static constexpr int MAX_RETRY = 20 * 60;  // e.g., 60 * 20 * 3s = 1h
    static constexpr std::chrono::milliseconds POLL_INTERVAL(3000);

    for (int i = 0; i < MAX_RETRY; ++i) {
      auto status_result = getQueryStatus(instance.id, maxQAInfo);
      if (!status_result.has_value()) {
        return makeError<void>(status_result.error().code,
                               status_result.error().message);
      }

      InstanceStatus status = status_result.value();
      instance.status = status;
      if (status == InstanceStatus::TERMINATED) {
        MCO_LOG_DEBUG("Query terminated successfully.");
        auto task_status_result = getTaskStatus(instance.id, maxQAInfo);
        if (!task_status_result.has_value()) {
          auto raw_result = getRawResult(instance.id, maxQAInfo);
          if (raw_result.has_value()) {
            return makeError<void>(task_status_result.error().code,
                                   raw_result.value());
          }
          return makeError<void>(task_status_result.error().code,
                                 task_status_result.error().message);
        }
        return makeSuccess();
      } else if (status == InstanceStatus::FAILED ||
                 status == InstanceStatus::UNKNOWN) {
        return makeError<void>(ErrorCode::ExecutionError,
                               "Query failed or was cancelled.");
      }
      // Still running, sleep and retry
      std::this_thread::sleep_for(POLL_INTERVAL);
    }
    return makeError<void>(ErrorCode::TimeoutError,
                           "Query did not complete within 1h timeout.");
  }

  Result<std::string> getSchemaJson(const ExecuteSQLRequest &request) {
    ExecuteSQLRequest explain_request = request;
    explain_request.query = "explain output " + request.query;

    (*explain_request.options->hints)["odps.sql.select.output.format"] = "json";

    MCO_LOG_DEBUG("Fetching schema via EXPLAIN CODE: {}",
                  explain_request.query);

    auto instance_result = submitQuery(explain_request);
    if (!instance_result.has_value()) {
      return makeError<std::string>(instance_result.error().code,
                                    instance_result.error().message);
    }

    Instance instance = instance_result.value();
    auto wait_result = waitForSuccess(instance);
    if (!wait_result.has_value()) {
      return makeError<std::string>(wait_result.error().code,
                                    wait_result.error().message);
    }

    // Now fetch the result (which should be the JSON schema)
    // 注意：getSchemaJson 用于 EXPLAIN 查询，通常不需要 MaxQA 支持
    // 传递 nullptr 作为 maxQAInfo 参数
    auto raw_result = getRawResult(instance.id, nullptr);
    if (!raw_result.has_value()) {
      return makeError<std::string>(raw_result.error().code,
                                    raw_result.error().message);
    }

    std::string schema_json = raw_result.value();
    MCO_LOG_DEBUG("Schema JSON received: {}", schema_json);
    return makeSuccess(schema_json);
  }

  Result<std::vector<std::string>> listSchemas() {
    std::string path;
    std::vector<std::string> res;
    if (config_.namespaceSchema) {
      res.emplace_back(config_.schema);
    } else {
      res.emplace_back("default");
    }
    return makeSuccess(res);

    // 展示所有 schema ，会遇到当用户选中某一特定 schema 中的表时，ODBC
    // 无法从传过来的 select * from tableName; 语句中判断 schema，导致程序失败。
    // 在解决这一问题之前，要求用户必须在链接信息中提供 schema

    // std::string marker = "init";
    //
    // httplib::Params params;
    // params.emplace("expectmarker", "true");
    //
    // while (!marker.empty()) {
    //   auto response_result = client_->Get(path, params);
    //   if (!response_result.has_value()) {
    //     return makeError<std::vector<std::string>>(
    //         ErrorCode::NetworkError,
    //         "Failed to list tables: " + response_result.error().message);
    //   }
    //
    //   auto response = response_result.value();
    //   pugi::xml_document doc;
    //
    //   // 1. 加载 XML 字符串
    //   pugi::xml_parse_result parse_result = doc.load_string(
    //       response.body.c_str(), pugi::parse_default | pugi::encoding_utf8);
    //   // 2. 检查 XML 解析是否成功
    //   if (!parse_result) {
    //     MCO_LOG_ERROR(
    //         "Failed to parse XML for table list. Error: {}, at offset: {}",
    //         parse_result.description(), parse_result.offset);
    //     return makeError<std::vector<std::string>>(
    //         ErrorCode::ParseError,
    //         "Failed to parse XML: " +
    //         std::string(parse_result.description()));
    //   }
    //   // 3. 查找根节点 <Schemas>
    //   pugi::xml_node schemas_node = doc.child("Schemas");
    //   if (!schemas_node) {
    //     MCO_LOG_ERROR("XML is valid, but root node <Schemas> was not
    //     found."); return makeError<std::vector<std::string>>(
    //         ErrorCode::ParseError,
    //         "Root node <Schemas> not found in XML response.");
    //   }
    //   // 4. 遍历所有的 <Schema> 子节点
    //   for (pugi::xml_node schema : schemas_node.children("Schema")) {
    //     res.emplace_back(schema.child_value("Name"));
    //   }
    //   marker = schemas_node.child_value("Marker");
    //
    //   params.emplace("marker", marker);
    // }
    // return makeSuccess(res);
  }

  Result<std::vector<TableId>> listTables(const std::string &schema) {
    std::string path;
    path = "/projects/" + config_.project + "/tables";

    std::vector<TableId> table_ids;
    std::string marker = "init";

    httplib::Params params;
    params.emplace("expectmarker", "true");
    if (config_.namespaceSchema) {
      params.emplace("curr_schema", schema);
    }
    while (!marker.empty()) {
      auto response_result = client_->Get(path, params);
      if (!response_result.has_value()) {
        return makeError<std::vector<TableId>>(
            ErrorCode::NetworkError,
            "Failed to list tables: " + response_result.error().message);
      }

      auto response = response_result.value();
      pugi::xml_document doc;

      // 1. 加载 XML 字符串
      pugi::xml_parse_result parse_result = doc.load_string(
          response.body.c_str(), pugi::parse_default | pugi::encoding_utf8);
      // 2. 检查 XML 解析是否成功
      if (!parse_result) {
        MCO_LOG_ERROR(
            "Failed to parse XML for table list. Error: {}, at offset: {}",
            parse_result.description(), parse_result.offset);
        return makeError<std::vector<TableId>>(
            ErrorCode::ParseError,
            "Failed to parse XML: " + std::string(parse_result.description()));
      }
      // 3. 查找根节点 <Tables>
      pugi::xml_node tables_node = doc.child("Tables");
      if (!tables_node) {
        MCO_LOG_ERROR("XML is valid, but root node <Tables> was not found.");
        return makeError<std::vector<TableId>>(
            ErrorCode::ParseError,
            "Root node <Tables> not found in XML response.");
      }
      // 4. 遍历所有的 <Table> 子节点
      for (pugi::xml_node table_node : tables_node.children("Table")) {
        TableId current_table;

        // 5. 提取 <Name> 和 <Owner>
        pugi::xml_node name_node = table_node.child("Name");

        // 检查节点是否存在，然后获取其文本内容
        if (name_node) {
          current_table.project_name = config_.project;
          current_table.schema_name = schema;
          current_table.name = name_node.text().as_string();
        } else {
          MCO_LOG_WARNING(
              "Found a <Table> node without a <Name> child. Skipping.");
          continue;
        }
        // 将解析出的 TableId 添加到结果向量中
        table_ids.push_back(std::move(current_table));
      }
      marker = tables_node.child_value("Marker");

      params.emplace("marker", marker);
    }
    return makeSuccess(table_ids);
  }

  Result<Table> getTable(const std::string &schema, const std::string &table) {
    std::string path = "/projects/" + config_.project + "/tables/" + table;

    httplib::Params params;
    if (config_.namespaceSchema) {
      params.emplace("curr_schema", schema);
    }
    auto response_result = client_->Get(path, params);

    if (!response_result.has_value()) {
      return makeError<Table>(
          ErrorCode::NetworkError,
          "Failed to get table metadata: " + response_result.error().message);
    }

    auto response = response_result.value();
    return Table::FromXml(response.body);
  }

  Result<Project> getProject() {
    std::string path = "/projects/" + config_.project;
    auto response_result = client_->Get(path);

    if (!response_result.has_value()) {
      return makeError<Project>(ErrorCode::NetworkError,
                                "Failed to get project information: " +
                                    response_result.error().message);
    }

    auto response = response_result.value();
    if (response.status != 200) {
      return makeError<Project>(
          ErrorCode::NetworkError,
          "Failed to get project. Status: " + std::to_string(response.status) +
              ", Body: " + response.body);
    }

    // Parse the project information from the XML response
    return Project::FromXml(response.body);
  }

  Result<std::string> getConnection() {
    if (!config_.interactiveMode) {
      MCO_LOG_DEBUG("interactiveMode is false, skipping getConnection");
      return makeSuccess(
          std::string(""));  // Return empty string when not in interactive mode
    }

    std::string quota_name = config_.quotaName.value_or("");
    MCO_LOG_INFO("Getting MaxQA connection for project: {}, quota: {}",
                 config_.project,
                 quota_name.empty() ? "(default)" : quota_name);

    return client_->getMaxQASessionId(config_.project, quota_name);
  }

  Result<void> cancelQuery(const std::string &instanceId) {
    return makeError<void>(ErrorCode::UnknownError,
                           "Cancel functionality not implemented.");
  }

  Result<std::string> getMaxQASessionId(const std::string &project,
                                        const std::string &quotaName) {
    return client_->getMaxQASessionId(project, quotaName);
  }

 private:
  Config config_;
  std::unique_ptr<ApiClient> client_;
};
}  // namespace internal

// ===================================================================
// ==  ResultStreamImpl 的完整定义
// ===================================================================
class ResultStreamImpl : public ResultStream {
 public:
  ResultStreamImpl(internal::MaxComputeClientImpl &sdk_impl, Instance instance,
                   std::shared_ptr<const ResultSetSchema> schema)
      : m_sdk_impl(sdk_impl),
        m_instance(std::move(instance)),
        m_query_id(m_instance.id),
        m_schema(std::move(schema)) {
    MCO_LOG_DEBUG("ResultStream created for QueryID: {}", m_query_id);
  }

  const std::string &getId() const override { return m_query_id; }

  Result<const ResultSetSchema *> getSchema() override {
    return makeSuccess(m_schema.get());
  }

  Result<void> cancel() override {
    return m_sdk_impl.cancelQuery(m_instance.id);
  }

  Result<std::optional<Record>> fetchNextRow() override {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_end_of_stream) {
      return makeSuccess(std::optional<Record>(std::nullopt));
    }

    // 首次调用时，需要等待查询完成并初始化数据流
    if (!m_stream_initialized) {
      auto wait_result = waitForQueryCompletion();
      if (!wait_result.has_value()) {
        m_end_of_stream = true;
        return makeError<std::optional<Record>>(wait_result.error().code,
                                                wait_result.error().message);
      }

      auto init_result = initializeTunnel();
      if (!init_result.has_value()) {
        m_end_of_stream = true;
        return makeError<std::optional<Record>>(init_result.error().code,
                                                init_result.error().message);
      }
    }

    if (!m_bufferedReader) {
      m_end_of_stream = true;
      return makeError<std::optional<Record>>(
          ErrorCode::UnknownError,
          "Internal error: BufferedRecordReader is null after initialization.");
    }

    auto record_result = m_bufferedReader->Next();
    if (!record_result.has_value()) {
      MCO_LOG_ERROR("Failed to read next record: {}",
                    record_result.error().message);
      m_end_of_stream = true;
      return makeError<std::optional<Record>>(record_result.error().code,
                                              record_result.error().message);
    }

    auto record_opt = std::move(record_result.value());
    if (!record_opt.has_value()) {
      MCO_LOG_DEBUG("End of data stream reached for QueryID: {}", m_query_id);
      m_end_of_stream = true;
    }
    return makeSuccess(std::move(record_opt));
  }

 private:
  Result<void> waitForQueryCompletion() {
    auto startTime = std::chrono::steady_clock::now();
    MCO_LOG_DEBUG("Waiting for query completion: {}", m_query_id);
    auto result = m_sdk_impl.waitForSuccess(m_instance);
    if (!result.has_value()) {
      MCO_LOG_ERROR("Query {} failed : {}", m_query_id, result.error().message);
      return result;
    }
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        endTime - startTime)
                        .count();
    MCO_LOG_INFO("Wait for query {} completed cost {} ms.", m_query_id,
                 duration);
    return makeSuccess();
  }

  Result<void> initializeTunnel() {
    MCO_LOG_DEBUG("Initializing Tunnel download session for QueryID: {}",
                  m_query_id);
    auto startTime = std::chrono::steady_clock::now();

    try {
      // 创建下载会话
      m_downloadSession = std::make_unique<DownloadSession>(
          m_sdk_impl.getConfig(), m_instance.id);

      auto endTime = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                          endTime - startTime)
                          .count();
      MCO_LOG_INFO(
          "Initialize tunnel download session for QueryID: {}, SessionID: {}, "
          "TotalRecordCount: {}, cost: {}ms",
          m_query_id, m_downloadSession->GetDownloadId(),
          m_downloadSession->GetRecordCount(), duration);

      // 创建 BufferedRecordReader
      m_bufferedReader = m_downloadSession->OpenConcurrentBufferedRecordReader(
          m_sdk_impl.getConfig().fetchResultSplitSize,
          m_sdk_impl.getConfig().fetchResultThreadNum,
          m_sdk_impl.getConfig().fetchResultPreloadSplitNum);

      m_stream_initialized = true;
      return makeSuccess();
    } catch (const std::exception &e) {
      MCO_LOG_ERROR("Failed to initialize tunnel session:" +
                    std::string(e.what()));
      return makeError<void>(
          ErrorCode::UnknownError,
          "Failed to initialize tunnel session: " + std::string(e.what()));
    }
  }

  internal::MaxComputeClientImpl &m_sdk_impl;
  Instance m_instance;
  std::string m_query_id;
  std::shared_ptr<const ResultSetSchema> m_schema;

  std::unique_ptr<DownloadSession> m_downloadSession;
  std::unique_ptr<ConcurrentBufferedRecordReader> m_bufferedReader;

  bool m_stream_initialized = false;
  bool m_end_of_stream = false;
  std::mutex m_mutex;
};

// ===================================================================
// ==  TinyMaxComputeSDK 公共方法的最终实现
// ===================================================================

MaxComputeClient::MaxComputeClient(const Config &config)
    : config_(config),
      impl_(std::make_unique<internal::MaxComputeClientImpl>(config)) {}

// [修正] 在.cpp文件中定义PIMPL所需的特殊成员函数
MaxComputeClient::~MaxComputeClient() = default;

MaxComputeClient::MaxComputeClient(MaxComputeClient &&) noexcept = default;

MaxComputeClient &MaxComputeClient::operator=(MaxComputeClient &&) noexcept =
    default;

Result<std::string> MaxComputeClient::getConnection() {
  return impl_->getConnection();
}

Result<std::unique_ptr<ResultStream>> MaxComputeClient::executeQuery(
    const std::string &query, const std::string &globalSettings) {
  auto startTime = std::chrono::steady_clock::now();
  // 1. 解析 SQL 中的设置
  ParseResult parse_result = SettingParser::parse(query);
  if (!parse_result.errors.empty()) {
    MCO_LOG_ERROR("Error parsing settings: {}", parse_result.errors[0]);
  }
  MCO_LOG_DEBUG("Schema successfully parsed for query: {}",
                parse_result.remainingQuery);
  // 2. 构造原始查询请求
  ExecuteSQLRequest original_request;
  original_request.project = config_.project;
  original_request.query = parse_result.remainingQuery;

  original_request.options = std::make_shared<SQLTaskOptions>();
  original_request.options->hints =
      std::make_shared<std::map<std::string, std::string>>(
          parse_result.settings);
  (*original_request.options->hints)["odps.default.schema"] =
      impl_->getConfig().schema;

  if (!parse_result.settings.empty() || !config_.globalSettings.empty() ||
      !globalSettings.empty()) {
    original_request.options = std::make_shared<SQLTaskOptions>();
    original_request.options->hints =
        std::make_shared<std::map<std::string, std::string>>(
            parse_result.settings);
    // 合并 config.globalSettings 到 hints
    for (const auto &item : config_.globalSettings) {
      (*original_request.options->hints)[item.first] = item.second;
    }
    // 解析 globalSettings JSON 并合并到 hints
    if (!globalSettings.empty()) {
      try {
        auto global_settings_json = nlohmann::json::parse(globalSettings);
        if (global_settings_json.is_object()) {
          for (auto &item : global_settings_json.items()) {
            if (item.value().is_string()) {
              (*original_request.options->hints)[item.key()] =
                  item.value().get<std::string>();
            } else {
              (*original_request.options->hints)[item.key()] =
                  item.value().dump();
            }
          }
        }
      } catch (const std::exception &e) {
        MCO_LOG_WARNING("Failed to parse globalSettings JSON: {}", e.what());
      }
    }
  }

  // 2.5. 处理 MaxQA 选项
  if (config_.interactiveMode) {
    MCO_LOG_INFO("MaxQA interactive mode is enabled for this query");
    auto maxQAOptions = std::make_shared<MaxQAOptions>();
    maxQAOptions->useMaxQA = true;

    // 获取 MaxQA Session ID
    auto session_id_result = impl_->getMaxQASessionId(
        config_.project, config_.quotaName.value_or(""));
    if (!session_id_result.has_value()) {
      return makeError<std::unique_ptr<ResultStream>>(
          session_id_result.error().code,
          "Failed to get MaxQA Session ID: " +
              session_id_result.error().message);
    }
    maxQAOptions->sessionID = session_id_result.value();
    if (config_.quotaName.has_value() && !config_.quotaName->empty()) {
      maxQAOptions->quotaName = config_.quotaName.value();
    }
    MCO_LOG_DEBUG("Got MaxQA Session ID: {} for quota: {}",
                  maxQAOptions->sessionID,
                  maxQAOptions->quotaName.empty() ? "(default)"
                                                  : maxQAOptions->quotaName);

    // 将 MaxQA 选项添加到请求中
    if (!original_request.options->instanceOptions) {
      original_request.options->instanceOptions =
          std::make_shared<InstanceOptions>();
    }
    original_request.options->instanceOptions->maxQAOptions = maxQAOptions;
  } else if (config_.quotaName.has_value() && !config_.quotaName->empty()) {
    // 如果不是交互模式但配置了 quotaName，添加 quota hints
    (*original_request.options->hints)["odps.task.wlm.quota"] =
        config_.quotaName.value();
    MCO_LOG_INFO("Added quota hint: {}", config_.quotaName.value());
  }

  // 3. 先通过 EXPLAIN CODE 获取 schema（同步阻塞）
  auto schema_json_result = impl_->getSchemaJson(original_request);
  if (!schema_json_result.has_value()) {
    return makeError<std::unique_ptr<ResultStream>>(
        schema_json_result.error().code,
        "Failed to retrieve schema: " + schema_json_result.error().message);
  }
  std::string schema_json = schema_json_result.value();
  std::shared_ptr<const ResultSetSchema> schema;
  bool non_tabular_result = false;
  if (schema_json.empty() ||
      schema_json.find("nothing to explain") != std::string::npos) {
    non_tabular_result = true;
  } else {
    auto schema_parse_result = ResultSetSchema::FromJson(schema_json);
    if (!schema_parse_result.has_value()) {
      return makeError<std::unique_ptr<ResultStream>>(
          ErrorCode::ParseError, "Failed to parse schema JSON: " +
                                     schema_parse_result.error().message);
    }
    schema = std::make_shared<const ResultSetSchema>(
        std::move(schema_parse_result.value()));
  }

  auto endTime = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime)
          .count();
  MCO_LOG_INFO("Get result schema succeeded in {} ms", duration);
  startTime = std::chrono::steady_clock::now();

  // 4. 提交真实查询（异步，立即返回 QueryID）
  auto instance_result = impl_->submitQuery(original_request);
  if (!instance_result.has_value()) {
    return makeError<std::unique_ptr<ResultStream>>(
        instance_result.error().code,
        "Failed to submit query: " + instance_result.error().message);
  }
  Instance instance = instance_result.value();

  // 非结构化结果通过 ODPS Worker 返回 StaticResultStream
  if (non_tabular_result) {
    // b. 等待查询完成
    auto wait_result = impl_->waitForSuccess(instance);
    if (!wait_result.has_value()) {
      return makeError<std::unique_ptr<ResultStream>>(
          wait_result.error().code, wait_result.error().message);
    }
    // c. 获取非结构化的原始结果（传递 MaxQA 信息）
    const MaxQASessionInfo *maxQAInfo =
        instance.maxQAInfo.isMaxQA ? &instance.maxQAInfo : nullptr;
    auto raw_result = impl_->getRawResult(instance.id, maxQAInfo);
    if (!raw_result.has_value()) {
      return makeError<std::unique_ptr<ResultStream>>(
          raw_result.error().code,
          "Failed to get raw result: " + raw_result.error().message);
    }
    // e. 创建一个包含单行单列数据的 Record
    auto stream = StaticResultSetBuilder()
                      .addColumn("Result", PrimitiveTypeInfo(OdpsType::STRING))
                      .addRowValues(raw_result.value())
                      .build();
    return makeSuccess(std::move(stream));
  }
  auto logview = generateLogview(instance.id);
  // 5. 结构化结果通过 InstanceTunnel 创建 ResultStreamImpl 实现
  auto stream =
      std::make_unique<ResultStreamImpl>(*impl_, std::move(instance), schema);
  endTime = std::chrono::steady_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime)
          .count();
  MCO_LOG_INFO("Submit query succeeded in {} ms, logview: {}", duration,
               logview);
  return makeSuccess(std::move(stream));
}

Result<Table> MaxComputeClient::getTable(const std::string &schema,
                                         const std::string &table) const {
  return impl_->getTable(schema, table);
}

std::string MaxComputeClient::generateLogview(std::string &instanceId) const {
  // use impl_->getConfig but not config_ because regionId is set inside impl_
  return config_.logviewHost + "/" + impl_->getConfig().regionId +
         "/job-insights?h=" + config_.endpoint + "&p=" + config_.project +
         "&i=" + instanceId;
}

Result<std::vector<TableId>> MaxComputeClient::listTables(
    const std::string &schema) const {
  return impl_->listTables(schema);
}

Result<std::vector<std::string>> MaxComputeClient::listSchemas() const {
  return impl_->listSchemas();
}
}  // namespace maxcompute_odbc
