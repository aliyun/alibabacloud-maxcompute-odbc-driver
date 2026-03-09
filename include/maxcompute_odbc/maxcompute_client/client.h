#pragma once

#include <memory>
#include <string>

#ifdef BOOL
#undef BOOL
#endif

#include "maxcompute_odbc/common/errors.h"
#include "maxcompute_odbc/config/config.h"
#include "maxcompute_odbc/maxcompute_client/models.h"

namespace maxcompute_odbc {
namespace internal {
class MaxComputeClientImpl;
}
class MaxComputeClient {
 public:
  explicit MaxComputeClient(const Config &config);

  // [修正] 为PIMPL模式正确声明析构和移动操作
  ~MaxComputeClient();
  MaxComputeClient(MaxComputeClient &&) noexcept;
  MaxComputeClient &operator=(MaxComputeClient &&) noexcept;

  // 禁止拷贝
  MaxComputeClient(const MaxComputeClient &) = delete;

  MaxComputeClient &operator=(const MaxComputeClient &) = delete;

  // [ODBC Mapping: SQLExecDirect]
  // [Perf: 必须快速返回，不能阻塞等待整个查询完成]
  // 执行一个SQL查询，并立即返回一个结果流对象。
  // **此方法内部的职责**:
  // 1. 异步提交查询到后端，获得QueryID。
  // 2. 阻塞式轮询后端的schema接口，直到获得JSON schema。
  // 3. 调用 ResultSetSchema::FromJson 将JSON解析为内部的schema对象。
  // 4. 创建一个 ResultStream
  // 的实现，将QueryID和解析好的ResultSetSchema对象传入。
  // 5. 快速返回这个ResultStream。
  Result<std::unique_ptr<ResultStream>> executeQuery(
      const std::string &sql, const std::string &globalSettings = "");

  // [ODBC Mapping: SQLColumns and other metadata functions]
  // Retrieves detailed information about a specific table including its schema
  Result<Table> getTable(const std::string &schema,
                         const std::string &table) const;

  std::string generateLogview(std::string &instanceId) const;

  Result<std::vector<TableId>> listTables(const std::string &schema) const;
  Result<std::vector<std::string>> listSchemas() const;

  // Get project information for the current connection
  Result<Project> getProject() const;

  // Get MaxQA connection (session) ID
  // When interactiveMode is true, this will call the getConnection API
  Result<std::string> getConnection();

 private:
  Config config_;
  std::unique_ptr<internal::MaxComputeClientImpl> impl_;
};
}  // namespace maxcompute_odbc
