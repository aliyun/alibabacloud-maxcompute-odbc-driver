#pragma once
#ifdef CPPHTTPLIB_BROTLI_SUPPORT
#undef CPPHTTPLIB_BROTLI_SUPPORT
#endif
#include "maxcompute_odbc/common/errors.h"
#include "maxcompute_odbc/common/logging.h"
#include <functional>
#include <httplib.h>
#include <map>
#include <memory>
#include <nlohmann/adl_serializer.hpp>
#include <pugixml.hpp>
#include <string>
#include <variant>
#include "typeinfo.h"

namespace maxcompute_odbc {

// 这个结构体专门用来存储从后端JSON解析出来的、ODBC驱动需要的所有元数据。
struct Column {
  std::string name;  // 列名 (用于 SQLDescribeCol 的 ColumnName)
  std::unique_ptr<typeinfo> type_info;

  static Column buildColumn(const std::string &name,
                            const typeinfo &type_info) {
    return buildColumn(name, type_info.clone());
  }

  static Column buildColumn(const std::string &name,
                            std::unique_ptr<typeinfo> type_info);
};

// 这个类封装了一个完整查询结果的所有列的元数据。
class ResultSetSchema {
 public:
  // Default constructor
  ResultSetSchema() = default;

  explicit ResultSetSchema(std::vector<Column> columns)
      : m_columns(std::move(columns)){};

  // Destructor
  ~ResultSetSchema() = default;

  // Delete copy constructor and copy assignment operator because of unique_ptr
  // members
  ResultSetSchema(const ResultSetSchema &) = delete;
  ResultSetSchema &operator=(const ResultSetSchema &) = delete;

  // Move constructor
  ResultSetSchema(ResultSetSchema &&) = default;

  // Move assignment operator
  ResultSetSchema &operator=(ResultSetSchema &&) = default;

  // 从后端返回的JSON schema直接构造
  static Result<ResultSetSchema> FromJson(const nlohmann::json &json);
  static Result<ResultSetSchema> FromJson(const std::string &json_string);

  // 将ResultSetSchema转换为JSON字符串
  static Result<std::string> ToJson(const ResultSetSchema &schema);

  size_t getColumnCount() const { return m_columns.size(); }
  const Column &getColumn(size_t index) const { return m_columns[index]; }
  const std::vector<Column> &getAllColumns() const { return m_columns; }

  const typeinfo &getColumnType(size_t index) const {
    return *m_columns[index].type_info;
  }
  // Helper method to get all type infos as references (since we can't transfer
  // ownership)
  std::vector<std::reference_wrapper<const typeinfo>> getAllColumnTypes()
      const {
    std::vector<std::reference_wrapper<const typeinfo>> typeInfos;
    typeInfos.reserve(m_columns.size());
    for (const auto &column : m_columns) {
      if (column.type_info) {
        typeInfos.emplace_back(*column.type_info);
      }
    }
    return typeInfos;
  }

 private:
  std::vector<Column> m_columns;
};

struct McArray;
struct McMap;
struct McStruct;

struct McDate {
  int32_t epochDays;
};

// 用于表示 TIMESTAMP (带时区信息，通常是 UTC)
struct McTimestamp {
  int64_t epochSeconds = 0;
  int32_t nanoseconds = 0;

  std::string timezone;
};

// 表示单个单元格的数据。使用std::monostate表示NULL。
using ColumnData =
    std::variant<std::monostate,  // for NULL
                 int, signed short int, bool, int64_t, double, std::string,
                 McDate,                    // 时间类型
                 McTimestamp,               // 时间戳类型
                 std::unique_ptr<McArray>,  // 复杂类型
                 std::unique_ptr<McMap>, std::unique_ptr<McStruct>>;

struct McArray {
  std::vector<ColumnData> elements;
};

struct McMap {
  std::vector<std::pair<ColumnData, ColumnData>> pairs;
};

struct McStruct {
  std::vector<ColumnData> fields;
  std::vector<std::string> field_names;
};

// 表示一行数据
struct Record {
  std::vector<ColumnData> values;

  // 默认构造
  Record() = default;

  // 预分配大小的构造函数 (必须是 explicit)
  explicit Record(size_t size) : values(size) {}
  explicit Record(std::vector<ColumnData> &&vals) : values(std::move(vals)) {}

  // --- Rule of Five: 明确 move-only 语义 ---
  Record(const Record &) = delete;
  Record &operator=(const Record &) = delete;
  Record(Record &&) noexcept = default;
  Record &operator=(Record &&) noexcept = default;

  template <typename... Args>
  static std::vector<ColumnData> createValues(Args &&...args) {
    std::vector<ColumnData> vals;
    vals.reserve(sizeof...(args));
    (vals.push_back(ColumnData(std::forward<Args>(args))), ...);
    return vals;
  }
};

struct Request {
  std::string method;
  std::string path;
  httplib::Headers headers;
  httplib::Params params;  // Used for URL query parameters
  std::string body;
  std::string contentType;

  std::optional<httplib::ContentReceiver> receiver;
};

class ResultStream {
 public:
  virtual const std::string &getId() const = 0;
  // [ODBC Mapping: SQLDescribeCol, SQLNumResultCols]
  // 获取已解析和缓存的ODBC原生Schema。
  // 这个方法将在SQLExecDirect的实现中被调用，必须是纯内存操作。
  virtual Result<const ResultSetSchema *> getSchema() = 0;

  // [ODBC Mapping: SQLFetch]
  // 获取下一行数据。如果返回的Result包含std::nullopt，则流结束。
  virtual Result<std::optional<Record>> fetchNextRow() = 0;

  // [ODBC Mapping: SQLCancel]
  // [Perf: 应该快速响应]
  // 尝试取消正在进行的查询。
  virtual Result<void> cancel() = 0;

  virtual ~ResultStream() = default;
};

/**
 * @class StaticResultStream
 * @brief 一个 ResultStream 的实现，用于处理预先加载到内存中的静态数据。
 *
 * 这个类非常适合用于 ODBC 的目录函数 (SQLColumns, SQLTables) 或返回少量、
 * 非结构化结果的 DML/DDL 语句。它的所有操作都是在内存中完成的，因此非常快速。
 */
class StaticResultStream : public ResultStream {
 public:
  // 构造函数接收一个 schema 和一个包含所有行的 vector（通过右值引用来移动）
  StaticResultStream(std::shared_ptr<const ResultSetSchema> schema,
                     std::vector<Record> &&rows)
      : m_schema(std::move(schema)),
        m_rows(std::move(rows)),
        m_current_row_index(0) {
    m_id = "static-result-" + std::to_string(reinterpret_cast<uintptr_t>(this));
    MCO_LOG_DEBUG("StaticResultStream created with ID: {}, holding {} rows.",
                  m_id, m_rows.size());
  }

  // --- 实现 ResultStream 虚拟接口 ---

  const std::string &getId() const override { return m_id; }

  Result<const ResultSetSchema *> getSchema() override {
    return makeSuccess(m_schema.get());
  }

  Result<void> cancel() override {
    // 对于内存中的结果集，没有需要取消的操作。
    MCO_LOG_DEBUG(
        "Cancel called on StaticResultStream (ID: {}), no action needed.",
        m_id);
    return makeSuccess();
  }

  Result<std::optional<Record>> fetchNextRow() override {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_current_row_index >= m_rows.size()) {
      // 没有更多数据了，返回 nullopt 表示流结束
      return makeSuccess(std::optional<Record>(std::nullopt));
    }

    // 返回当前行，并将索引向后移动一位 (通过移动来传递所有权)
    return makeSuccess(
        std::optional<Record>(std::move(m_rows[m_current_row_index++])));
  }

 private:
  std::string m_id;
  std::shared_ptr<const ResultSetSchema> m_schema;
  std::vector<Record> m_rows;
  size_t m_current_row_index;
  std::mutex m_mutex;
};

/**
 * @class StaticResultSetBuilder
 * @brief 使用建造者模式 (Builder Pattern) 来流畅地创建 StaticResultStream。
 *
 * 这个类极大地简化了为目录函数或简单查询结果构建内存结果集的过程。
 */
class StaticResultSetBuilder {
 public:
  StaticResultSetBuilder() = default;

  // --- Column API ---

  // 添加一列，通过 TypeInfo 对象
  StaticResultSetBuilder &addColumn(const std::string &name,
                                    const typeinfo &type_info) {
    // 假设 Column::buildColumn 存在且已正确实现
    m_columns.push_back(Column::buildColumn(name, type_info));
    return *this;
  }

  // 添加一列，通过 unique_ptr<TypeInfo>
  StaticResultSetBuilder &addColumn(const std::string &name,
                                    std::unique_ptr<typeinfo> type_info) {
    if (type_info) {
      addColumn(name, *type_info);
    }
    return *this;
  }

  // 添加一行，通过可变参数模板提供值
  template <typename... Args>
  StaticResultSetBuilder &addRowValues(Args &&...args) {
    if (sizeof...(args) != m_columns.size()) {
      throw std::logic_error("Row data count does not match column count.");
    }
    auto values = Record::createValues(std::forward<Args>(args)...);
    m_rows.emplace_back(std::move(values));
    return *this;
  }

  // 构建最终的 StaticResultStream 实例
  std::unique_ptr<StaticResultStream> build() {
    auto schema = std::make_shared<ResultSetSchema>(std::move(m_columns));
    return std::make_unique<StaticResultStream>(schema, std::move(m_rows));
  }

 private:
  std::vector<Column> m_columns;
  std::vector<Record> m_rows;
};

// --- Enums and Base Models (mostly unchanged) ---
enum class InstanceStatus { RUNNING, SUCCESS, TERMINATED, FAILED, UNKNOWN };

/**
 * @brief MaxQA 配置选项
 *
 * 用于配置 MaxQA 查询的参数，包括是否启用 MaxQA、Session ID 和 Quota Name
 */
struct MaxQAOptions {
  bool useMaxQA = false;  // 是否启用 MaxQA
  std::string sessionID;  // MaxQA Session ID（可选）
  std::string quotaName;  // MaxQA Quota Name（可选，与 sessionID 二选一）
  std::string queryCookie;  // MaxQA Query Cookie（由后端返回）
};

/**
 * @brief MaxQA 会话信息
 *
 * 存储 MaxQA 查询的会话状态信息
 */
struct MaxQASessionInfo {
  bool isMaxQA = false;     // 是否是 MaxQA 查询
  std::string sessionID;    // MaxQA Session ID
  std::string queryCookie;  // MaxQA Query Cookie
};

struct InstanceOptions {
  std::shared_ptr<int> priority;
  std::shared_ptr<bool> tryWait;
  std::shared_ptr<std::string> uniqueIdentifyID;
  std::shared_ptr<MaxQAOptions> maxQAOptions;  // MaxQA 选项
};

struct SQLTaskOptions {
  std::shared_ptr<InstanceOptions> instanceOptions;
  std::shared_ptr<std::string> taskName;
  std::shared_ptr<std::map<std::string, std::string>> hints;
};

struct ExecuteSQLRequest {
  std::string project;
  std::string query;
  std::shared_ptr<SQLTaskOptions> options;

  // XML serialization
  std::string toXmlString() const;

  Request toRequest() const;

  ExecuteSQLRequest() { options = std::make_shared<SQLTaskOptions>(); }
};

struct Instance {
  std::string id;
  InstanceStatus status = InstanceStatus::UNKNOWN;
  MaxQASessionInfo maxQAInfo;  // MaxQA 会话信息

  static Result<InstanceStatus> parseInstanceStatus(const std::string &);
  static Result<std::string> parseRawResult(const std::string &);
  static Result<void> checkTaskStatus(const std::string &);
};

struct TableId {
  std::string name;
  std::string project_name;
  std::string schema_name;

  static Result<std::vector<TableId>> FromXml(const std::string &project_name,
                                              const std::string &schema_name,
                                              const std::string &xml_string);
};

// Table Definition with complete schema information
struct Table {
  std::string name;
  std::string description;
  std::vector<Column> columns;            // Regular columns
  std::vector<Column> partition_columns;  // Partition columns
  std::string project_name;
  std::string schema_name;

  // Parse table from XML response
  static Result<Table> FromXml(const std::string &xml_string);
  static Result<Table> FromJsonSchema(const nlohmann::json &json_schema);

  // Get all columns (regular + partition)
  template <typename Func>
  void forEachColumn(Func &&func) const {
    for (const auto &col : columns) {
      func(col);
    }
    for (const auto &col : partition_columns) {
      func(col);
    }
  }

  // 1. 默认构造函数
  Table() = default;

  // 2. 删除拷贝操作
  Table(const Table &) = delete;
  Table &operator=(const Table &) = delete;

  // 3. 显式声明默认的移动操作
  Table(Table &&) noexcept = default;
  Table &operator=(Table &&) noexcept = default;
};

// Project Definition with complete project information including properties
struct Project {
  std::string name;
  std::string type;
  std::string tenant_id;
  std::string region;
  std::map<std::string, std::string>
      properties;  // Properties map extracted from nested Property elements

  // Check if a specific property is enabled
  bool isPropertyEnabled(const std::string &property_name,
                         bool default_value = false) const {
    auto it = properties.find(property_name);
    if (it != properties.end()) {
      std::string value = it->second;
      // Convert to lowercase for case-insensitive comparison
      std::transform(value.begin(), value.end(), value.begin(), ::tolower);
      return (value == "true" || value == "1" || value == "yes" ||
              value == "on");
    }
    return default_value;
  }

  std::string getProperty(const std::string &property_name,
                          std::string default_value) const {
    auto it = properties.find(property_name);
    if (it != properties.end()) {
      return it->second;
      ;
    }
    return default_value;
  }

  // Parse project from XML response
  static Result<Project> FromXml(const std::string &xml_string);

  // 1. Default constructor
  Project() = default;

  // 2. Delete copy operations
  Project(const Project &) = delete;
  Project &operator=(const Project &) = delete;

  // 3. 显式 declare default move operations
  Project(Project &&) noexcept = default;
  Project &operator=(Project &&) noexcept = default;
};
}  // namespace maxcompute_odbc
