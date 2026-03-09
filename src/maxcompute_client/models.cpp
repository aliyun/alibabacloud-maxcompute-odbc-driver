#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/maxcompute_client/client.h"
#include "maxcompute_odbc/maxcompute_client/models.h"
#include "maxcompute_odbc/maxcompute_client/typeinfo.h"
#include <sstream>
#include "cpp-base64/base64.h"
#include "nlohmann/json.hpp"

namespace {
// 放在匿名命名空间中，使其成为文件内部的辅助函数

/**
 * @brief 如果 shared_ptr 有值，或者提供了有效的默认值，则向 XML
 * 节点添加一个子元素。
 *
 * @tparam T 值的类型 (e.g., std::string, int, bool)。
 * @param parent  父 pugi::xml_node。
 * @param name    要创建的子元素的标签名。
 * @param ptr     指向要设置的值的 shared_ptr。
 * @param default_value (可选) 一个 std::optional 包裹的默认值。如果 ptr 为空且
 * default_value 有值，则使用此默认值。
 */
template <typename T>
void append_xml_child_if_set(
    pugi::xml_node &parent, const char *name, const std::shared_ptr<T> &ptr,
    const std::optional<T> &default_value = std::nullopt  // C++17
) {
  const T *value_to_set = nullptr;

  if (ptr) {
    // 如果 ptr 有值，优先使用 ptr 的值
    value_to_set = ptr.get();
  } else if (default_value.has_value()) {
    // 如果 ptr 为空，但提供了默认值，则使用默认值
    value_to_set = &default_value.value();
  }

  // 如果最终有值需要设置
  if (value_to_set) {
    auto node = parent.append_child(name);

    // 使用 if constexpr 在编译时处理不同类型
    if constexpr (std::is_same_v<T, std::string>) {
      node.text().set(value_to_set->c_str());
    } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
      node.text().set(*value_to_set);
    } else if constexpr (std::is_same_v<T, bool>) {
      node.text().set(*value_to_set ? "true" : "false");
    }
  }
  // 如果 ptr 和 default_value 都为空，则什么也不做
}

void append_xml_child_if_set(pugi::xml_node &parent, const char *name,
                             const std::string &value) {
  parent.append_child(name).text().set(value.c_str());
}

/**
 * @brief 将 std::map<string, string> 转换为 JSON 字符串。
 * @param map_ptr 指向 map 的 shared_ptr。
 * @return 如果 map_ptr 有效且 map 不为空，则返回 JSON
 * 字符串；否则返回空字符串。
 */
std::string mapToJson(
    const std::shared_ptr<std::map<std::string, std::string>> &map_ptr) {
  if (!map_ptr || map_ptr->empty()) {
    return "";
  }

  nlohmann::json j = *map_ptr;
  return j.dump();  // .dump() 会生成紧凑的 JSON 字符串
}
}  // namespace

namespace maxcompute_odbc {
Request ExecuteSQLRequest::toRequest() const {
  Request req;

  // 1. 设置固定的请求参数
  req.method = "POST";
  req.contentType = "application/xml";

  // 2. 生成请求体 (Body)
  // 我们信任 toXmlString() 已经处理了其内部的所有边界条件
  req.body = this->toXmlString();

  auto safe_instance_options = options->instanceOptions
                                   ? options->instanceOptions
                                   : std::make_shared<InstanceOptions>();

  // 3.1. 构建 Path
  req.path = "/projects/" + project + "/instances";

  // 3.2. 处理 MaxQA 选项
  if (safe_instance_options->maxQAOptions &&
      safe_instance_options->maxQAOptions->useMaxQA) {
    const auto &maxQAOptions = *safe_instance_options->maxQAOptions;

    // 如果启用了 MaxQA，路径前缀需要添加 /mcqa
    req.path = "/mcqa" + req.path;

    // 添加 MaxQA 请求头
    if (!maxQAOptions.sessionID.empty()) {
      req.headers.emplace("x-odps-mcqa-conn", maxQAOptions.sessionID);
    }

    if (!maxQAOptions.queryCookie.empty()) {
      req.headers.emplace("x-odps-mcqa-query-cookie", maxQAOptions.queryCookie);
    }
  }

  // 3.3. 处理 Params (tryWait)
  // 检查 shared_ptr 是否存在，并且其包裹的布尔值是否为 true
  if (safe_instance_options->tryWait && *(safe_instance_options->tryWait)) {
    // 当 tryWait 为 true 时，添加 ?tryWait=true 到 URL
    req.params.emplace("tryWait", "true");
  }

  return req;
}

std::string ExecuteSQLRequest::toXmlString() const {
  pugi::xml_document doc;

  // --- 防御性编程：处理 nullptr ---
  auto safe_instance_options = options->instanceOptions
                                   ? options->instanceOptions
                                   : std::make_shared<InstanceOptions>();

  // ------------------------------------

  // 1. 创建根节点 <Instance>
  pugi::xml_node instance_node = doc.append_child("Instance");

  // 2. 创建 <Job> 节点
  pugi::xml_node job_node = instance_node.append_child("Job");

  // 3. 处理可选的 Job 级别选项 (现在使用 safe_instance_options，非常安全)
  append_xml_child_if_set<int>(job_node, "Priority",
                               safe_instance_options->priority,
                               std::optional<int>(9));  // 默认值 9
  append_xml_child_if_set(job_node, "Guid",
                          safe_instance_options->uniqueIdentifyID);

  // 4. 创建 <Tasks> 容器
  pugi::xml_node tasks_node = job_node.append_child("Tasks");

  // 5. 创建 <SQL> 节点 (一个 Job 至少有一个 Task)
  pugi::xml_node sql_node = tasks_node.append_child("SQL");

  // 5.1 设置 SQL Task 的名字 (现在使用 safe_options，非常安全)
  append_xml_child_if_set<std::string>(
      sql_node, "Name", options->taskName,
      std::optional<std::string>("AnonymousSQLTask"));

  // 5.2 处理 Config/Property (hints)
  std::string hints_json = mapToJson(options->hints);
  if (!hints_json.empty()) {
    pugi::xml_node config_node = sql_node.append_child("Config");
    pugi::xml_node property_node = config_node.append_child("Property");
    // 注意：这里的 Name 是固定的 "settings"，不是一个变量
    property_node.append_child("Name").text().set("settings");
    property_node.append_child("Value").text().set(hints_json.c_str());
  }

  // 5.3 设置 Query
  // Always include the Query element, even if empty
  sql_node.append_child("Query").text().set(query.c_str());

  // 6. 将 XML 文档保存到字符串流中
  std::stringstream ss;
  doc.save(ss, "  ", pugi::format_default, pugi::encoding_utf8);

  return ss.str();
}

InstanceStatus stringToInstanceStatus(std::string_view status_str) {
  if (status_str == "Running") return InstanceStatus::RUNNING;
  if (status_str == "Success")
    return InstanceStatus::SUCCESS;  // 假设后端可能返回Success
  if (status_str == "Terminated") return InstanceStatus::TERMINATED;
  if (status_str == "Failed") return InstanceStatus::FAILED;
  return InstanceStatus::UNKNOWN;
}

Result<InstanceStatus> Instance::parseInstanceStatus(const std::string &str) {
  pugi::xml_document doc;

  // 2. 加载XML字符串。pugi::parse_result会告诉你解析是否成功。
  //    使用 response.body.c_str() 来获取C风格字符串。
  pugi::xml_parse_result parse_result = doc.load_string(str.c_str());

  // 3. 检查解析结果。如果失败，返回错误。
  if (!parse_result) {
    return makeError<InstanceStatus>(
        ErrorCode::ParseError,  // 假设你有这个错误码
        "Failed to parse XML response: " +
            std::string(parse_result.description()));
  }

  // 4. 查找<Status>节点。
  //    我们可以使用XPath查询，或者简单的节点遍历。这里用节点遍历更清晰。
  //    首先找到根节点<Instance>
  pugi::xml_node instance_node = doc.child("Instance");
  if (!instance_node) {
    return makeError<InstanceStatus>(
        ErrorCode::ParseError, "XML response is missing <Instance> root node.");
  }

  //    然后在<Instance>下查找<Status>子节点
  pugi::xml_node status_node = instance_node.child("Status");
  if (!status_node) {
    return makeError<InstanceStatus>(
        ErrorCode::ParseError,
        "XML response is missing <Status> node inside <Instance>.");
  }

  // 5. 获取<Status>节点内的文本内容。
  //    .child_value() 是一个方便的快捷方式，等同于
  //    .child("Status").text().get()
  const char *status_text = status_node.child_value();

  // 6. 将文本内容映射到你的枚举类型。
  InstanceStatus status = stringToInstanceStatus(status_text);
  return makeSuccess(status);  // 假设你有 makeSuccess
}

Result<std::string> Instance::parseRawResult(const std::string &xml_body) {
  pugi::xml_document doc;
  pugi::xml_parse_result parse_result = doc.load_string(xml_body.c_str());

  if (!parse_result) {
    return makeError<std::string>(ErrorCode::ParseError,
                                  "Failed to parse RawResult XML: " +
                                      std::string(parse_result.description()));
  }

  pugi::xpath_node result_xpath_node =
      doc.select_node("/Instance/Tasks/Task/Result");
  if (!result_xpath_node) {
    pugi::xpath_node task_status_node =
        doc.select_node("/Instance/Tasks/Task/Status");
    if (task_status_node &&
        std::string(task_status_node.node().child_value()) == "Failed") {
      pugi::xpath_node task_result_node =
          doc.select_node("/Instance/Tasks/Task/Result");
      if (task_result_node) {
        return makeError<std::string>(
            ErrorCode::ParseError,
            "SQL task failed. Details: " +
                std::string(task_result_node.node().child_value()));
      }
      return makeError<std::string>(
          ErrorCode::ParseError,
          "SQL task failed, but no detailed error message was found.");
    }
    return makeError<std::string>(
        ErrorCode::ParseError,
        "Could not find '/Instance/Tasks/Task/Result' path in XML response.");
  }

  pugi::xml_node result_node = result_xpath_node.node();

  // 检查 Transform 属性
  const char *transform_attr = result_node.attribute("Transform").value();
  bool is_base64 =
      (transform_attr && std::strcmp(transform_attr, "Base64") == 0);

  if (is_base64) {
    // Base64 模式：取节点的直接文本内容（非 CDATA）
    const char *text_content = result_node.text().get();
    if (!text_content || std::strlen(text_content) == 0) {
      return makeError<std::string>(
          ErrorCode::ParseError,
          "Result node has Transform=\"Base64\" but contains no text content.");
    }

    try {
      std::string decoded = base64_decode(std::string(text_content), true);
      return makeSuccess(decoded);
    } catch (const std::exception &e) {
      return makeError<std::string>(
          ErrorCode::ParseError,
          "Failed to decode Base64 content: " + std::string(e.what()));
    }
  } else {
    // 普通模式：期望 CDATA
    pugi::xml_node cdata_node = result_node.first_child();
    if (cdata_node && cdata_node.type() == pugi::node_cdata) {
      return makeSuccess(std::string(cdata_node.value()));
    } else {
      // 兜底：尝试直接取文本（某些情况下可能没有 CDATA）
      const char *text_content = result_node.text().get();
      if (text_content && std::strlen(text_content) > 0) {
        return makeSuccess(std::string(text_content));
      }
      return makeError<std::string>(
          ErrorCode::ParseError,
          "Found <Result> node, but it contains neither CDATA nor plain text.");
    }
  }
}

Result<void> Instance::checkTaskStatus(const std::string &xml_body) {
  pugi::xml_document doc;
  pugi::xml_parse_result parse_result = doc.load_string(xml_body.c_str());

  if (!parse_result) {
    return makeError<void>(ErrorCode::ParseError,
                           "Failed to parse TaskStatus XML: " +
                               std::string(parse_result.description()));
  }

  pugi::xml_node instance_node = doc.child("Instance");
  if (!instance_node) {
    return makeError<void>(ErrorCode::ParseError,
                           "XML response is missing <Instance> root node.");
  }

  pugi::xml_node tasks_node = instance_node.child("Tasks");
  if (!tasks_node) {
    return makeError<void>(
        ErrorCode::ParseError,
        "XML response is missing <Tasks> node inside <Instance>.");
  }

  for (pugi::xml_node task_node : tasks_node.children("Task")) {
    pugi::xml_node status_node = task_node.child("Status");
    if (!status_node) {
      continue;
    }

    const char *status_text = status_node.child_value();
    if (std::string(status_text) == "Failed") {
      pugi::xml_node name_node = task_node.child("Name");
      std::string task_name = name_node ? name_node.child_value() : "Unknown";
      return makeError<void>(ErrorCode::ExecutionError,
                             "Task '" + task_name + "' failed.");
    }
  }

  return makeSuccess();
}

// Implement Table::FromXml static method to parse XML response from MaxCompute
// API
Result<Table> Table::FromXml(const std::string &xml_string) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_string(xml_string.c_str());

  if (!result) {
    MCO_LOG_ERROR("Failed to parse XML: {}", result.description());
    return makeError<Table>(
        ErrorCode::ParseError,
        "Failed to parse XML response: " + std::string(result.description()));
  }

  Table table;
  pugi::xml_node root = doc.child("Table");
  if (!root) {
    return makeError<Table>(ErrorCode::ParseError,
                            "XML does not contain 'Table' root element");
  }

  // Parse basic table information
  table.name = root.child("Name").text().get();
  table.description = root.child("Comment").text().get();
  table.project_name = root.child("Project").text().get();
  table.schema_name = root.child("SchemaName").text().get();
  // Parse Schema part which contains JSON
  pugi::xml_node schema_node = root.child("Schema");
  if (schema_node) {
    std::string schema_format = schema_node.attribute("format").as_string();
    if (schema_format == "Json") {
      std::string json_content = schema_node.text().get();
      if (!json_content.empty()) {
        try {
          auto json_obj = nlohmann::json::parse(json_content);
          auto parse_result = FromJsonSchema(json_obj);
          if (!parse_result.has_value()) {
            return parse_result;  // propagate error
          }

          Table &json_table = parse_result.value();
          // Merge the JSON content with XML content
          table.columns = std::move(json_table.columns);
          table.partition_columns = std::move(json_table.partition_columns);

        } catch (const std::exception &e) {
          MCO_LOG_ERROR("Failed to parse JSON schema: {}", e.what());
          return makeError<Table>(
              ErrorCode::ParseError,
              "Failed to parse JSON schema: " + std::string(e.what()));
        }
      }
    }
  }

  MCO_LOG_DEBUG(
      "Successfully parsed table: '{}' with {} columns and {} partition keys",
      table.name, table.columns.size(), table.partition_columns.size());
  return makeSuccess(std::move(table));
}

// Implement Table::FromJsonSchema static method
Result<Table> Table::FromJsonSchema(const nlohmann::json &json_schema) {
  Table table;

  if (json_schema.contains("comment")) {
    table.description = json_schema["comment"].get<std::string>();
  }

  // Parse columns array
  if (json_schema.contains("columns") && json_schema["columns"].is_array()) {
    for (const auto &col_json : json_schema["columns"]) {
      auto name = col_json.value("name", "");
      auto type_str = col_json.value("type", "");

      std::unique_ptr<typeinfo> type_info;
      try {
        type_info = TypeInfoParser::parse(type_str);
      } catch (const std::exception &e) {
        return makeError<Table>(ErrorCode::ParseError,
                                "Failed to parse type '" + type_str +
                                    "' for column '" + name + "': " + e.what());
      }

      Column col = Column::buildColumn(name, std::move(type_info));
      table.columns.push_back(std::move(col));
    }
  }

  // Parse partitionKeys array if present
  if (json_schema.contains("partitionKeys") &&
      json_schema["partitionKeys"].is_array()) {
    for (const auto &part_json : json_schema["partitionKeys"]) {
      auto name = part_json.value("name", "");
      auto type_str = part_json.value("type", "");

      std::unique_ptr<typeinfo> type_info;
      try {
        type_info = TypeInfoParser::parse(type_str);
      } catch (const std::exception &e) {
        return makeError<Table>(ErrorCode::ParseError,
                                "Failed to parse type '" + type_str +
                                    "' for column '" + name + "': " + e.what());
      }

      Column col = Column::buildColumn(name, std::move(type_info));
      table.partition_columns.push_back(std::move(col));
    }
  }

  MCO_LOG_DEBUG(
      "Successfully parsed JSON schema for table '{}' with {} columns and {} "
      "partition keys",
      table.name, table.columns.size(), table.partition_columns.size());

  return makeSuccess(std::move(table));
}

Column Column::buildColumn(const std::string &name,
                           std::unique_ptr<typeinfo> type_info) {
  if (!type_info) {
    throw std::invalid_argument("type_info cannot be null in buildColumn.");
  }

  Column col;
  col.name = name;
  col.type_info = std::move(type_info);
  return col;
}

Result<ResultSetSchema> ResultSetSchema::FromJson(const nlohmann::json &json) {
  try {
    // 验证顶层结构：必须有 "columns" 数组
    if (!json.contains("columns") || !json["columns"].is_array()) {
      return makeError<ResultSetSchema>(
          ErrorCode::ParseError,
          "Invalid schema JSON: missing 'columns' array.");
    }

    const auto &columns_json = json["columns"];
    std::vector<Column> columns;
    columns.reserve(columns_json.size());

    for (const auto &col_json : columns_json) {
      // ... (这里的循环内部逻辑完全不需要改变，它已经是在处理 nlohmann::json
      // 对象了) ...
      if (!col_json.is_object()) {
        return makeError<ResultSetSchema>(ErrorCode::ParseError,
                                          "Column must be an object.");
      }
      // ...
      std::string name = col_json.at("name").get<std::string>();
      std::string type_str = col_json.at("type").get<std::string>();

      // 使用 at() 代替 [] 可以提供更好的错误信息（如果字段不存在会抛出异常）

      std::unique_ptr<typeinfo> type_info;
      try {
        type_info = TypeInfoParser::parse(type_str);
      } catch (const std::exception &e) {
        return makeError<ResultSetSchema>(ErrorCode::ParseError,
                                          "Failed to parse type '" + type_str +
                                              "' for column '" + name +
                                              "': " + e.what());
      }

      Column col = Column::buildColumn(name, std::move(type_info));
      columns.push_back(std::move(col));
    }

    ResultSetSchema schema;
    schema.m_columns = std::move(columns);
    return makeSuccess(std::move(schema));

  } catch (const nlohmann::json::exception &e) {
    // 捕获 nlohmann::json 的异常，比如 at() 失败
    return makeError<ResultSetSchema>(
        ErrorCode::ParseError, "JSON access error: " + std::string(e.what()));
  } catch (const std::exception &e) {
    // 捕获其他意外错误，比如 TypeInfoParser 的
    return makeError<ResultSetSchema>(
        ErrorCode::ParseError,
        "Unexpected error during schema parsing: " + std::string(e.what()));
  }
}

// (可选) 如果你还想支持从字符串解析，可以让旧的 FromJson 调用新的
Result<ResultSetSchema> ResultSetSchema::FromJson(
    const std::string &json_string) {
  if (json_string.empty()) {
    return makeSuccess(ResultSetSchema{});
  }
  try {
    auto json = nlohmann::json::parse(json_string);
    return FromJson(json);  // 调用新的方法
  } catch (const nlohmann::json::parse_error &e) {
    return makeError<ResultSetSchema>(
        ErrorCode::ParseError, "JSON parse error: " + std::string(e.what()));
  }
}

// 实现 ToJson 方法
Result<std::string> ResultSetSchema::ToJson(const ResultSetSchema &schema) {
  try {
    nlohmann::json json;
    nlohmann::json columns_json = nlohmann::json::array();

    for (const auto &column : schema.m_columns) {
      nlohmann::json col_json;
      col_json["name"] = column.name;
      col_json["type"] = column.type_info ? column.type_info->toString()
                                          : "string";  // fallback to string
      columns_json.push_back(col_json);
    }

    json["columns"] = columns_json;
    return makeSuccess(json.dump());
  } catch (const nlohmann::json::exception &e) {
    return makeError<std::string>(
        ErrorCode::ParseError,
        std::string("JSON generation error: ") + e.what());
  } catch (const std::exception &e) {
    return makeError<std::string>(
        ErrorCode::ParseError,
        std::string("Unexpected error during JSON creation: ") + e.what());
  }
}

Result<std::vector<TableId>> TableId::FromXml(const std::string &project_name,
                                              const std::string &schema_name,
                                              const std::string &xml_string) {
  pugi::xml_document doc;

  // 1. 加载 XML 字符串
  pugi::xml_parse_result parse_result = doc.load_string(
      xml_string.c_str(), pugi::parse_default | pugi::encoding_utf8);

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
        ErrorCode::ParseError, "Root node <Tables> not found in XML response.");
  }

  std::vector<TableId> table_ids;

  // 4. 遍历所有的 <Table> 子节点
  for (pugi::xml_node table_node : tables_node.children("Table")) {
    TableId current_table;

    // 5. 提取 <Name> 和 <Owner>
    pugi::xml_node name_node = table_node.child("Name");

    // 检查节点是否存在，然后获取其文本内容
    if (name_node) {
      current_table.project_name = project_name;
      current_table.schema_name = schema_name;
      current_table.name = name_node.text().as_string();
    } else {
      MCO_LOG_WARNING("Found a <Table> node without a <Name> child. Skipping.");
      continue;
    }
    // 将解析出的 TableId 添加到结果向量中
    table_ids.push_back(std::move(current_table));
  }

  MCO_LOG_DEBUG("Successfully parsed {} tables from XML.", table_ids.size());

  // 6. 返回成功的结果
  return makeSuccess(std::move(table_ids));
}

// Implement Project::FromXml static method to parse XML response from
// MaxCompute API
Result<Project> Project::FromXml(const std::string &xml_string) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_string(xml_string.c_str());

  if (!result) {
    MCO_LOG_ERROR("Failed to parse Project XML: {}", result.description());
    return makeError<Project>(
        ErrorCode::ParseError,
        "Failed to parse XML response: " + std::string(result.description()));
  }

  Project project;
  pugi::xml_node root = doc.child("Project");
  if (!root) {
    return makeError<Project>(ErrorCode::ParseError,
                              "XML does not contain 'Project' root element");
  }

  // Parse basic project information
  project.name = root.child("Name").text().get();
  project.type = root.child("Type").text().get();
  project.tenant_id = root.child("TenantId").text().get();
  project.region = root.child("Region").text().get();

  // Parse Properties section which contains multiple Property entries
  pugi::xml_node properties_node = root.child("Properties");
  if (properties_node) {
    for (pugi::xml_node property_node : properties_node.children("Property")) {
      std::string name = property_node.child("Name").text().get();
      std::string value = property_node.child("Value").text().get();
      if (!name.empty()) {
        project.properties[name] = value;
      }
    }
  }

  MCO_LOG_DEBUG("Successfully parsed project: '{}' with {} properties",
                project.name, project.properties.size());
  return makeSuccess(std::move(project));
}
}  // namespace maxcompute_odbc
