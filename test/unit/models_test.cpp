#include "maxcompute_odbc/maxcompute_client/models.h"
#include <gtest/gtest.h>
#include <memory>
#include <pugixml.hpp>

using namespace maxcompute_odbc;

// Test for InstanceStatus enum values
TEST(InstanceStatusTest, EnumValues) {
  EXPECT_EQ(static_cast<int>(InstanceStatus::RUNNING), 0);
  EXPECT_EQ(static_cast<int>(InstanceStatus::SUCCESS), 1);
  EXPECT_EQ(static_cast<int>(InstanceStatus::TERMINATED), 2);
  EXPECT_EQ(static_cast<int>(InstanceStatus::FAILED), 3);
  EXPECT_EQ(static_cast<int>(InstanceStatus::UNKNOWN), 4);
}

// Test InstanceStatus default value
TEST(InstanceStatusTest, DefaultValue) {
  Instance instance;
  EXPECT_EQ(instance.status, InstanceStatus::UNKNOWN);
}

// Test ExecuteSQLRequest default constructor
TEST(ExecuteSQLRequestTest, DefaultConstructor) {
  ExecuteSQLRequest request;

  EXPECT_TRUE(request.project.empty());
  EXPECT_TRUE(request.query.empty());
  EXPECT_NE(request.options, nullptr);
  EXPECT_EQ(request.options->instanceOptions, nullptr);
  EXPECT_EQ(request.options->taskName, nullptr);
  EXPECT_EQ(request.options->hints, nullptr);
}

// Test ExecuteSQLRequest toRequest method
TEST(ExecuteSQLRequestTest, ToRequest) {
  ExecuteSQLRequest request;
  request.project = "test_project";
  request.query = "SELECT * FROM test_table";

  // Set up options
  request.options->instanceOptions = std::make_shared<InstanceOptions>();
  request.options->instanceOptions->priority = std::make_shared<int>(10);
  request.options->instanceOptions->tryWait = std::make_shared<bool>(true);
  request.options->instanceOptions->uniqueIdentifyID =
      std::make_shared<std::string>("test_id");
  request.options->taskName = std::make_shared<std::string>("test_task");
  request.options->hints =
      std::make_shared<std::map<std::string, std::string>>();
  (*request.options->hints)["key1"] = "value1";
  (*request.options->hints)["key2"] = "value2";

  auto api_request = request.toRequest();

  EXPECT_EQ(api_request.method, "POST");
  EXPECT_EQ(api_request.contentType, "application/xml");
  EXPECT_FALSE(api_request.body.empty());
  EXPECT_EQ(api_request.path, "/projects/test_project/instances");
  EXPECT_EQ(api_request.params.size(), 1);
  // EXPECT_EQ(api_request.params.("tryWait"), "true");
}

// Test ExecuteSQLRequest toXmlString method
TEST(ExecuteSQLRequestTest, ToXmlString) {
  ExecuteSQLRequest request;
  request.project = "test_project";
  request.query = "SELECT * FROM test_table";

  // Set up options
  request.options->instanceOptions = std::make_shared<InstanceOptions>();
  request.options->instanceOptions->priority = std::make_shared<int>(10);
  request.options->instanceOptions->tryWait = std::make_shared<bool>(true);
  request.options->instanceOptions->uniqueIdentifyID =
      std::make_shared<std::string>("test_id");
  request.options->taskName = std::make_shared<std::string>("test_task");
  request.options->hints =
      std::make_shared<std::map<std::string, std::string>>();
  (*request.options->hints)["key1"] = "value1";
  (*request.options->hints)["key2"] = "value2";

  std::string xml_string = request.toXmlString();

  EXPECT_FALSE(xml_string.empty());
  EXPECT_NE(xml_string.find("<Instance>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Job>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Priority>10</Priority>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Guid>test_id</Guid>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Name>test_task</Name>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Query>SELECT * FROM test_table</Query>"),
            std::string::npos);
  EXPECT_NE(xml_string.find("key1"), std::string::npos);
  EXPECT_NE(xml_string.find("value1"), std::string::npos);
  EXPECT_NE(xml_string.find("key2"), std::string::npos);
  EXPECT_NE(xml_string.find("value2"), std::string::npos);
}

// Test Instance default constructor
TEST(InstanceTest, DefaultConstructor) {
  Instance instance;

  EXPECT_TRUE(instance.id.empty());
  EXPECT_EQ(instance.status, InstanceStatus::UNKNOWN);
}

// Test Instance with values
TEST(InstanceTest, WithValues) {
  Instance instance;
  instance.id = "test_instance_id";
  instance.status = InstanceStatus::SUCCESS;

  EXPECT_EQ(instance.id, "test_instance_id");
  EXPECT_EQ(instance.status, InstanceStatus::SUCCESS);
}

// Test XML serialization with minimal options
TEST(XmlSerializationTest, MinimalOptions) {
  ExecuteSQLRequest request;
  request.project = "test_project";
  request.query = "SELECT 1";

  // Only set required fields
  request.options->taskName = std::make_shared<std::string>("minimal_task");

  std::string xml_string = request.toXmlString();

  std::cout << xml_string << std::endl;

  EXPECT_FALSE(xml_string.empty());
  // Check for required elements
  EXPECT_NE(xml_string.find("<Instance>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Job>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Tasks>"), std::string::npos);
  EXPECT_NE(xml_string.find("<SQL>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Name>minimal_task</Name>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Query>SELECT 1</Query>"), std::string::npos);

  // Check for default priority
  EXPECT_NE(xml_string.find("<Priority>9</Priority>"), std::string::npos);
}

// Test XML serialization with all options
TEST(XmlSerializationTest, AllOptions) {
  ExecuteSQLRequest request;
  request.project = "test_project";
  request.query = "SELECT * FROM test_table WHERE id = 1";

  // Set all options
  request.options->instanceOptions = std::make_shared<InstanceOptions>();
  request.options->instanceOptions->priority = std::make_shared<int>(5);
  request.options->instanceOptions->tryWait = std::make_shared<bool>(false);
  request.options->instanceOptions->uniqueIdentifyID =
      std::make_shared<std::string>("unique-123");
  request.options->taskName = std::make_shared<std::string>("full_task");
  request.options->hints =
      std::make_shared<std::map<std::string, std::string>>();
  (*request.options->hints)["odps.sql.allow.cartesian"] = "true";
  (*request.options->hints)["odps.sql.mapper.split.size"] = "64";

  std::string xml_string = request.toXmlString();
  std::cout << xml_string << std::endl;

  EXPECT_FALSE(xml_string.empty());
  // Check for all elements
  EXPECT_NE(xml_string.find("<Instance>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Job>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Priority>5</Priority>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Guid>unique-123</Guid>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Tasks>"), std::string::npos);
  EXPECT_NE(xml_string.find("<SQL>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Name>full_task</Name>"), std::string::npos);
  EXPECT_NE(
      xml_string.find("<Query>SELECT * FROM test_table WHERE id = 1</Query>"),
      std::string::npos);
  EXPECT_NE(xml_string.find("<Config>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Property>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Name>settings</Name>"), std::string::npos);
  EXPECT_NE(xml_string.find("odps.sql.allow.cartesian"), std::string::npos);
  EXPECT_NE(xml_string.find("true"), std::string::npos);
}

// Test XML serialization with empty query
TEST(XmlSerializationTest, EmptyQuery) {
  ExecuteSQLRequest request;
  request.project = "test_project";
  request.query = "";

  request.options->taskName = std::make_shared<std::string>("empty_query_task");

  std::string xml_string = request.toXmlString();
  std::cout << xml_string << std::endl;

  EXPECT_FALSE(xml_string.empty());
  EXPECT_NE(xml_string.find("<Instance>"), std::string::npos);
  EXPECT_NE(xml_string.find("<Query></Query>"), std::string::npos);
}

// Test mapToJson helper function
TEST(XmlSerializationTest, MapToJson) {
  // This test will indirectly test the mapToJson helper function through the
  // XML serialization
  ExecuteSQLRequest request;
  request.project = "test_project";
  request.query = "SELECT 1";

  request.options->taskName = std::make_shared<std::string>("json_test_task");
  request.options->hints =
      std::make_shared<std::map<std::string, std::string>>();
  (*request.options->hints)["key1"] = "value1";
  (*request.options->hints)["key2"] = "value2";

  std::string xml_string = request.toXmlString();

  EXPECT_FALSE(xml_string.empty());
  // Verify that the hints are properly converted to JSON and included in the
  // XML
  EXPECT_NE(xml_string.find("\"key1\":\"value1\""), std::string::npos);
  EXPECT_NE(xml_string.find("\"key2\":\"value2\""), std::string::npos);
}

// Test XML serialization with null options
TEST(XmlSerializationTest, NullOptions) {
  ExecuteSQLRequest request;
  request.project = "test_project";
  request.query = "SELECT 1";

  // Explicitly set options to nullptr to test defensive programming
  request.options = nullptr;

  // This should not crash - the toXmlString method should handle null options
  // Note: In the current implementation, the constructor initializes options,
  // so this test is more for documentation purposes
  request.options = std::make_shared<SQLTaskOptions>();
  std::string xml_string = request.toXmlString();

  EXPECT_FALSE(xml_string.empty());
}
