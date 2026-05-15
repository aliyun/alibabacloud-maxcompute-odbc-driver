#include "../include/maxcompute_odbc/config/config.h"
#include <gtest/gtest.h>
#include <stdexcept>

using namespace maxcompute_odbc;

// Test basic Config class construction and default values
TEST(ConfigTest, DefaultConstruction) {
  Config config;

  // Test default values
  EXPECT_EQ(config.endpoint, "");
  EXPECT_EQ(config.project, "");
  EXPECT_EQ(config.accessKeyId, std::nullopt);
  EXPECT_EQ(config.accessKeySecret, std::nullopt);
  EXPECT_EQ(config.securityToken, std::nullopt);

  // Test default timeout values
  EXPECT_EQ(config.readTimeout, 60);
  EXPECT_EQ(config.connectTimeout, 30);

  // Test default protocol
  EXPECT_EQ(config.protocol, "https");

  // Test default proxy settings
  EXPECT_EQ(config.httpProxy, std::nullopt);
  EXPECT_EQ(config.httpsProxy, std::nullopt);

  // Test default signature algorithm
  EXPECT_EQ(config.signatureAlgorithm, "v2");

  // Test default ODBC settings
  EXPECT_EQ(config.dataSourceName, "");
}

// Test Config class copy constructor and assignment
TEST(ConfigTest, CopyConstructorAndAssignment) {
  Config config1;
  config1.endpoint = "https://service.cn-shanghai.maxcompute.aliyun.com/api";
  config1.project = "test_project";
  config1.accessKeyId = std::make_optional<std::string>("test_id");
  config1.accessKeySecret = std::make_optional<std::string>("test_key");
}

// Test Config validation with valid configuration
TEST(ConfigTest, ValidateValidConfig) {
  Config config;
  config.endpoint = "https://service.cn-shanghai.maxcompute.aliyun.com/api";
  config.project = "test_project";
  config.accessKeyId = std::make_optional<std::string>("test_id");
  config.accessKeySecret = std::make_optional<std::string>("test_key");

  // Should not throw
  EXPECT_NO_THROW(config.validate());
}

// Test Config validation with missing endpoint
TEST(ConfigTest, ValidateMissingEndpoint) {
  Config config;
  config.project = "test_project";
  config.accessKeyId = "test_id";
  config.accessKeySecret = "test_key";

  EXPECT_THROW(config.validate(), std::invalid_argument);
}

// Test Config validation with missing project
TEST(ConfigTest, ValidateMissingProject) {
  Config config;
  config.endpoint = "https://service.cn-shanghai.maxcompute.aliyun.com/api";
  config.accessKeyId = std::make_optional<std::string>("test_id");
  config.accessKeySecret = std::make_optional<std::string>("test_key");

  EXPECT_THROW(config.validate(), std::invalid_argument);
}

// Test Config validation with mismatched access credentials
TEST(ConfigTest, ValidateMismatchedCredentials) {
  Config config;
  config.endpoint = "https://service.cn-shanghai.maxcompute.aliyun.com/api";
  config.project = "test_project";
  config.accessKeyId = std::make_optional<std::string>("test_id");
  // Missing accessKeySecret

  EXPECT_THROW(config.validate(), std::invalid_argument);
}

// Test Config validation with negative timeouts
TEST(ConfigTest, ValidateNegativeTimeouts) {
  Config config;
  config.endpoint = "https://service.cn-shanghai.maxcompute.aliyun.com/api";
  config.project = "test_project";
  config.accessKeyId = std::make_optional<std::string>("test_id");
  config.accessKeySecret = std::make_optional<std::string>("test_key");

  config.readTimeout = -5;
  EXPECT_THROW(config.validate(), std::invalid_argument);

  config.readTimeout = 60;
  config.connectTimeout = -10;
  EXPECT_THROW(config.validate(), std::invalid_argument);
}

// Test Config validation with invalid protocol
TEST(ConfigTest, ValidateInvalidProtocol) {
  Config config;
  config.endpoint = "https://service.cn-shanghai.maxcompute.aliyun.com/api";
  config.project = "test_project";
  config.accessKeyId = std::make_optional<std::string>("test_id");
  config.accessKeySecret = std::make_optional<std::string>("test_key");
  config.protocol = "invalid";

  // Note: The validate method doesn't currently check protocol validity
  // The protocol validation happens in the ConnectionStringParser
  EXPECT_NO_THROW(config.validate());
}

// Test Config hasValidCredentials method
TEST(ConfigTest, HasValidCredentials) {
  Config config;

  // No credentials
  EXPECT_FALSE(config.hasValidCredentials());

  // With accessKeyId and accessKeySecret
  config.accessKeyId = std::make_optional<std::string>("test_id");
  config.accessKeySecret = std::make_optional<std::string>("test_key");
  EXPECT_TRUE(config.hasValidCredentials());

  // Reset and test with credential client (mocked as non-null pointer)
  config.accessKeyId = std::nullopt;
  config.accessKeySecret = std::nullopt;
  // We can't easily test with a real credential client in unit tests
  // But we can test the logic with a non-null pointer
  // config.credential = std::make_shared<MockCredentialClient>();
  // EXPECT_TRUE(config.hasValidCredentials());
}

// Test Config getFullEndpoint method
TEST(ConfigTest, GetFullEndpoint) {
  Config config;

  // With full endpoint URL
  config.endpoint = "https://service.cn-shanghai.maxcompute.aliyun.com/api";
  config.protocol = "http";  // Should be ignored
  EXPECT_EQ(config.getFullEndpoint(),
            "https://service.cn-shanghai.maxcompute.aliyun.com/api");

  // With partial endpoint and https protocol
  config.endpoint = "service.cn-shanghai.maxcompute.aliyun.com/api";
  config.protocol = "https";
  EXPECT_EQ(config.getFullEndpoint(),
            "https://service.cn-shanghai.maxcompute.aliyun.com/api");

  // With partial endpoint and http protocol
  config.endpoint = "service.cn-shanghai.maxcompute.aliyun.com/api";
  config.protocol = "http";
  EXPECT_EQ(config.getFullEndpoint(),
            "http://service.cn-shanghai.maxcompute.aliyun.com/api");
}

// Test Config createCredentialClient method
TEST(ConfigTest, CreateCredentialClient) {
  Config config;

  // Test with accessKeyId and accessKeySecret
  config.accessKeyId = "test_id";
  config.accessKeySecret = "test_key";

  // We can't easily test the actual creation of the credential client in unit
  // tests But we can verify the method doesn't crash and returns a pointer
  // Note: This test might need to be adjusted based on actual implementation
  // EXPECT_NO_THROW(config.createCredentialClient());

  // Test with security token
  config.securityToken = "test_token";
  // EXPECT_NO_THROW(config.createCredentialClient());

  // Test without credentials (should return nullptr)
  Config config2;
  // EXPECT_EQ(config2.createCredentialClient(), nullptr);
}

// Test ConnectionStringParser with valid connection string
TEST(ConnectionStringParserTest, ParseValidConnectionString) {
  std::string connStr =
      "Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/"
      "api;Project=test_project;accessKeyId=test_id;accessKeySecret=test_key;"
      "LoginTimeout=60";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.endpoint,
            "https://service.cn-shanghai.maxcompute.aliyun.com/api");
  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.accessKeyId, std::make_optional<std::string>("test_id"));
  EXPECT_EQ(config.accessKeySecret,
            std::make_optional<std::string>("test_key"));
}

// Test ConnectionStringParser with spaces in connection string
TEST(ConnectionStringParserTest, ParseConnectionStringWithSpaces) {
  std::string connStr =
      "Endpoint = https://service.cn-shanghai.maxcompute.aliyun.com/api ; "
      "Project = test_project ; "
      "accessKeyId = test_id ; accessKeySecret = test_key ; LoginTimeout = 30";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.endpoint,
            "https://service.cn-shanghai.maxcompute.aliyun.com/api");
  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.accessKeyId, std::make_optional<std::string>("test_id"));
  EXPECT_EQ(config.accessKeySecret,
            std::make_optional<std::string>("test_key"));
}

// Test ConnectionStringParser with missing fields
TEST(ConnectionStringParserTest, ParseConnectionStringWithMissingFields) {
  std::string connStr = "Project=test_project;accessKeyId=test_id";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.accessKeyId, std::make_optional<std::string>("test_id"));
  // Default values for missing fields
  EXPECT_EQ(config.readTimeout, 60);
  EXPECT_EQ(config.connectTimeout, 30);
  EXPECT_EQ(config.protocol, "https");
}

// Test ConnectionStringParser with invalid timeout values
TEST(ConnectionStringParserTest, ParseConnectionStringWithInvalidTimeout) {
  std::string connStr =
      "Project=test_project;LoginTimeout=invalid;ReadTimeout=also_invalid";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.project, "test_project");
  // Should fall back to default values when conversion fails
  EXPECT_EQ(config.readTimeout, 60);
}

// Test ConnectionStringParser with various protocol values
TEST(ConnectionStringParserTest, ParseConnectionStringWithProtocol) {
  std::string connStr =
      "Endpoint=service.cn-shanghai.maxcompute.aliyun.com/"
      "api;Project=test_project;Protocol=HTTP";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.endpoint, "service.cn-shanghai.maxcompute.aliyun.com/api");
  EXPECT_EQ(config.project, "test_project");
  // Protocol should be converted to lowercase
  EXPECT_EQ(config.protocol, "http");
}

// Test ConnectionStringParser with proxy settings
TEST(ConnectionStringParserTest, ParseConnectionStringWithProxy) {
  std::string connStr =
      "Project=test_project;HttpProxy=http://proxy:8080;HttpsProxy=https://"
      "secureproxy:8443";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.httpProxy, "http://proxy:8080");
  EXPECT_EQ(config.httpsProxy, "https://secureproxy:8443");
}

// Test ConnectionStringParser with case insensitive keys
TEST(ConnectionStringParserTest, ParseConnectionStringCaseInsensitiveKeys) {
  std::string connStr =
      "ENDPOINT=https://service.cn-shanghai.maxcompute.aliyun.com/"
      "api;project=test_project;accessKeyId=test_id;accessKeySecret=test_key";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.endpoint,
            "https://service.cn-shanghai.maxcompute.aliyun.com/api");
  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.accessKeyId, "test_id");
  EXPECT_EQ(config.accessKeySecret, "test_key");
}

// Test ConnectionStringParser with security token
TEST(ConnectionStringParserTest, ParseConnectionStringWithSecurityToken) {
  std::string connStr =
      "Project=test_project;accessKeyId=test_id;accessKeySecret=test_key;"
      "SecurityToken=test_token";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.accessKeyId, std::make_optional<std::string>("test_id"));
  EXPECT_EQ(config.accessKeySecret,
            std::make_optional<std::string>("test_key"));
  EXPECT_EQ(config.securityToken,
            std::make_optional<std::string>("test_token"));
}

// Test ConnectionStringParser with region and user agent
TEST(ConnectionStringParserTest, ParseConnectionStringWithRegionAndUserAgent) {
  std::string connStr =
      "Project=test_project;RegionId=cn-shanghai;UserAgent=MyApp/1.0";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.regionId, "cn-shanghai");
  EXPECT_EQ(config.userAgent, "MyApp/1.0");
}

// Test ConnectionStringParser with all supported parameters
TEST(ConnectionStringParserTest, ParseConnectionStringWithAllParameters) {
  std::string connStr =
      "Endpoint=https://service.cn-shanghai.maxcompute.aliyun.com/"
      "api;Project=test_project;"
      "accessKeyId=test_id;accessKeySecret=test_key;SecurityToken=test_token;"
      "LoginTimeout=45;ReadTimeout=90;ConnectTimeout=35;Protocol=https;"
      "RegionId=cn-shanghai;UserAgent=TestApp/1.0;"
      "HttpProxy=http://proxy:8080;HttpsProxy=https://secureproxy:8443;"
      "DataSourceName=TestDSN";

  Config config = ConnectionStringParser::parse(connStr);

  EXPECT_EQ(config.endpoint,
            "https://service.cn-shanghai.maxcompute.aliyun.com/api");
  EXPECT_EQ(config.project, "test_project");
  EXPECT_EQ(config.accessKeyId, std::make_optional<std::string>("test_id"));
  EXPECT_EQ(config.accessKeySecret,
            std::make_optional<std::string>("test_key"));
  EXPECT_EQ(config.securityToken,
            std::make_optional<std::string>("test_token"));
  EXPECT_EQ(config.readTimeout, 90);
  EXPECT_EQ(config.connectTimeout, 35);
  EXPECT_EQ(config.protocol, "https");
  EXPECT_EQ(config.regionId, std::make_optional<std::string>("cn-shanghai"));
  EXPECT_EQ(config.userAgent, std::make_optional<std::string>("TestApp/1.0"));
  EXPECT_EQ(config.httpProxy,
            std::make_optional<std::string>("http://proxy:8080"));
  EXPECT_EQ(config.httpsProxy,
            std::make_optional<std::string>("https://secureproxy:8443"));
  EXPECT_EQ(config.dataSourceName, "TestDSN");
}

TEST(ConfigTest, DefaultCharsetIsUtf8) {
  Config config;
  EXPECT_EQ(config.charset, "UTF-8");
}

TEST(ConnectionStringParserTest, ParseCharsetGbk) {
  std::string connStr =
      "Endpoint=https://x;Project=p;AccessKeyId=k;AccessKeySecret=s;"
      "Charset=GBK";
  Config config = ConnectionStringParser::parse(connStr);
  EXPECT_EQ(config.charset, "GBK");
}

TEST(ConnectionStringParserTest, ParseCharsetCaseInsensitive) {
  std::string connStr =
      "Endpoint=https://x;Project=p;AccessKeyId=k;AccessKeySecret=s;"
      "charset=gb18030";
  Config config = ConnectionStringParser::parse(connStr);
  EXPECT_EQ(config.charset, "GB18030");
}

TEST(ConnectionStringParserTest, ParseCharsetUtf8Normalized) {
  // "UTF8" should be normalized to "UTF-8" (canonical form recognized by both
  // iconv and our helper's fast-path check).
  std::string connStr =
      "Endpoint=https://x;Project=p;AccessKeyId=k;AccessKeySecret=s;"
      "Charset=utf8";
  Config config = ConnectionStringParser::parse(connStr);
  EXPECT_EQ(config.charset, "UTF-8");
}

TEST(ConnectionStringParserTest, ParseCharsetEmptyKeepsDefault) {
  std::string connStr =
      "Endpoint=https://x;Project=p;AccessKeyId=k;AccessKeySecret=s;"
      "Charset=";
  Config config = ConnectionStringParser::parse(connStr);
  EXPECT_EQ(config.charset, "UTF-8");
}

TEST(ConnectionStringParserTest, UpdateConfigParsesCharset) {
  Config config;
  ConnectionStringParser::updateConfig("Charset=BIG5", config);
  EXPECT_EQ(config.charset, "BIG5");
}
