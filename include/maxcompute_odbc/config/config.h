#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include "credentials.h"
namespace maxcompute_odbc {

/**
 * Unified configuration class for MaxCompute ODBC driver and SDK
 * This class holds all configuration parameters needed for connecting to
 * MaxCompute
 */
class Config {
 public:
  // Core connection parameters
  std::string endpoint;
  std::string project;
  std::string schema = "default";
  std::optional<std::string> accessKeyId;
  std::optional<std::string> accessKeySecret;
  std::optional<std::string> securityToken;  // For temporary credentials
  std::string logviewHost = "https://maxcompute.console.aliyun.com";

  int readTimeout = 60;     // Read timeout in seconds
  int connectTimeout = 30;  // Connection timeout in seconds

  std::string protocol = "https";  // Connection protocol

  // Proxy settings
  std::optional<std::string> httpProxy;
  std::optional<std::string> httpsProxy;
  std::optional<std::string> proxyUser;      // Proxy authentication username
  std::optional<std::string> proxyPassword;  // Proxy authentication password

  // Authentication and identity
  std::shared_ptr<ICredentialsProvider> credential;
  std::string regionId;
  std::optional<std::string> userAgent;
  std::optional<std::string> signatureAlgorithm = "v2";

  // ODBC specific settings
  std::string dataSourceName;  // Data source name
  std::optional<std::string> tunnelEndpoint;
  std::optional<std::string> tunnelQuotaName;
  std::map<std::string, std::string> globalSettings;
  std::string timezone = "unknown";  // Timezone for timestamp handling
  bool namespaceSchema = true;

  // MaxQA settings
  bool interactiveMode = false;          // Enable MaxQA interactive mode
  std::optional<std::string> quotaName;  // Quota name for MaxQA or quota hints

  // download result settings
  uint64_t fetchResultSplitSize = 10000;
  size_t fetchResultThreadNum = 5;
  size_t fetchResultPreloadSplitNum = 5;

  /**
   * Default constructor
   */
  Config() = default;

  /**
   * Copy constructor
   */
  Config(const Config &other) = default;

  /**
   * Assignment operator
   */
  Config &operator=(const Config &other) = default;

  /**
   * Destructor
   */
  virtual ~Config() = default;

  /**
   * Validates the configuration parameters
   * @throws std::invalid_argument if required parameters are missing
   */
  void validate() const;

  /**
   * Checks if the configuration has valid credentials
   * @return true if credentials are valid, false otherwise
   */
  bool hasValidCredentials() const;

  /**
   * Creates an credential client from the configuration
   * @return shared pointer to credential client
   */
  std::shared_ptr<ICredentialsProvider> getCredentialsProvider() const;

  /**
   * Gets the full endpoint URL, ensuring it has the proper protocol prefix
   * @return full endpoint URL
   */
  std::string getFullEndpoint() const;
};

/**
 * Parser for ODBC connection strings
 * Converts connection string to Config object
 */
class ConnectionStringParser {
 public:
  /**
   * Parses an ODBC connection string into a Config object
   * @param connStr ODBC connection string
   * @return Config object with parsed parameters
   */
  static Config parse(const std::string &connStr);

  static std::map<std::string, std::string> parseDriverConnectString(
      const std::string &conn_str);

  static void updateConfig(const std::string &connStr, Config &config);
};

}  // namespace maxcompute_odbc
