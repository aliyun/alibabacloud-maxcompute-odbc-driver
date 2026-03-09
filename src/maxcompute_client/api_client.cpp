#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/common/utils.h"
#include "maxcompute_odbc/maxcompute_client/api_client.h"
#include <sstream>

namespace maxcompute_odbc {

ApiClient::ApiClient(const Config &config) : config_(config) {
  MCO_LOG_DEBUG("ApiClient created");

  requestSigner_ = std::make_unique<RequestSigner>(config_);

  std::string base_url = config_.endpoint;
  auto pair = parsePrefix(base_url);
  prefix_ = pair.second;
  client_ = std::make_unique<httplib::Client>(pair.first);
  MCO_LOG_DEBUG("Base URL: {}", base_url);

  // Set default timeouts
  client_->set_connection_timeout(config_.connectTimeout, 0);
  client_->set_read_timeout(config_.readTimeout, 0);

  // Setup proxy configuration
  setupProxy();

  // Enable TCP keep-alive for better performance with frequent requests
  client_->set_keep_alive(true);
}

void ApiClient::setupProxy() {
  // Determine which proxy to use based on endpoint protocol
  bool isHttps = config_.endpoint.find("https://") == 0;
  auto proxy_optional = isHttps ? config_.httpsProxy : config_.httpProxy;

  // If proxy not configured in connection string, try to read from system
  // settings
  if (!proxy_optional.has_value() || proxy_optional->empty()) {
    auto system_proxy = getSystemProxy(isHttps);
    std::string system_proxy_url = std::get<0>(system_proxy);
    std::string system_proxy_user = std::get<1>(system_proxy);
    std::string system_proxy_pass = std::get<2>(system_proxy);

    if (!system_proxy_url.empty()) {
      proxy_optional = system_proxy_url;
      // Set proxy user/password from system settings if not already set
      if (!system_proxy_user.empty() &&
          (!config_.proxyUser.has_value() || config_.proxyUser->empty())) {
        config_.proxyUser = system_proxy_user;
      }
      if (!system_proxy_pass.empty() && (!config_.proxyPassword.has_value() ||
                                         config_.proxyPassword->empty())) {
        config_.proxyPassword = system_proxy_pass;
      }
    }
  }

  if (!proxy_optional.has_value() || proxy_optional->empty()) {
    return;  // No proxy configured
  }

  std::string proxy = proxy_optional.value();

  // Parse proxy URL to extract host, port, username, password
  auto parsed = parseProxyUrl(proxy);
  std::string proxyHost = std::get<0>(parsed);
  int proxyPort = std::get<1>(parsed);
  std::string proxyUser = std::get<2>(parsed);
  std::string proxyPassword = std::get<3>(parsed);

  // Use explicit proxy user/password from config if available
  if (config_.proxyUser.has_value() && !config_.proxyUser->empty()) {
    proxyUser = config_.proxyUser.value();
  }
  if (config_.proxyPassword.has_value() && !config_.proxyPassword->empty()) {
    proxyPassword = config_.proxyPassword.value();
  }

  // Set proxy host and port using httplib's native method
  if (!proxyHost.empty()) {
    client_->set_proxy(proxyHost, proxyPort);
    MCO_LOG_INFO("Using proxy: {}:{}", proxyHost, proxyPort);
  } else {
    // Fallback to old parsing method if new parsing failed
    auto host_and_port = parseHostAndPort(proxy);
    client_->set_proxy(host_and_port.first, host_and_port.second);
    MCO_LOG_INFO("Using proxy: '{}'", proxy);
  }

  // Set proxy authentication using httplib's native method
  if (!proxyUser.empty()) {
    client_->set_proxy_basic_auth(proxyUser, proxyPassword);
    MCO_LOG_DEBUG("Proxy authentication configured for user: {}", proxyUser);
  }
}

// Private core execution method
Result<httplib::Response> ApiClient::execute(const Request &req) {
  MCO_LOG_DEBUG("Sending {} request to: {}", req.method, req.path);

  // Prepare the request object for httplib
  httplib::Request http_req;
  http_req.method = req.method;
  http_req.path = req.path;
  http_req.headers = req.headers;
  http_req.params = req.params;
  http_req.body = req.body;

  if (req.receiver.has_value()) {
    auto upstream_receiver = req.receiver.value();
    MCO_LOG_DEBUG("Attaching content receiver to the request.");
    // httplib::Request 的 content_receiver 是 ContentReceiverWithProgress 类型
    http_req.content_receiver =
        [upstream_receiver](const char *data, size_t data_length,
                            uint64_t offset, uint64_t total_length) -> bool {
      MCO_LOG_DEBUG("Received data chunk. Size: {}, Offset: {}, Total: {}",
                    data_length, offset, total_length);

      // 调用上游 receiver，并进行必要的类型转换 (size_t -> uint64_t)
      return upstream_receiver(data, static_cast<uint64_t>(data_length));
    };
  }

  // Set Content-Type header if provided for methods that have a body
  if (!req.contentType.empty()) {
    http_req.headers.emplace("Content-Type", req.contentType);
  }
  // Build User-Agent header following the standard format
  std::string userAgent = "MaxCompute-ODBC-Driver/1.0";
  if (config_.userAgent.has_value() && !config_.userAgent.value().empty()) {
    userAgent += " " + config_.userAgent.value();
  }
  http_req.headers.emplace("User-Agent", userAgent);
  http_req.headers.emplace("x-odps-user-agent", userAgent);
  http_req.params.emplace("curr_project", config_.project);

  // Use the generic send method
  requestSigner_->sign(http_req);
  http_req.path = prefix_ + http_req.path;
  auto response = client_->send(http_req);

  // --- Common Error Handling ---
  if (!response) {
    std::string error_msg = "Failed to send " + req.method + " request to " +
                            req.path + ": " + to_string(response.error());
    MCO_LOG_ERROR(error_msg);
    return makeError<httplib::Response>(ErrorCode::NetworkError, error_msg);
  }

  // Check for successful status codes (2xx range)
  if (response->status < 200 || response->status >= 300) {
    std::string error_msg =
        req.method + " request to " + req.path +
        " failed with status: " + std::to_string(response->status) +
        ". Body: " + response->body;
    MCO_LOG_ERROR(error_msg);
    return makeError<httplib::Response>(ErrorCode::NetworkError, error_msg);
  }

  MCO_LOG_DEBUG("{} request to {} succeeded with status: {}", req.method,
                req.path, response->status);
  return makeSuccess(*response);
}

// --- Public Methods (Thin Wrappers) ---

Result<httplib::Response> ApiClient::Get(const std::string &path,
                                         const httplib::Params &params,
                                         const httplib::Headers &headers) {
  Request req;
  req.method = "GET";
  req.path = path;
  req.params = params;
  req.headers = headers;
  return execute(req);
}

Result<httplib::Response> ApiClient::Post(const std::string &path,
                                          const httplib::Params &params,
                                          const httplib::Headers &headers,
                                          const std::string &body,
                                          const std::string &contentType) {
  Request req;
  req.method = "POST";
  req.path = path;
  req.params = params;
  req.headers = headers;
  req.body = body;
  req.contentType = contentType;
  return execute(req);
}

Result<httplib::Response> ApiClient::Put(const std::string &path,
                                         const httplib::Params &params,
                                         const httplib::Headers &headers,
                                         const std::string &body,
                                         const std::string &contentType) {
  Request req;
  req.method = "PUT";
  req.path = path;
  req.params = params;
  req.headers = headers;
  req.body = body;
  req.contentType = contentType;
  return execute(req);
}

Result<httplib::Response> ApiClient::Delete(const std::string &path,
                                            const httplib::Headers &headers) {
  Request req;
  req.method = "DELETE";
  req.path = path;
  req.headers = headers;
  return execute(req);
}

Result<httplib::Response> ApiClient::GetStreaming(
    const std::string &path, const httplib::Params &params,
    const httplib::Headers &headers, httplib::ContentReceiver receiver) {
  Request req;
  req.method = "GET";
  req.path = path;
  req.params = params;
  req.headers = headers;
  req.receiver = std::move(receiver);
  return execute(req);
}

Result<std::string> ApiClient::getMaxQASessionId(const std::string &project,
                                                 const std::string &quotaName) {
  std::string path = "/mcqa/projects/" + project + "/sessions";
  httplib::Params params;
  params.emplace("quota", quotaName);
  httplib::Headers headers;

  MCO_LOG_DEBUG("Getting MaxQA Session ID for project: {}, quota: {}", project,
                quotaName);

  auto response_result = Post(path, params, headers, "", "application/json");
  if (!response_result.has_value()) {
    return makeError<std::string>(
        ErrorCode::NetworkError,
        "Failed to get MaxQA Session ID: " + response_result.error().message);
  }

  auto response = response_result.value();
  if (response.status != 200 && response.status != 201) {
    return makeError<std::string>(ErrorCode::NetworkError,
                                  "Failed to get MaxQA Session ID. Status: " +
                                      std::to_string(response.status) +
                                      ", Body: " + response.body);
  }

  // 从响应头中获取 x-odps-mcqa-conn
  auto session_id_it = response.headers.find("x-odps-mcqa-conn");
  if (session_id_it == response.headers.end()) {
    return makeError<std::string>(
        ErrorCode::ParseError,
        "MaxQA Session ID not found in response headers");
  }

  std::string session_id = session_id_it->second;
  MCO_LOG_DEBUG("Got MaxQA Session ID: {}", session_id);
  return makeSuccess(session_id);
}
}  // namespace maxcompute_odbc
