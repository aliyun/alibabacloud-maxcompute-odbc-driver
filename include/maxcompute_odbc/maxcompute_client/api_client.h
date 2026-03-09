#ifndef MAXCOMPUTE_ODBC_TINY_MAXCOMPUTE_SDK_API_CLIENT_H_
#define MAXCOMPUTE_ODBC_TINY_MAXCOMPUTE_SDK_API_CLIENT_H_

#ifdef CPPHTTPLIB_BROTLI_SUPPORT
#undef CPPHTTPLIB_BROTLI_SUPPORT
#endif

#include "maxcompute_odbc/common/errors.h"
#include "maxcompute_odbc/config/config.h"
#include <httplib.h>
#include <map>
#include <memory>
#include <string>
#include "models.h"
#include "request_signer.h"

namespace maxcompute_odbc {

/**
 * @brief A generic API client for making HTTP requests.
 *
 * This class provides a simplified and generic interface over httplib,
 * handling common logic for requests, error checking, and logging.
 */
class ApiClient {
 public:
  /**
   * @brief Constructs an ApiClient with the given configuration.
   * @param config The configuration for the client (endpoint, timeouts, etc.).
   */
  explicit ApiClient(const Config &config);

  /**
   * @brief Sends a GET request.
   * @param path The request path (e.g., "/api/users").
   * @param params URL query parameters.
   * @param headers Custom request headers.
   * @return A Result containing the full httplib::Response on success.
   */
  Result<httplib::Response> Get(const std::string &path,
                                const httplib::Params &params = {},
                                const httplib::Headers &headers = {});

  /**
   * @brief Sends a POST request.
   * @param path The request path.
   * @param body The request body.
   * @param contentType The Content-Type of the request body. Defaults to
   * "application/json".
   * @param headers Custom request headers.
   * @return A Result containing the full httplib::Response on success.
   */
  Result<httplib::Response> Post(
      const std::string &path, const httplib::Params &params,
      const httplib::Headers &headers, const std::string &body = "",
      const std::string &contentType = "application/json");

  /**
   * @brief Sends a PUT request.
   * @param path The request path.
   * @param body The request body.
   * @param contentType The Content-Type of the request body. Defaults to
   * "application/json".
   * @param headers Custom request headers.
   * @return A Result containing the full httplib::Response on success.
   */
  Result<httplib::Response> Put(
      const std::string &path, const httplib::Params &params,
      const httplib::Headers &headers, const std::string &body = "",
      const std::string &contentType = "application/json");

  /**
   * @brief Sends a DELETE request.
   * @param path The request path.
   * @param headers Custom request headers.
   * @return A Result containing the full httplib::Response on success.
   */
  Result<httplib::Response> Delete(const std::string &path,
                                   const httplib::Headers &headers = {});

  /**
   * @brief Core private method to execute any HTTP request.
   * @param req The Request object describing the HTTP call.
   * @return A Result containing the full httplib::Response on success.
   */
  Result<httplib::Response> execute(const Request &req);

  /**
   * @brief Sends a GET request and streams the response body.
   *
   * This method is for handling large responses without buffering them entirely
   * in memory. It invokes the content receiver callback for each chunk of data
   * received.
   *
   * @param path The request path.
   * @param params URL query parameters.
   * @param headers Custom request headers.
   * @param receiver A callback function that will be called with chunks of the
   * response body. It should return true to continue receiving data, or false
   * to stop.
   * @return A Result containing a simplified response (status and headers) on
   * success. The body will be empty as it's handled by the receiver.
   */
  Result<httplib::Response> GetStreaming(const std::string &path,
                                         const httplib::Params &params,
                                         const httplib::Headers &headers,
                                         httplib::ContentReceiver receiver);

  /**
   * @brief 获取 MaxQA Session ID
   *
   * 通过向 MaxQA 端点发送请求来获取 Session ID
   *
   * @param project 项目名称
   * @param quotaName MaxQA Quota 名称
   * @return Result 包含 MaxQA Session ID
   */
  Result<std::string> getMaxQASessionId(const std::string &project,
                                        const std::string &quotaName);

 private:
  std::string prefix_;
  Config config_;
  std::unique_ptr<httplib::Client> client_;
  std::unique_ptr<RequestSigner> requestSigner_;

  /**
   * @brief Sets up proxy configuration from config
   */
  void setupProxy();
};

}  // namespace maxcompute_odbc

#endif  // MAXCOMPUTE_ODBC_TINY_MAXCOMPUTE_SDK_API_CLIENT_H_
