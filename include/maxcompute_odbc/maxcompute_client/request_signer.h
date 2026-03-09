#pragma once

#include <string>
#ifdef CPPHTTPLIB_BROTLI_SUPPORT
#undef CPPHTTPLIB_BROTLI_SUPPORT
#endif
#include "maxcompute_odbc/config/config.h"
#include "httplib.h"

namespace maxcompute_odbc {
class RequestSigner {
 public:
  explicit RequestSigner(const Config &config);

  void sign(httplib::Request &req) const;

  ~RequestSigner() = default;

  // 禁止拷贝
  RequestSigner(const RequestSigner &) = delete;

  RequestSigner &operator=(const RequestSigner &) = delete;

  // 允许移动
  RequestSigner(RequestSigner &&) = default;

  RequestSigner &operator=(RequestSigner &&) = default;

 private:
  Config config_;
  std::shared_ptr<ICredentialsProvider> credential_;
};
}  // namespace maxcompute_odbc
