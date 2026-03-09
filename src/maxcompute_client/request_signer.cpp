#include "maxcompute_odbc/common/logging.h"
#include "maxcompute_odbc/maxcompute_client/request_signer.h"
#include <algorithm>
#include <cctype>
#include <iomanip>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <sstream>
#include "cpp-base64/base64.h"

namespace maxcompute_odbc {
RequestSigner::RequestSigner(const Config &config) : config_(config) {
  MCO_LOG_DEBUG("RequestSigner created");

  credential_ = config_.getCredentialsProvider();
  if (credential_ == nullptr) {
    MCO_LOG_ERROR("Failed to create credential config");
    return;
  }
}

// HMAC-SHA1 签名
std::vector<unsigned char> hmacSha1(const std::string &data,
                                    const std::string &key) {
  unsigned char *digest;
  unsigned int digest_len;
  digest = HMAC(EVP_sha1(), key.c_str(), key.length(),
                reinterpret_cast<const unsigned char *>(data.c_str()),
                data.length(), NULL, &digest_len);

  return std::vector<unsigned char>(digest, digest + digest_len);
}

// --- 核心签名逻辑实现 ---

std::string getRfc822GmtDate() {
  auto now = std::chrono::system_clock::now();
  auto itt = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  // 使用 std::put_time 来格式化，并手动添加 "GMT"
  ss << std::put_time(std::gmtime(&itt), "%a, %d %b %Y %H:%M:%S") << " GMT";
  return ss.str();
}

std::string buildCanonicalResource(const std::string &resource,
                                   const httplib::Params &params) {
  std::stringstream builder;
  builder << resource;

  if (!params.empty()) {
    // httplib::Params 内部是 multimap，我们需要先提取唯一的 key 并排序
    std::map<std::string, std::string> sorted_params;
    for (const auto &pair : params) {
      // 如果一个 key 有多个值，这里只取第一个，符合大多数签名实践
      if (sorted_params.find(pair.first) == sorted_params.end()) {
        sorted_params[pair.first] = pair.second;
      }
    }

    char separater = '?';
    for (const auto &pair : sorted_params) {
      builder << separater;
      builder << pair.first;
      if (!pair.second.empty()) {
        builder << "=" << pair.second;
      }
      separater = '&';
    }
  }
  return builder.str();
}

std::string buildCanonicalString(const std::string &method,
                                 const std::string &resource,
                                 const httplib::Params &params,
                                 const httplib::Headers &headers) {
  std::stringstream builder;
  builder << method << "\n";

  std::map<std::string, std::string> headersToSign;
  const std::string PREFIX = "x-odps-";

  // 1. 提取需要签名的 header
  for (const auto &header : headers) {
    std::string lowerKey = header.first;
    std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(),
                   ::tolower);

    if (lowerKey == "content-md5" || lowerKey == "content-type" ||
        lowerKey == "date" || lowerKey.rfind(PREFIX, 0) == 0) {
      headersToSign[lowerKey] = header.second;
    }
  }

  // 2. 如果 Content-Type 和 Content-MD5 不存在，添加空值
  if (headersToSign.find("content-type") == headersToSign.end()) {
    headersToSign["content-type"] = "";
  }
  if (headersToSign.find("content-md5") == headersToSign.end()) {
    headersToSign["content-md5"] = "";
  }

  // 3. 添加 x-odps- 开头的 params 到待签名列表
  for (const auto &p : params) {
    if (p.first.rfind(PREFIX, 0) == 0) {
      headersToSign[p.first] = p.second;
    }
  }

  // 4. 按 key 字典序拼接字符串 (std::map 自动保证了顺序)
  for (const auto &entry : headersToSign) {
    if (entry.first.rfind(PREFIX, 0) == 0) {
      builder << entry.first << ":" << entry.second;
    } else {
      builder << entry.second;
    }
    builder << "\n";
  }

  // 5. 添加 canonical resource
  builder << buildCanonicalResource(resource, params);

  return builder.str();
}

std::string getSignature(const std::string &strToSign,
                         const std::string &accessKeyId,
                         const std::string &accessKeySecret) {
  auto hmac_result = hmacSha1(strToSign, accessKeySecret);
  std::string signature = base64_encode(hmac_result.data(), hmac_result.size());

  std::stringstream auth_header;
  auth_header << "ODPS " << accessKeyId << ":" << signature;
  return auth_header.str();
}

void RequestSigner::sign(httplib::Request &req) const {
  MCO_LOG_DEBUG("Preparing to execute {} request to: {}", req.method, req.path);

  // --- 签名流程开始 ---

  // 1. 添加 Date 头
  if (req.headers.find("Date") == req.headers.end()) {
    req.headers.emplace("Date", getRfc822GmtDate());
  }

  // 2. 构建待签名字符串
  std::string stringToSign =
      buildCanonicalString(req.method, req.path, req.params, req.headers);
  MCO_LOG_DEBUG("String to sign:\n---\n{}\n---", stringToSign);

  ICredentials &credential_model = credential_->getCredentials();

  // 3. 计算签名并生成 Authorization 头
  std::string authorization =
      getSignature(stringToSign, credential_model.getAccessKeyId(),
                   credential_model.getAccessKeySecret());
  // 添加或覆盖 Authorization 头
  req.headers.erase("Authorization");
  req.headers.emplace("Authorization", authorization);

  if (!credential_model.getSecurityToken().empty()) {
    req.headers.emplace("authorization-sts-token",
                        credential_model.getSecurityToken());
  }

  MCO_LOG_DEBUG("Authorization header set.");
}
}  // namespace maxcompute_odbc
