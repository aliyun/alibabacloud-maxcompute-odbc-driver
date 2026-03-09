#pragma once

#include <string>

namespace maxcompute_odbc {

/**
 * @brief ICredentials interface.
 * Defines the basic contract for credential data.
 */
class ICredentials {
 public:
  // A virtual destructor is CRUCIAL for any base class with virtual functions.
  // It ensures that derived class destructors are called correctly.
  virtual ~ICredentials() = default;

  virtual std::string getAccessKeyId() const = 0;
  virtual std::string getAccessKeySecret() const = 0;
  virtual std::string getSecurityToken() const = 0;
};

/**
 * @brief C++ equivalent of ICredentialsProvider.
 * Note: AutoCloseable/close() is handled by the C++ destructor (RAII).
 */
class ICredentialsProvider {
 public:
  virtual ~ICredentialsProvider() = default;

  // Return a reference to the credentials.
  // The provider maintains ownership of the credentials object.
  virtual ICredentials &getCredentials() = 0;
};

/**
 * @brief A concrete implementation of ICredentials holding static keys.
 */
class Credentials : public ICredentials {
 public:
  // Constructor
  Credentials(const std::string &accessKeyId,
              const std::string &accessKeySecret,
              const std::string &securityToken = "")
      : accessKeyId_(accessKeyId),
        accessKeySecret_(accessKeySecret),
        securityToken_(securityToken) {}

  // Override methods from the interface
  std::string getAccessKeyId() const override { return accessKeyId_; }

  std::string getAccessKeySecret() const override { return accessKeySecret_; }

  std::string getSecurityToken() const override { return securityToken_; }

 private:
  std::string accessKeyId_;
  std::string accessKeySecret_;
  std::string securityToken_;
};

class StaticCredentialProvider : public ICredentialsProvider {
 public:
  // Constructor from AK/SK
  StaticCredentialProvider(const std::string &accessKeyId,
                           const std::string &accessKeySecret)
      : credentials_(accessKeyId, accessKeySecret, "") {}

  StaticCredentialProvider(const std::string &accessKeyId,
                           const std::string &accessKeySecret,
                           const std::string &securityToken)
      : credentials_(accessKeyId, accessKeySecret, securityToken) {}

  // Constructor from an existing Credentials object (copy)
  explicit StaticCredentialProvider(const Credentials &credentials)
      : credentials_(credentials) {}

  // Constructor from an existing Credentials object (move)
  explicit StaticCredentialProvider(Credentials &&credentials)
      : credentials_(std::move(credentials)) {}

  // Static factory method 'of'
  static StaticCredentialProvider of(const std::string &accessKeyId,
                                     const std::string &accessKeySecret) {
    return StaticCredentialProvider(accessKeyId, accessKeySecret);
  }

  // Override methods from the interface
  ICredentials &getCredentials() override { return credentials_; }

  // The destructor is implicitly defined and does nothing,
  // which matches the empty close() method in Java.
  ~StaticCredentialProvider() override = default;

 private:
  Credentials credentials_;
};

}  // namespace maxcompute_odbc
