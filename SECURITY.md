# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in the MaxCompute ODBC Driver, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please report security issues by emailing the maintainers or by using [GitHub's private vulnerability reporting](https://github.com/aliyun/maxcompute-odbc-driver/security/advisories/new).

### What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Timeline

- We will acknowledge receipt within 3 business days.
- We will provide an initial assessment within 10 business days.
- We will work with you to understand and address the issue before any public disclosure.

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | Yes       |

## Security Best Practices for Users

- **Never hardcode credentials** in connection strings or source code. Use environment variables instead:
  ```bash
  export ALIBABA_CLOUD_ACCESS_KEY_ID="your_key_id"
  export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_key_secret"
  ```
- Keep the driver updated to the latest version.
- Use HTTPS endpoints for MaxCompute connections.
- Configure appropriate file permissions for log files (`MCO_LOG_FILE`), as they may contain query text.
