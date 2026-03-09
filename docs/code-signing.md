# Windows Code Signing Setup Guide

## Problem

Windows SmartScreen shows "This file is not commonly downloaded and may be dangerous" when users download the MSI installer or DLL files from GitHub Releases. This warning appears because the files are not digitally signed by a trusted publisher.

## Solution

Sign the driver DLL and MSI installer with a code signing certificate. This eliminates the SmartScreen warning and verifies the files have not been tampered with.

## Recommended: Azure Trusted Signing

Azure Trusted Signing (formerly Azure Code Signing) is Microsoft's official cloud-based signing service. It is the recommended approach because:

- **Immediate SmartScreen trust** -- Signed files are trusted by SmartScreen from day one
- **Low cost** -- ~$10/month (vs $300-600/year for traditional EV certificates)
- **CI/CD native** -- Official GitHub Action available, no USB tokens needed
- **HSM-backed** -- Meets the 2023 CA/Browser Forum requirement for hardware key storage
- **No key management burden** -- Microsoft manages the HSM infrastructure

### Alternative Options

| Option | Cost | SmartScreen | CI/CD Friendly | Notes |
|--------|------|-------------|----------------|-------|
| Azure Trusted Signing | ~$10/month | Immediate trust | Yes (GitHub Action) | Recommended |
| DigiCert EV + KeyLocker | ~$500/year | Immediate trust | Yes (cloud HSM) | Established vendor |
| SSL.com EV + eSigner | ~$350/year | Immediate trust | Yes (cloud API) | Budget alternative |
| Sectigo/Comodo OV | ~$200/year | Trust after reputation builds | Partial | Cheaper but slow SmartScreen trust |
| Self-signed | $0 | No trust | Yes | Only for development/testing |

> **Note**: Since 2023, EV code signing certificates require private keys stored in hardware security modules (HSM). This means `.pfx` files cannot be used directly in CI -- a cloud HSM service is required.

## Setup Instructions (Azure Trusted Signing)

### Step 1: Create Azure Resources

1. Sign in to [Azure Portal](https://portal.azure.com)
2. Create a **Trusted Signing Account**:
   - Search for "Trusted Signing Accounts" in the portal
   - Click "Create"
   - Select a subscription, resource group, and region
   - Choose the account name (e.g., `maxcompute-odbc-signing`)
3. Create a **Certificate Profile**:
   - In your Trusted Signing Account, go to "Certificate Profiles"
   - Click "Add" and select the profile type:
     - **Public Trust** -- for production releases (requires identity validation)
     - **Public Trust Test** -- for testing the signing pipeline
   - Complete identity validation (requires organization verification documents)

### Step 2: Create an Azure AD App Registration

1. Go to "App registrations" in Azure Portal
2. Click "New registration", name it `maxcompute-odbc-ci-signing`
3. After creation, note the **Application (client) ID** and **Directory (tenant) ID**
4. Go to "Certificates & secrets" > "Client secrets" > "New client secret"
5. Note the **secret value** (you won't see it again)
6. Assign the **Trusted Signing Certificate Profile Signer** role:
   - Go to your Trusted Signing Account > "Access control (IAM)"
   - Click "Add role assignment"
   - Select "Trusted Signing Certificate Profile Signer"
   - Assign to your app registration

### Step 3: Configure GitHub Repository Secrets

In your GitHub repository, go to Settings > Secrets and variables > Actions, and add:

| Secret Name | Value |
|-------------|-------|
| `AZURE_TENANT_ID` | Azure AD Directory (tenant) ID |
| `AZURE_CLIENT_ID` | App registration Application (client) ID |
| `AZURE_CLIENT_SECRET` | App registration client secret value |
| `AZURE_CODE_SIGNING_ACCOUNT` | Trusted Signing Account name |
| `AZURE_CERTIFICATE_PROFILE` | Certificate Profile name |

### Step 4: Verify

Once secrets are configured, the release workflow will automatically sign files:

1. Push a version tag: `git tag v1.0.1 && git push origin v1.0.1`
2. The `build-windows` job will:
   - Build the driver DLL and MSI
   - Sign both using Azure Trusted Signing
   - Upload signed artifacts
3. The `create-release` job will publish signed files to GitHub Releases

### Step 5: Validate the Signature

After downloading a signed MSI:

1. Right-click the file > Properties > Digital Signatures tab
2. You should see "Alibaba Cloud" (or your organization name) as the signer
3. Click "Details" > "View Certificate" to verify the certificate chain

Or via command line:

```powershell
# Check signature
Get-AuthenticodeSignature .\MaxCompute_ODBC_Driver_1.0.0.msi

# Expected output:
# SignerCertificate: [Thumbprint]  CN=Alibaba Cloud, O=Alibaba Cloud Computing Ltd.
# Status: Valid
```

## Signing Without Azure (Alternative)

If you prefer a traditional code signing certificate (e.g., from DigiCert, SSL.com):

### Using DigiCert KeyLocker in GitHub Actions

```yaml
- name: Sign with DigiCert KeyLocker
  env:
    SM_HOST: ${{ secrets.SM_HOST }}
    SM_API_KEY: ${{ secrets.SM_API_KEY }}
    SM_CLIENT_CERT_FILE_B64: ${{ secrets.SM_CLIENT_CERT_FILE_B64 }}
    SM_CLIENT_CERT_PASSWORD: ${{ secrets.SM_CLIENT_CERT_PASSWORD }}
    SM_CERT_ALIAS: ${{ secrets.SM_CERT_ALIAS }}
  run: |
    # Install smctl
    curl -L -o smtools.msi https://one.digicert.com/signingmanager/api-ui/v1/releases/smtools-windows-x64.msi
    msiexec /i smtools.msi /quiet

    # Configure
    echo "$SM_CLIENT_CERT_FILE_B64" | base64 -d > /d/Certificate_pkcs12.p12
    smctl credentials save $SM_API_KEY $SM_CLIENT_CERT_PASSWORD

    # Sign
    smctl sign --fingerprint $SM_CERT_ALIAS --input MaxCompute_ODBC_Driver_*.msi
    smctl sign --fingerprint $SM_CERT_ALIAS --input build_win64\bin\maxcompute_odbc.dll
```

## Troubleshooting

### "Signing step skipped"

The signing step uses `if: env.AZURE_CODE_SIGNING_ACCOUNT != ''` to check if secrets are configured. If signing is skipped:
- Verify all 5 secrets are set in GitHub repository settings
- Secrets are not available in pull request builds from forks (by design)

### "Access denied" during signing

- Verify the app registration has the "Trusted Signing Certificate Profile Signer" role
- Check that the certificate profile identity validation is complete (status: "Verified")

### SmartScreen still shows warning after signing

- If using an OV (non-EV) certificate, SmartScreen reputation takes time to build
- Azure Trusted Signing and EV certificates should provide immediate trust
- Ensure both the DLL and MSI are signed (unsigned DLLs inside a signed MSI can still trigger warnings)
