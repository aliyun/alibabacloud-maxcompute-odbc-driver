# PowerShell script to uninstall MaxCompute ODBC Driver
# This script automates the removal of the MaxCompute ODBC Driver

param(
    [switch]$Silent = $false,
    [switch]$Force = $false,
    [string]$MsiPath = "MaxCompute_ODBC_Driver.msi"
)

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"

    if ($Level -eq "ERROR") {
        Write-Host $logMessage -ForegroundColor Red
    } elseif ($Level -eq "WARNING") {
        Write-Host $logMessage -ForegroundColor Yellow
    } elseif ($Level -eq "SUCCESS") {
        Write-Host $logMessage -ForegroundColor Green
    } else {
        Write-Host $logMessage -ForegroundColor Cyan
    }
}

function Find-InstalledOdbcDriver {
    # Check if the driver is installed via Windows registry
    $uninstallKeys = @(
        "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*",
        "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*"
    )

    foreach ($key in $uninstallKeys) {
        try {
            $items = Get-ChildItem -Path $key -ErrorAction SilentlyContinue
            foreach ($item in $items) {
                $displayName = (Get-ItemProperty -Path $item.PSPath -ErrorAction SilentlyContinue).DisplayName
                if ($displayName -and $displayName -like "*MaxCompute ODBC Driver*") {
                    return @{
                        DisplayName = $displayName
                        ProductCode = $item.PSChildName
                        UninstallString = (Get-ItemProperty -Path $item.PSPath -ErrorAction SilentlyContinue).UninstallString
                    }
                }
            }
        } catch {
            # Continue to next key if current key is not accessible
            continue
        }
    }

    return $null
}

function Uninstall-ByProductCode {
    param([string]$ProductCode)

    Write-Log "Attempting to uninstall using Product Code: $ProductCode"

    try {
        $arguments = "/x $ProductCode /quiet /norestart"

        if ($Silent) {
            $arguments += " /qn"
        } else {
            $arguments += " /qb"
        }

        if ($Force) {
            $arguments += " /forcerestart"
        }

        # Execute msiexec with proper error handling
        $result = Start-Process -FilePath "msiexec.exe" -ArgumentList $arguments -Wait -NoNewWindow -PassThru

        if ($result.ExitCode -eq 0) {
            Write-Log "Successfully uninstalled MaxCompute ODBC Driver" "SUCCESS"
            return $true
        } else {
            Write-Log "Uninstall failed with exit code: $($result.ExitCode)" "ERROR"
            return $false
        }
    } catch {
        Write-Log "Failed to execute uninstall command: $_" "ERROR"
        return $false
    }
}

Write-Log "Starting MaxCompute ODBC Driver uninstallation"

# Find if the driver is already installed
$installedDriver = Find-InstalledOdbcDriver

if ($installedDriver) {
    Write-Log "Found installed MaxCompute ODBC Driver: $($installedDriver.DisplayName)"
    Write-Log "Product Code: $($installedDriver.ProductCode)"

    if (-not $Force) {
        if (-not $Silent) {
            $confirmation = Read-Host "Do you want to uninstall this driver? (Y/N)"
            if ($confirmation -ne 'Y' -and $confirmation -ne 'y') {
                Write-Log "Uninstallation cancelled by user"
                exit 0
            }
        }
    }

    # Attempt uninstall using product code
    $result = Uninstall-ByProductCode -ProductCode $installedDriver.ProductCode

    if ($result) {
        Write-Log "Uninstallation completed successfully" "SUCCESS"
    } else {
        Write-Log "Uninstallation failed" "ERROR"
        exit 1
    }
} else {
    Write-Log "MaxCompute ODBC Driver not found in installed programs" "WARNING"

    # Check if the MSI file exists in the specified path
    if (Test-Path $MsiPath) {
        Write-Log "Found MSI file at $MsiPath, attempting uninstall with MSI file"

        if (-not $Force) {
            if (-not $Silent) {
                $confirmation = Read-Host "MSI file found. Do you want to try uninstalling using the MSI? (Y/N)"
                if ($confirmation -ne 'Y' -and $confirmation -ne 'y') {
                    Write-Log "Uninstallation cancelled by user"
                    exit 0
                }
            }
        }

        try {
            $arguments = "/x `"$MsiPath`""

            if ($Silent) {
                $arguments += " /quiet"
            } else {
                $arguments += " /qb"
            }

            if ($Force) {
                $arguments += " /forcerestart"
            }

            $result = Start-Process -FilePath "msiexec.exe" -ArgumentList $arguments -Wait -NoNewWindow -PassThru

            if ($result.ExitCode -eq 0) {
                Write-Log "Successfully uninstalled MaxCompute ODBC Driver using MSI file" "SUCCESS"
            } else {
                Write-Log "Uninstall failed with exit code: $($result.ExitCode)" "ERROR"
                exit 1
            }
        } catch {
            Write-Log "Failed to execute uninstall command: $_" "ERROR"
            exit 1
        }
    } else {
        Write-Log "MSI file not found at $MsiPath. Cannot proceed with uninstallation." "ERROR"
        Write-Log "This driver may have been uninstalled already or installed differently." "WARNING"
        exit 1
    }
}

Write-Log "Uninstallation process completed"