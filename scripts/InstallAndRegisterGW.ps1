# Logging function
function Write-Log {
    param (
        [string]$message,
        [string]$logLevel = "INFO"
    )
    
    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $logMessage = "$timestamp [$logLevel] - $message"
    
    # Write to console (can also write to a file or event log if needed)
    Write-Host $logMessage
}

# Retrieve credentials from environment variables (set in the VM)
$applicationId = $env:APPLICATION_ID
$tenantId = $env:TENANT_ID
$gatewayName = $env:GATEWAY_NAME

# If credentials are missing, log an error and exit
if (-not $applicationId) {
    Write-Log "ERROR: APPLICATION_ID environment variable is missing" "ERROR"
    Exit
}

if (-not $tenantId) {
    Write-Log "ERROR: TENANT_ID environment variable is missing" "ERROR"
    Exit
}

# Retrieve the client secret and recovery key from environment variables
$clientSecret = $env:CLIENT_SECRET
$recoveryKey = $env:RECOVERY_KEY

# If credentials are missing, log an error and exit
if (-not $clientSecret) {
    Write-Log "ERROR: CLIENT_SECRET environment variable is missing" "ERROR"
    Exit
}

if (-not $recoveryKey) {
    Write-Log "ERROR: RECOVERY_KEY environment variable is missing" "ERROR"
    Exit
}

# Secure the credentials (convert the plaintext to SecureString)
$securePassword = ConvertTo-SecureString -String $clientSecret -AsPlainText -Force
$secureRecoveryKey = ConvertTo-SecureString -String $recoveryKey -AsPlainText -Force

# Start the script with logging
Write-Log "Starting the script execution"

# Install necessary modules
Try {
    Write-Log "Installing DataGateway module"
    Install-Module -Name DataGateway -Force -ErrorAction Stop
} Catch {
    Write-Log "Error installing DataGateway module: $_" "ERROR"
    Exit
}

# Connect to Data Gateway Service Account
Try {
    Write-Log "Connecting to Data Gateway service account"
    Connect-DataGatewayServiceAccount -ApplicationId $applicationId -ClientSecret $securePassword -Tenant $tenantId -ErrorAction Stop
} Catch {
    Write-Log "Error connecting to Data Gateway service account: $_" "ERROR"
    Exit
}

# Install the Data Gateway
Try {
    Write-Log "Installing Data Gateway"
    Install-DataGateway -Accept -ErrorAction Stop
} Catch {
    Write-Log "Error installing Data Gateway: $_" "ERROR"
    Exit
}

# Add Data Gateway Cluster
Try {
    Write-Log "Adding Data Gateway Cluster with name: $gatewayName"
    Add-DataGatewayCluster -Name $gatewayName -RecoveryKey $secureRecoveryKey -OverwriteExistingGateway -ErrorAction Stop
    Write-Log "Data Gateway Cluster added successfully"
} Catch {
    Write-Log "Error adding Data Gateway Cluster: $_" "ERROR"
    Exit
}

Write-Log "Script execution completed successfully"
