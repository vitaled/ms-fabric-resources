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
$objectId = $env:OBJECT_ID  # Added Object ID

# Validate mandatory environment variables
if (-not $applicationId) {
    Write-Log "ERROR: APPLICATION_ID environment variable is missing" "ERROR"
    Exit 1
}

if (-not $tenantId) {
    Write-Log "ERROR: TENANT_ID environment variable is missing" "ERROR"
    Exit 1
}

if (-not $gatewayName) {
    Write-Log "ERROR: GATEWAY_NAME environment variable is missing" "ERROR"
    Exit 1
}

if (-not $objectId) {
    Write-Log "ERROR: OBJECT_ID environment variable is missing" "ERROR"
    Exit 1
}

# Retrieve the client secret and recovery key from environment variables
$clientSecret = $env:CLIENT_SECRET
$recoveryKey = $env:RECOVERY_KEY

if (-not $clientSecret) {
    Write-Log "ERROR: CLIENT_SECRET environment variable is missing" "ERROR"
    Exit 1
}

if (-not $recoveryKey) {
    Write-Log "ERROR: RECOVERY_KEY environment variable is missing" "ERROR"
    Exit 1
}

# Secure the credentials
$securePassword = ConvertTo-SecureString -String $clientSecret -AsPlainText -Force
$secureRecoveryKey = ConvertTo-SecureString -String $recoveryKey -AsPlainText -Force

# Start the script with logging
Write-Log "Starting the script execution"

# Check if the DataGateway module is installed before attempting installation
if (-not (Get-Module -Name DataGateway -ListAvailable)) {
    Try {
        Write-Log "Installing DataGateway module..."
        Install-Module -Name DataGateway -Force -ErrorAction Stop
        Write-Log "DataGateway module installed successfully."
    } Catch {
        Write-Log "Error installing DataGateway module: $_" "ERROR"
        Exit 1
    }
} else {
    Write-Log "DataGateway module is already installed."
}

# Import the module to ensure it is loaded
Import-Module DataGateway -ErrorAction Stop

# Connect to Data Gateway Service Account
Try {
    Write-Log "Connecting to Data Gateway service account..."
    Connect-DataGatewayServiceAccount -ApplicationId $applicationId -ClientSecret $securePassword -Tenant $tenantId -ErrorAction Stop
    Write-Log "Connected to Data Gateway service account successfully."
} Catch {
    Write-Log "Error connecting to Data Gateway service account: $_" "ERROR"
    Exit 1
}

# Install the Data Gateway
Try {
    Write-Log "Installing Data Gateway..."
    Install-DataGateway -Accept -ErrorAction Stop
    Write-Log "Data Gateway installed successfully."
} Catch {
    Write-Log "Error installing Data Gateway: $_" "ERROR"
    Exit 1
}

# Check if a gateway with the given name already exists
Try {
    Write-Log "Checking for existing Data Gateway with name: $gatewayName..."
    $existingGateway = Get-DataGatewayCluster | Where-Object { $_.Name -eq $gatewayName }

    if ($existingGateway) {
        Write-Log "A gateway with the name '$gatewayName' already exists. Registering to the existing gateway."
    } else {
        Write-Log "No existing gateway found. Proceeding with new gateway creation."
        
        # Add Data Gateway Cluster
        Try {
            Write-Log "Adding Data Gateway Cluster with name: $gatewayName..."
            Add-DataGatewayCluster -Name $gatewayName -RecoveryKey $secureRecoveryKey -OverwriteExistingGateway -ErrorAction Stop
            Write-Log "Data Gateway Cluster added successfully."
            
            # Re-fetch the cluster details
            $existingGateway = Get-DataGatewayCluster | Where-Object { $_.Name -eq $gatewayName }

            if (-not $existingGateway) {
                Write-Log "ERROR: Failed to retrieve newly created Data Gateway Cluster." "ERROR"
                Exit 1
            }
        } Catch {
            Write-Log "Error adding Data Gateway Cluster: $_" "ERROR"
            Exit 1
        }
    }
} Catch {
    Write-Log "Error checking existing gateways: $_" "ERROR"
    Exit 1
}

# Assign user to the gateway
if ($objectId) {
    Try {
        Write-Log "Adding user with Object ID: $objectId as an Admin to the Data Gateway Cluster..."
        Add-DataGatewayClusterUser -GatewayClusterId $existingGateway.Id -PrincipalObjectId $objectId -Role Admin -ErrorAction Stop
        Write-Log "Successfully added user with Object ID: $objectId to the Data Gateway Cluster."
    } Catch {
        Write-Log "Error adding user to Data Gateway Cluster: $_" "ERROR"
        Exit 1
    }
} else {
    Write-Log "Skipping user assignment as Object ID is missing."
}

Write-Log "Script execution completed successfully."
