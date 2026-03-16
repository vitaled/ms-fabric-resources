# Scripts — On-Premises Data Gateway Installer

Automated PowerShell script to install and register a **Microsoft On-Premises Data Gateway** on a Windows VM.

## InstallAndRegisterGW.ps1

Installs the `DataGateway` PowerShell module, authenticates with a service principal, and either joins an existing gateway cluster or creates a new one.

### Environment Variables

All configuration is passed via environment variables (no config files):

| Variable | Required | Description |
|---|---|---|
| `APPLICATION_ID` | Yes | Service principal (app registration) client ID |
| `TENANT_ID` | Yes | Azure AD tenant ID |
| `CLIENT_SECRET` | Yes | Service principal client secret |
| `GATEWAY_NAME` | Yes | Name of the gateway cluster to create or join |
| `OBJECT_ID` | Yes | AAD Object ID of the user/SP to add as gateway admin |
| `RECOVERY_KEY` | Yes | Recovery key for the gateway cluster |

### What the Script Does

1. Installs the `DataGateway` PowerShell module (if not already present)
2. Authenticates to the Data Gateway service using the service principal
3. Installs the Data Gateway runtime on the machine
4. Checks if a gateway cluster with the given name already exists:
   - **Exists** — Registers as a new member, then removes stale members
   - **Does not exist** — Creates a new gateway cluster
5. Adds the specified user/SP (`OBJECT_ID`) as a gateway admin

### Usage

```powershell
# Set environment variables (e.g. via Azure VM startup script or CI pipeline)
$env:APPLICATION_ID = "<app-id>"
$env:TENANT_ID      = "<tenant-id>"
$env:CLIENT_SECRET   = "<secret>"
$env:GATEWAY_NAME    = "MyGatewayCluster"
$env:OBJECT_ID       = "<aad-object-id>"
$env:RECOVERY_KEY    = "<recovery-key>"

.\InstallAndRegisterGW.ps1
```

### Prerequisites

- **Windows Server** or Windows 10+ (gateway runtime requirement)
- **PowerShell 5.1+**
- Outbound internet access to download the `DataGateway` module and gateway runtime
- A service principal with permissions to manage Data Gateway clusters
