# Azure Service Tags → Analysis Services Firewall Updater

PowerShell script that automatically downloads Microsoft Azure Service Tags, extracts IP ranges related to **Dataflow Gen2** (PowerBI + DataFactory service tags), and updates your **Azure Analysis Services** firewall rules.

## Prerequisites

- **PowerShell 5.1+** or **PowerShell 7+**
- **Az.Accounts** module (auto-installed if missing)
- **Az.AnalysisServices** module (auto-installed if missing)
- Azure RBAC: **Contributor** role on the Analysis Services resource

## Setup

1. Copy the sample config and fill in your Azure details:

```powershell
copy config.json.sample config.json
```

2. Edit `config.json` with your Azure details:

```json
{
    "AnalysisServices": {
        "SubscriptionId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "ResourceGroupName": "my-resource-group",
        "ServerName": "my-aas-server"
    },
    "ServiceTags": {
        "ServiceTagNames": ["PowerBI", "DataFactory"],
        "Regions": []
    },
    "AdditionalIPs": {
        "IpRanges": []
    }
}
```

| Field | Description |
|---|---|
| `SubscriptionId` | Azure subscription ID containing the AAS server |
| `ResourceGroupName` | Resource group name of the AAS server |
| `ServerName` | Analysis Services server name (without `asazure://` prefix) |
| `ServiceTagNames` | Service tags to include. `PowerBI` and `DataFactory` cover Dataflow Gen2 |
| `Regions` | Empty `[]` = all regions. Specify like `["westeurope", "eastus"]` to filter |
| `AdditionalIPs` | Extra IPs/CIDR ranges to always include in the firewall |

## Usage

```powershell
# Basic run (uses config.json in same folder)
.\Update-AASFirewall.ps1

# Custom config path
.\Update-AASFirewall.ps1 -ConfigPath "C:\configs\production.json"

# Dry run — see what would change without applying
.\Update-AASFirewall.ps1 -WhatIf

# Remove all firewall rules from the AAS server
.\Update-AASFirewall.ps1 -RemoveAll

# Dry run of remove all
.\Update-AASFirewall.ps1 -RemoveAll -WhatIf
```

## What the script does

1. **Loads configuration** from `config.json`
2. **Downloads** the latest [Service Tags JSON](https://www.microsoft.com/en-us/download/details.aspx?id=56519) from Microsoft (cached for the day)
3. **Filters** IP ranges for the configured service tags (default: PowerBI + DataFactory)
4. **Converts** CIDR ranges to start/end IP pairs (required by AAS firewall format)
5. **Exports** the IP list to `last_ip_export.txt` for reference
6. **Updates** the Analysis Services firewall with the new rules and enables the PowerBI service flag

## Scheduling

To run this automatically (e.g., weekly), create a Windows Scheduled Task:

```powershell
$action = New-ScheduledTaskAction -Execute "pwsh.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\path\to\Update-AASFirewall.ps1`""
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At "06:00"
Register-ScheduledTask -TaskName "Update-AAS-Firewall" -Action $action -Trigger $trigger -Description "Updates AAS firewall with Dataflow Gen2 IPs"
```

> **Note:** Automated runs require a pre-authenticated Azure context (e.g., via Service Principal or Managed Identity).

## Service Tags Reference

| Service Tag | Covers |
|---|---|
| `PowerBI` | Power BI service, including Dataflow Gen2 compute |
| `DataFactory` | Azure Data Factory / Fabric Data Pipelines (Dataflow Gen2 engine) |

Microsoft updates these IPs periodically. Running this script weekly is recommended.
