<#
.SYNOPSIS
    Downloads Microsoft Azure Service Tags and updates Analysis Services firewall rules
    with IPs related to Dataflow Gen2 (PowerBI / DataFactory service tags).

.DESCRIPTION
    This script:
    1. Downloads the latest Service Tags JSON from the Microsoft Download Center
    2. Filters IP ranges for Dataflow Gen2 related services (PowerBI, DataFactory)
    3. Updates the Azure Analysis Services firewall with the extracted IP ranges

.PARAMETER ConfigPath
    Path to the JSON configuration file. Defaults to .\config.json

.PARAMETER RemoveAll
    Removes all firewall rules from the Analysis Services server, leaving the firewall empty.

.PARAMETER WhatIf
    Shows what changes would be made without applying them.

.EXAMPLE
    .\Update-AASFirewall.ps1
    .\Update-AASFirewall.ps1 -ConfigPath "C:\configs\myconfig.json"
    .\Update-AASFirewall.ps1 -WhatIf
    .\Update-AASFirewall.ps1 -RemoveAll

.NOTES
    Prerequisites:
    - Az.AnalysisServices PowerShell module
    - Az.Accounts PowerShell module
    - Appropriate Azure RBAC permissions on the Analysis Services resource
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter()]
    [string]$ConfigPath = (Join-Path $PSScriptRoot "config.json"),

    [Parameter()]
    [switch]$RemoveAll
)

#region Logging

function Write-Log {
    param(
        [string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR", "SUCCESS")]
        [string]$Level = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $colors = @{
        INFO    = "Cyan"
        WARN    = "Yellow"
        ERROR   = "Red"
        SUCCESS = "Green"
    }
    Write-Host "[$timestamp] [$Level] $Message" -ForegroundColor $colors[$Level]
}

#endregion

#region Configuration

function Get-ScriptConfiguration {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        Write-Log "Configuration file not found: $Path" -Level ERROR
        Write-Log "Please create a config.json file. See README.md for reference." -Level ERROR
        throw "Configuration file not found: $Path"
    }

    try {
        $config = Get-Content $Path -Raw | ConvertFrom-Json
        # Validate required fields
        $requiredFields = @(
            @{ Path = "AnalysisServices.SubscriptionId"; Value = $config.AnalysisServices.SubscriptionId },
            @{ Path = "AnalysisServices.ResourceGroupName"; Value = $config.AnalysisServices.ResourceGroupName },
            @{ Path = "AnalysisServices.ServerName"; Value = $config.AnalysisServices.ServerName }
        )

        foreach ($field in $requiredFields) {
            if ([string]::IsNullOrWhiteSpace($field.Value) -or $field.Value -like "<*>") {
                Write-Log "Configuration field '$($field.Path)' is not set. Please update config.json." -Level ERROR
                throw "Missing configuration: $($field.Path)"
            }
        }

        Write-Log "Configuration loaded from: $Path" -Level SUCCESS
        return $config
    }
    catch [System.ArgumentException] {
        Write-Log "Invalid JSON in configuration file: $Path" -Level ERROR
        throw
    }
}

#endregion

#region Service Tags Download

function Get-ServiceTagsDownloadUrl {
    <#
    .SYNOPSIS
        Retrieves the latest download URL for the Azure Service Tags JSON file
        from the Microsoft Download Center.
    #>

    Write-Log "Retrieving Service Tags download URL from Microsoft..."

    $downloadPageUrl = "https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519"

    try {
        $response = Invoke-WebRequest -Uri $downloadPageUrl -UseBasicParsing -ErrorAction Stop
        # Extract the direct download link from the confirmation page
        $downloadUrl = ($response.Links | Where-Object {
            $_.href -match "ServiceTags_Public.*\.json$"
        } | Select-Object -First 1).href

        if (-not $downloadUrl) {
            throw "Could not extract download URL from the Microsoft Download Center page."
        }

        Write-Log "Download URL found: $downloadUrl" -Level SUCCESS
        return $downloadUrl
    }
    catch {
        Write-Log "Failed to retrieve download URL: $($_.Exception.Message)" -Level ERROR
        throw
    }
}

function Get-ServiceTagsJson {
    <#
    .SYNOPSIS
        Downloads and parses the Azure Service Tags JSON file.
    #>

    $url = Get-ServiceTagsDownloadUrl
    Write-Log "Downloading Service Tags JSON..."

    try {
        $tempFile = Join-Path $env:TEMP "ServiceTags_Public_$(Get-Date -Format 'yyyyMMdd').json"

        # Use cached file if downloaded today
        if (Test-Path $tempFile) {
            Write-Log "Using cached Service Tags file: $tempFile" -Level INFO
        }
        else {
            Invoke-WebRequest -Uri $url -OutFile $tempFile -UseBasicParsing -ErrorAction Stop
            Write-Log "Service Tags downloaded to: $tempFile" -Level SUCCESS
        }

        $json = Get-Content $tempFile -Raw | ConvertFrom-Json
        Write-Log "Service Tags version: $($json.changeNumber) | Total entries: $($json.values.Count)" -Level INFO
        return $json
    }
    catch {
        Write-Log "Failed to download Service Tags: $($_.Exception.Message)" -Level ERROR
        throw
    }
}

#endregion

#region IP Filtering

function Get-DataflowGen2IpRanges {
    <#
    .SYNOPSIS
        Extracts IP ranges from the Service Tags JSON that are related to Dataflow Gen2.
        Dataflow Gen2 uses PowerBI and DataFactory service tags.
    #>
    param(
        [Parameter(Mandatory)]
        [PSCustomObject]$ServiceTagsJson,

        [string[]]$ServiceTagNames = @("PowerBI", "DataFactory"),

        [string[]]$Regions = @()
    )

    Write-Log "Filtering Service Tags for: $($ServiceTagNames -join ', ')"

    $allIpRanges = @()
    $matchedTags = @()

    foreach ($tagName in $ServiceTagNames) {
        # Build the list of tag IDs to match
        $matchingEntries = $ServiceTagsJson.values | Where-Object {
            # If the tag name already contains a dot (e.g., AzureCloud.westus3), match it directly
            if ($tagName -match '\.') {
                return ($_.name -eq $tagName -or $_.id -eq $tagName)
            }

            if ($Regions.Count -gt 0) {
                # Match region-specific tags like "PowerBI.WestEurope"
                foreach ($region in $Regions) {
                    if ($_.name -eq "$tagName.$region" -or $_.id -eq "$tagName.$region") {
                        return $true
                    }
                }
                return $false
            }
            else {
                # Match the global tag (no region suffix) - contains all regions
                $_.name -eq $tagName -or $_.id -eq $tagName
            }
        }

        foreach ($entry in $matchingEntries) {
            $ipCount = $entry.properties.addressPrefixes.Count
            Write-Log "  Found tag: $($entry.name) - $ipCount IP ranges" -Level INFO
            $allIpRanges += $entry.properties.addressPrefixes
            $matchedTags += $entry.name
        }
    }

    if ($allIpRanges.Count -eq 0) {
        Write-Log "No IP ranges found for the specified service tags and regions." -Level WARN
        return @()
    }

    # Filter to IPv4 only (AAS firewall does not support IPv6)
    $ipv4Ranges = $allIpRanges | Where-Object { $_ -match '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}' } | Sort-Object -Unique

    Write-Log "Total unique IPv4 ranges extracted: $($ipv4Ranges.Count)" -Level SUCCESS
    Write-Log "Matched service tags: $($matchedTags -join ', ')" -Level INFO

    return $ipv4Ranges
}

#endregion

#region Analysis Services Firewall

function Get-AASServerName {
    <#
    .SYNOPSIS
        Extracts the short server name from a full asazure:// URI or returns as-is.
    #>
    param([string]$ServerNameOrUri)

    if ($ServerNameOrUri -match 'asazure://[^/]+/(.+)$') {
        return $Matches[1]
    }
    return $ServerNameOrUri
}

function Update-AASFirewall {
    <#
    .SYNOPSIS
        Updates the Azure Analysis Services firewall rules with the provided IP ranges.
    #>
    param(
        [Parameter(Mandatory)]
        [string]$SubscriptionId,

        [Parameter(Mandatory)]
        [string]$ResourceGroupName,

        [Parameter(Mandatory)]
        [string]$ServerName,

        [Parameter(Mandatory)]
        [string[]]$IpRanges,

        [switch]$WhatIfMode
    )

    Write-Log "Connecting to Azure..."

    # Ensure Az modules are available
    $requiredModules = @("Az.Accounts", "Az.AnalysisServices")
    foreach ($mod in $requiredModules) {
        if (-not (Get-Module -ListAvailable -Name $mod)) {
            Write-Log "Required module '$mod' is not installed. Installing..." -Level WARN
            Install-Module -Name $mod -Force -AllowClobber -Scope CurrentUser
        }
    }

    Import-Module Az.Accounts -ErrorAction Stop
    Import-Module Az.AnalysisServices -ErrorAction Stop

    # Connect to Azure (will prompt if not already connected)
    # Use -WhatIf:$false to ensure auth calls are not suppressed by ShouldProcess
    try {
        $context = Get-AzContext
        if (-not $context) {
            Write-Log "Not logged in. Initiating Azure login via device code..." -Level WARN
            Write-Log "A browser URL and code will be displayed. Open the URL and enter the code to authenticate." -Level WARN
            Connect-AzAccount -SubscriptionId $SubscriptionId -DeviceCode -ErrorAction Stop -WhatIf:$false | Out-Null
            $context = Get-AzContext
        }
        if ($context.Subscription.Id -ne $SubscriptionId) {
            Write-Log "Setting Azure context to subscription: $SubscriptionId"
            Set-AzContext -SubscriptionId $SubscriptionId -ErrorAction Stop -WhatIf:$false | Out-Null
        }
        $context = Get-AzContext
        Write-Log "Azure context: $($context.Account.Id) - Subscription: $($context.Subscription.Name)" -Level SUCCESS
    }
    catch {
        Write-Log "Failed to authenticate to Azure: $($_.Exception.Message)" -Level ERROR
        throw
    }

    # Extract short server name from URI if needed
    $shortName = Get-AASServerName -ServerNameOrUri $ServerName
    Write-Log "AAS server short name: $shortName" -Level INFO

    # Get current AAS server
    Write-Log "Retrieving Analysis Services server: $shortName in $ResourceGroupName..."
    try {
        $server = Get-AzAnalysisServicesServer -ResourceGroupName $ResourceGroupName -Name $shortName -ErrorAction Stop
    }
    catch {
        Write-Log "Failed to retrieve AAS server: $($_.Exception.Message)" -Level ERROR
        throw
    }

    # Build firewall rules from IP ranges
    Write-Log "Building firewall rules from $($IpRanges.Count) IP ranges..."

    $firewallRules = [System.Collections.Generic.List[Microsoft.Azure.Commands.AnalysisServices.Models.PsAzureAnalysisServicesFirewallRule]]::new()

    $ruleIndex = 0
    foreach ($ipRange in $IpRanges) {
        $ruleIndex++

        if ($ipRange -match '/') {
            # CIDR notation - convert to start/end range
            $network = $ipRange.Split('/')[0]
            $prefix  = [int]$ipRange.Split('/')[1]

            $ipBytes = [System.Net.IPAddress]::Parse($network).GetAddressBytes()
            [Array]::Reverse($ipBytes)
            $ipInt = [BitConverter]::ToUInt32($ipBytes, 0)

            $mask      = ([Math]::Pow(2, 32) - [Math]::Pow(2, (32 - $prefix)))
            $startInt  = [UInt32]($ipInt -band $mask)
            $endInt    = [UInt32]($startInt + [Math]::Pow(2, (32 - $prefix)) - 1)

            $startBytes = [BitConverter]::GetBytes($startInt)
            [Array]::Reverse($startBytes)
            $rangeStart = ([System.Net.IPAddress]::new($startBytes)).ToString()

            $endBytes = [BitConverter]::GetBytes($endInt)
            [Array]::Reverse($endBytes)
            $rangeEnd = ([System.Net.IPAddress]::new($endBytes)).ToString()
        }
        else {
            # Single IP
            $rangeStart = $ipRange
            $rangeEnd   = $ipRange
        }

        $rule = [Microsoft.Azure.Commands.AnalysisServices.Models.PsAzureAnalysisServicesFirewallRule]::new(
            "DataflowGen2_$ruleIndex",
            $rangeStart,
            $rangeEnd
        )

        $firewallRules.Add($rule)
    }

    # Build the firewall config
    $firewallConfig = [Microsoft.Azure.Commands.AnalysisServices.Models.PsAzureAnalysisServicesFirewallConfig]::new(
        $true,
        $firewallRules
    )

    Write-Log "Firewall rules built: $($firewallRules.Count) rules" -Level SUCCESS

    # Show current state
    if ($server.FirewallConfig) {
        $currentRuleCount = $server.FirewallConfig.FirewallRules.Count
        Write-Log "Current firewall: $currentRuleCount rules, PowerBI enabled: $($server.FirewallConfig.EnablePowerBIService)" -Level INFO
    }
    else {
        Write-Log "Current firewall: No rules configured" -Level INFO
    }

    # Apply the firewall rules
    if ($WhatIfMode) {
        Write-Log "[WhatIf] Would update AAS server '$shortName' with $($firewallRules.Count) firewall rules." -Level WARN
        Write-Log "[WhatIf] First 5 rules:" -Level WARN
        $firewallRules | Select-Object -First 5 | ForEach-Object {
            Write-Log "  $($_.FirewallRuleName): $($_.RangeStart) - $($_.RangeEnd)" -Level WARN
        }
        return
    }

    Write-Log "Updating Analysis Services firewall (this may take a moment)..."
    try {
        Set-AzAnalysisServicesServer `
            -ResourceGroupName $ResourceGroupName `
            -Name $shortName `
            -FirewallConfig $firewallConfig `
            -ErrorAction Stop | Out-Null

        Write-Log "Analysis Services firewall updated successfully with $($firewallRules.Count) rules!" -Level SUCCESS
    }
    catch {
        Write-Log "Failed to update firewall: $($_.Exception.Message)" -Level ERROR
        throw
    }
}

function Remove-AASFirewallRules {
    <#
    .SYNOPSIS
        Removes all firewall rules from the Azure Analysis Services server.
    #>
    param(
        [Parameter(Mandatory)]
        [string]$SubscriptionId,

        [Parameter(Mandatory)]
        [string]$ResourceGroupName,

        [Parameter(Mandatory)]
        [string]$ServerName,

        [switch]$WhatIfMode
    )

    Write-Log "Connecting to Azure..."

    # Ensure Az modules are available
    $requiredModules = @("Az.Accounts", "Az.AnalysisServices")
    foreach ($mod in $requiredModules) {
        if (-not (Get-Module -ListAvailable -Name $mod)) {
            Write-Log "Required module '$mod' is not installed. Installing..." -Level WARN
            Install-Module -Name $mod -Force -AllowClobber -Scope CurrentUser
        }
    }

    Import-Module Az.Accounts -ErrorAction Stop
    Import-Module Az.AnalysisServices -ErrorAction Stop

    # Connect to Azure
    try {
        $context = Get-AzContext
        if (-not $context) {
            Write-Log "Not logged in. Initiating Azure login via device code..." -Level WARN
            Write-Log "A browser URL and code will be displayed. Open the URL and enter the code to authenticate." -Level WARN
            Connect-AzAccount -SubscriptionId $SubscriptionId -DeviceCode -ErrorAction Stop -WhatIf:$false | Out-Null
            $context = Get-AzContext
        }
        if ($context.Subscription.Id -ne $SubscriptionId) {
            Write-Log "Setting Azure context to subscription: $SubscriptionId"
            Set-AzContext -SubscriptionId $SubscriptionId -ErrorAction Stop -WhatIf:$false | Out-Null
        }
        $context = Get-AzContext
        Write-Log "Azure context: $($context.Account.Id) - Subscription: $($context.Subscription.Name)" -Level SUCCESS
    }
    catch {
        Write-Log "Failed to authenticate to Azure: $($_.Exception.Message)" -Level ERROR
        throw
    }

    # Extract short server name from URI if needed
    $shortName = Get-AASServerName -ServerNameOrUri $ServerName
    Write-Log "AAS server short name: $shortName" -Level INFO

    # Get current AAS server
    Write-Log "Retrieving Analysis Services server: $shortName in $ResourceGroupName..."
    try {
        $server = Get-AzAnalysisServicesServer -ResourceGroupName $ResourceGroupName -Name $shortName -ErrorAction Stop
    }
    catch {
        Write-Log "Failed to retrieve AAS server: $($_.Exception.Message)" -Level ERROR
        throw
    }

    # Show current state
    if ($server.FirewallConfig -and $server.FirewallConfig.FirewallRules.Count -gt 0) {
        $currentRuleCount = $server.FirewallConfig.FirewallRules.Count
        Write-Log "Current firewall: $currentRuleCount rules" -Level INFO
    }
    else {
        Write-Log "Firewall already has no rules. Nothing to remove." -Level WARN
        return
    }

    # Build empty firewall config (enabled but with no rules)
    $emptyRules = [System.Collections.Generic.List[Microsoft.Azure.Commands.AnalysisServices.Models.PsAzureAnalysisServicesFirewallRule]]::new()
    $firewallConfig = [Microsoft.Azure.Commands.AnalysisServices.Models.PsAzureAnalysisServicesFirewallConfig]::new(
        $false,
        $emptyRules
    )

    if ($WhatIfMode) {
        Write-Log "[WhatIf] Would remove all $currentRuleCount firewall rules from AAS server '$shortName'." -Level WARN
        return
    }

    Write-Log "Removing all firewall rules from Analysis Services (this may take a moment)..."
    try {
        Set-AzAnalysisServicesServer `
            -ResourceGroupName $ResourceGroupName `
            -Name $shortName `
            -FirewallConfig $firewallConfig `
            -ErrorAction Stop | Out-Null

        Write-Log "All firewall rules removed successfully from AAS server '$shortName'!" -Level SUCCESS
    }
    catch {
        Write-Log "Failed to remove firewall rules: $($_.Exception.Message)" -Level ERROR
        throw
    }
}

#endregion

#region Main Execution

try {
    Write-Log "=============================================" -Level INFO
    Write-Log " Azure Service Tags -> AAS Firewall Updater" -Level INFO
    Write-Log "=============================================" -Level INFO

    # 1. Load configuration
    $config = Get-ScriptConfiguration -Path $ConfigPath
    # Handle -RemoveAll: remove all firewall rules and exit
    if ($RemoveAll) {
        Write-Log "RemoveAll flag set - removing all firewall rules..." -Level WARN
        Remove-AASFirewallRules `
            -SubscriptionId $config.AnalysisServices.SubscriptionId `
            -ResourceGroupName $config.AnalysisServices.ResourceGroupName `
            -ServerName $config.AnalysisServices.ServerName `
            -WhatIfMode:$WhatIfPreference

        Write-Log "============================================="  -Level INFO
        Write-Log " Script completed successfully!" -Level SUCCESS
        Write-Log "============================================="  -Level INFO
        exit 0
    }
    # 2. Download Service Tags
    $serviceTags = Get-ServiceTagsJson

    # 3. Extract Dataflow Gen2 IP ranges
    $serviceTagNames = @("PowerBI", "DataFactory")
    if ($config.ServiceTags.ServiceTagNames) {
        $serviceTagNames = @($config.ServiceTags.ServiceTagNames)
    }

    $regions = @()
    if ($config.ServiceTags.Regions -and $config.ServiceTags.Regions.Count -gt 0) {
        $regions = @($config.ServiceTags.Regions)
    }

    $ipRanges = Get-DataflowGen2IpRanges `
        -ServiceTagsJson $serviceTags `
        -ServiceTagNames $serviceTagNames `
        -Regions $regions

    # 4. Add any additional manually configured IPs
    if ($config.AdditionalIPs.IpRanges -and $config.AdditionalIPs.IpRanges.Count -gt 0) {
        Write-Log "Adding $($config.AdditionalIPs.IpRanges.Count) additional IP ranges from config..." -Level INFO
        $ipRanges = @($ipRanges) + @($config.AdditionalIPs.IpRanges) | Sort-Object -Unique
    }

    if ($ipRanges.Count -eq 0) {
        Write-Log "No IP ranges found. Nothing to update." -Level WARN
        exit 0
    }

    # 5. Export IP list for reference
    $exportPath = Join-Path $PSScriptRoot "last_ip_export.txt"
    $header1 = "# Dataflow Gen2 IP Ranges - Exported " + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
    $header2 = "# Service Tags: " + ($serviceTagNames -join ', ')
    $header3 = "# Total IPv4 ranges: " + $ipRanges.Count
    $exportContent = @($header1, $header2, $header3, "") + $ipRanges
    $exportContent | Set-Content -Path $exportPath -Force -WhatIf:$false
    Write-Log "IP ranges exported to: $exportPath" -Level INFO

    # 6. Update AAS firewall
    Update-AASFirewall `
        -SubscriptionId $config.AnalysisServices.SubscriptionId `
        -ResourceGroupName $config.AnalysisServices.ResourceGroupName `
        -ServerName $config.AnalysisServices.ServerName `
        -IpRanges $ipRanges `
        -WhatIfMode:$WhatIfPreference

    Write-Log "=============================================" -Level INFO
    Write-Log " Script completed successfully!" -Level SUCCESS
    Write-Log "=============================================" -Level INFO
}
catch {
    Write-Log "Script failed: $($_.Exception.Message)" -Level ERROR
    Write-Log $_.ScriptStackTrace -Level ERROR
    exit 1
}

#endregion
