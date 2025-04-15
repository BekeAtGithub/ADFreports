# Script to generate a CSV report of all pipelines in an ADF factory using pure REST API
# This script doesn't depend on Az modules at all
# how to use:  - UPDATE CLIENT ID on line 39
# .\ADFreports.ps1 -ResourceGroupName "RG" -DataFactoryName "ADF" -TenantId "12345678-abcdefg-tenantid" -SubscriptionId "12345678-abcdefg-subid" -DaysToCheck 30

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$DataFactoryName,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = "$($env:USERPROFILE)\Documents\ADF_Report.csv",
    
    [Parameter(Mandatory=$false)]
    [string]$TenantId,
    
    [Parameter(Mandatory=$false)]
    [string]$SubscriptionId,
    
    [Parameter(Mandatory=$false)]
    [int]$DaysToCheck = 30,  # Default to 30 days, can be changed when running the script
    
    [Parameter(Mandatory=$false)]
    [switch]$IncludeRunDetails = $false  # Option to include details of each run
)

# Function to get an access token using a browser-based authentication
function Get-AccessTokenInteractive {
    param (
        [string]$TenantId
    )
    
    $deviceCodeUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/devicecode"
    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    
    # Application (client) ID for Azure CLI
    $clientId = "12345678-abcdefg-clientid"
    $resource = "https://management.azure.com/"
    $scope = "$resource.default"
    
    # Step 1: Request device code
    $deviceCodeBody = @{
        client_id = $clientId
        scope = $scope
    }
    
    $deviceCodeResponse = Invoke-RestMethod -Method Post -Uri $deviceCodeUrl -Body $deviceCodeBody
    
    # Display the message to the user
    Write-Host $deviceCodeResponse.message -ForegroundColor Yellow
    
    # Step 2: Poll for the token
    $tokenBody = @{
        grant_type = "device_code"
        client_id = $clientId
        device_code = $deviceCodeResponse.device_code
    }
    
    # Poll until the user completes the sign-in
    $token = $null
    $timeoutSeconds = 300  # 5 minutes timeout
    $startTime = Get-Date
    
    while (-not $token -and ((Get-Date) - $startTime).TotalSeconds -lt $timeoutSeconds) {
        try {
            $tokenResponse = Invoke-RestMethod -Method Post -Uri $tokenUrl -Body $tokenBody -ErrorAction SilentlyContinue
            if ($tokenResponse.access_token) {
                $token = $tokenResponse.access_token
                break
            }
        }
        catch {
            # Expected error until the user completes authentication
            if ($_.Exception.Response.StatusCode.value__ -ne 400) {
                Write-Host "Error: $_" -ForegroundColor Red
            }
        }
        
        # Wait before polling again
        Start-Sleep -Seconds 5
    }
    
    if (-not $token) {
        throw "Timed out waiting for authentication."
    }
    
    return $token
}

# Function to get subscription ID from a list of available subscriptions
function Select-SubscriptionInteractive {
    param (
        [string]$Token
    )
    
    $headers = @{
        "Authorization" = "Bearer $Token"
    }
    
    $subscriptionsUrl = "https://management.azure.com/subscriptions?api-version=2020-01-01"
    
    try {
        $subscriptionsResponse = Invoke-RestMethod -Uri $subscriptionsUrl -Headers $headers -Method Get
        $subscriptions = $subscriptionsResponse.value
        
        if ($subscriptions.Count -eq 0) {
            throw "No subscriptions found for the authenticated user."
        }
        
        if ($subscriptions.Count -eq 1) {
            Write-Host "Using subscription: $($subscriptions[0].displayName) ($($subscriptions[0].subscriptionId))"
            return $subscriptions[0].subscriptionId
        }
        
        # Display available subscriptions
        Write-Host "Available subscriptions:" -ForegroundColor Cyan
        for ($i = 0; $i -lt $subscriptions.Count; $i++) {
            Write-Host "[$i] $($subscriptions[$i].displayName) ($($subscriptions[$i].subscriptionId))"
        }
        
        # Ask the user to select a subscription
        $selection = Read-Host "Enter the number of the subscription to use"
        $subscriptionId = $subscriptions[$selection].subscriptionId
        
        Write-Host "Selected subscription: $($subscriptions[$selection].displayName) ($subscriptionId)"
        return $subscriptionId
    }
    catch {
        throw "Failed to retrieve subscriptions: $_"
    }
}

try {
    # Step 1: Get tenant ID if not provided
    if (-not $TenantId) {
        $TenantId = Read-Host "Enter your Azure AD tenant ID (or press Enter to use common authentication)"
        if (-not $TenantId) {
            $TenantId = "common"  # Use the common endpoint if tenant ID is not provided
        }
    }
    
    # Step 2: Authenticate and get access token
    Write-Host "Authenticating to Azure..."
    $token = Get-AccessTokenInteractive -TenantId $TenantId
    
    $headers = @{
        'Authorization' = "Bearer $token"
        'Content-Type' = 'application/json'
    }
    
    # Step 3: Get subscription ID if not provided
    if (-not $SubscriptionId) {
        $SubscriptionId = Select-SubscriptionInteractive -Token $token
    }
    
    Write-Host "Using subscription: $SubscriptionId"
    Write-Host "Retrieving Data Factory: $DataFactoryName"
    
    # API version
    $apiVersion = "2018-06-01"
    
    # Step 4: Check if data factory exists
    $factoryUrl = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName`?api-version=$apiVersion"
    try {
        $dataFactory = Invoke-RestMethod -Uri $factoryUrl -Headers $headers -Method Get
        Write-Host "Successfully connected to data factory: $($dataFactory.name)"
    } catch {
        Write-Error "Data Factory not found: $DataFactoryName. Error: $_"
        exit
    }
    
    # Step 5: Get all pipelines with pagination support
    Write-Host "Retrieving pipelines..."
    $pipelines = @()
    $nextLink = $null
    $pipelinesUrl = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/pipelines?api-version=$apiVersion"

    do {
        $currentUrl = if ($nextLink) { $nextLink } else { $pipelinesUrl }
        $pipelinesResponse = Invoke-RestMethod -Uri $currentUrl -Headers $headers -Method Get
        
        if ($pipelinesResponse.value) {
            $pipelines += $pipelinesResponse.value
            Write-Host "Retrieved $($pipelines.Count) pipelines so far..."
        }
        
        # Get the nextLink if it exists
        $nextLink = $pipelinesResponse.nextLink
    } while ($nextLink)

    Write-Host "Found $($pipelines.Count) pipelines in total"
    if ($pipelines.Count -eq 0) {
        Write-Host "No pipelines found in data factory $DataFactoryName"
        exit
    }
    
    # Step 6: Calculate time for the last 30 days (or specified days)
    $startDate = (Get-Date).AddDays(-$DaysToCheck)
    $startDateString = $startDate.ToString("yyyy-MM-ddTHH:mm:ssZ")
    $now = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $reportPeriod = "$($startDate.ToString('yyyy-MM-dd')) to $(Get-Date -Format 'yyyy-MM-dd')"
    
    Write-Host "Checking pipeline utilization from $reportPeriod ($DaysToCheck days)"
    
    # Step 7: Create an array to store the results
    $results = @()
    
    # Step 8: Process each pipeline
    $totalPipelines = $pipelines.Count
    $currentPipeline = 0
    
    foreach ($pipeline in $pipelines) {
        $currentPipeline++
        Write-Host "Processing pipeline ${currentPipeline} of ${totalPipelines}: $($pipeline.name)"
        
        try {
            # Query pipeline runs using REST API
            $runQueryUrl = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/queryPipelineRuns?api-version=$apiVersion"
            
            $runQueryBody = @{
                lastUpdatedAfter = $startDateString   # Use the longer time period
                lastUpdatedBefore = $now
                filters = @(
                    @{
                        operand = "PipelineName"
                        operator = "Equals" 
                        values = @($pipeline.name)
                    }
                )
            } | ConvertTo-Json -Depth 4
            
            $pipelineRunsResponse = Invoke-RestMethod -Uri $runQueryUrl -Headers $headers -Method Post -Body $runQueryBody
            $pipelineRuns = $pipelineRunsResponse.value
            
            # Determine if used in the specified time period
            $isUtilized = ($pipelineRuns -and $pipelineRuns.Count -gt 0)
            $runCount = if ($pipelineRuns) { $pipelineRuns.Count } else { 0 }
            
            # Get most recent run date if available
            $mostRecentRun = "N/A"
            if ($pipelineRuns -and $pipelineRuns.Count -gt 0) {
                # Get the latest run (should already be sorted by API)
                $mostRecentRun = $pipelineRuns[0].runEnd
            }
            
            # Add to results
            $results += [PSCustomObject]@{
                PipelineName = $pipeline.name
                IsUtilized = if ($isUtilized) { "Yes" } else { "No" }
                RunCount = $runCount
                LastUpdated = if ($pipeline.properties.lastPublishTime) { $pipeline.properties.lastPublishTime } else { "N/A" }
                ReportPeriod = $reportPeriod
                MostRecentRun = $mostRecentRun
            }
        }
        catch {
            Write-Host "Error processing pipeline runs for $($pipeline.name): $_"
            $results += [PSCustomObject]@{
                PipelineName = $pipeline.name
                IsUtilized = "Error"
                RunCount = 0
                LastUpdated = if ($pipeline.properties.lastPublishTime) { $pipeline.properties.lastPublishTime } else { "N/A" }
                ReportPeriod = $reportPeriod
                MostRecentRun = "Error"
                Error = $_.Exception.Message
            }
        }
    }
    
    # Step 9: Export results to CSV
    if ($results.Count -gt 0) {
        $results | Export-Csv -Path $OutputPath -NoTypeInformation
        Write-Host "Report generated successfully with $($results.Count) pipelines at: $OutputPath"
        Write-Host "Report period: $reportPeriod ($DaysToCheck days)"
        
        # Count utilized vs. non-utilized
        $utilizedCount = ($results | Where-Object { $_.IsUtilized -eq "Yes" }).Count
        $notUtilizedCount = ($results | Where-Object { $_.IsUtilized -eq "No" }).Count
        $errorCount = ($results | Where-Object { $_.IsUtilized -eq "Error" }).Count
        
        Write-Host "Summary:"
        Write-Host "  - Utilized pipelines: $utilizedCount"
        Write-Host "  - Non-utilized pipelines: $notUtilizedCount"
        Write-Host "  - Pipelines with errors: $errorCount"
    }
    else {
        Write-Host "WARNING: No pipeline results were collected."
    }
}
catch {
    Write-Error "An error occurred: $_"
}
