param(
  [Parameter(Mandatory = $true)]
  [string]$workspaceId,

  [Parameter(Mandatory = $true)]
  [string]$modelName,

  [Parameter(Mandatory = $true)]
  [string]$targetServer,

  [Parameter(Mandatory = $true)]
  [string]$targetDatabase,

  [switch]$Refresh
)

# Rebind a Fabric semantic model's SQL data source to the target stage's Lakehouse
# SQL analytics endpoint, then optionally refresh it.
#
# Why this is needed: the semantic model hardcodes the Dev SQL endpoint. Fabric
# Deployment Pipelines copy that connection as-is (no auto-binding for a hardcoded
# SQL connection), so after each promotion the model in Acc/Prod still points at
# Dev. This script repoints it to the correct stage, running as the service
# principal, which then owns the model.

$ErrorActionPreference = "Stop"

$tenantId     = $env:FABRIC_TENANT_ID
$clientId     = $env:FABRIC_CLIENT_ID
$clientSecret = $env:FABRIC_CLIENT_SECRET

if ([string]::IsNullOrWhiteSpace($tenantId) -or
    [string]::IsNullOrWhiteSpace($clientId) -or
    [string]::IsNullOrWhiteSpace($clientSecret)) {
  throw "FABRIC_TENANT_ID, FABRIC_CLIENT_ID and FABRIC_CLIENT_SECRET must be set"
}

# Power BI scoped token (semantic model / dataset APIs live under the Power BI API)
$tokenResponse = Invoke-RestMethod `
  -Uri "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token" `
  -Method POST `
  -Body @{
    client_id     = $clientId
    client_secret = $clientSecret
    scope         = "https://analysis.windows.net/powerbi/api/.default"
    grant_type    = "client_credentials"
  } `
  -ContentType "application/x-www-form-urlencoded"

$headers = @{
  Authorization  = "Bearer $($tokenResponse.access_token)"
  "Content-Type" = "application/json"
}

$pbi = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId"

# Resolve the semantic model by name
$datasets = Invoke-RestMethod -Headers $headers -Uri "$pbi/datasets" -Method GET
$model = $datasets.value | Where-Object { $_.name -eq $modelName } | Select-Object -First 1
if (-not $model) {
  throw "Semantic model '$modelName' not found in workspace '$workspaceId'"
}
$datasetId = $model.id
Write-Host "Semantic model '$modelName' = $datasetId"

# Take over as the service principal so we can update it
Invoke-RestMethod -Headers $headers -Method POST `
  -Uri "$pbi/datasets/$datasetId/Default.TakeOver" | Out-Null
Write-Host "Took over the semantic model as the service principal"

# Find the current SQL data source (the 'from' side of the swap)
$sources = (Invoke-RestMethod -Headers $headers -Uri "$pbi/datasets/$datasetId/datasources" -Method GET).value
$sqlSource = $sources | Where-Object { $_.datasourceType -eq "Sql" } | Select-Object -First 1
if (-not $sqlSource) {
  throw "No SQL data source found on semantic model '$modelName'"
}

$currentServer   = $sqlSource.connectionDetails.server
$currentDatabase = $sqlSource.connectionDetails.database
Write-Host "Current SQL source : $currentServer / $currentDatabase"
Write-Host "Target SQL source  : $targetServer / $targetDatabase"

if ($currentServer -eq $targetServer -and $currentDatabase -eq $targetDatabase) {
  Write-Host "Already bound to the target endpoint; no rebind needed"
}
else {
  $body = @{
    updateDetails = @(
      @{
        datasourceSelector = @{
          datasourceType    = "Sql"
          connectionDetails = @{ server = $currentServer; database = $currentDatabase }
        }
        connectionDetails = @{ server = $targetServer; database = $targetDatabase }
      }
    )
  } | ConvertTo-Json -Depth 8

  Invoke-RestMethod -Headers $headers -Method POST `
    -Uri "$pbi/datasets/$datasetId/Default.UpdateDatasources" -Body $body | Out-Null
  Write-Host "Rebound the semantic model to the target endpoint"
}

if (-not $Refresh) {
  return
}

# Refresh so the model reads the target Lakehouse tables
Invoke-RestMethod -Headers $headers -Method POST `
  -Uri "$pbi/datasets/$datasetId/refreshes" `
  -Body (@{ notifyOption = "NoNotification" } | ConvertTo-Json) | Out-Null
Write-Host "Refresh queued; waiting for completion..."

do {
  Start-Sleep -Seconds 12
  $last = (Invoke-RestMethod -Headers $headers -Uri "$pbi/datasets/$datasetId/refreshes?`$top=1" -Method GET).value[0]
  Write-Host "Refresh status = $($last.status)"
}
while ($last.status -eq "Unknown")

if ($last.status -ne "Completed") {
  throw "Semantic model refresh failed: $($last.serviceExceptionJson)"
}

Write-Host "Semantic model '$modelName' rebound and refreshed successfully"
