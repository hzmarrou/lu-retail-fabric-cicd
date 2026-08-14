param(
  [Parameter(Mandatory = $true)]
  [string]$deploymentPipelineName,

  [Parameter(Mandatory = $true)]
  [string]$sourceStageName,

  [Parameter(Mandatory = $true)]
  [string]$targetStageName,

  [string]$deploymentNote
)

$baseUrl = "https://api.fabric.microsoft.com/v1"
$token = $env:FABRIC_TOKEN

if ([string]::IsNullOrWhiteSpace($token)) {
  throw "FABRIC_TOKEN not provided"
}

$headers = @{
  Authorization  = "Bearer $token"
  "Content-Type" = "application/json"
}

$pipelines = Invoke-RestMethod `
  -Headers $headers `
  -Uri "$baseUrl/deploymentPipelines" `
  -Method GET

$pipeline = $pipelines.value |
  Where-Object { $_.displayName -eq $deploymentPipelineName } |
  Select-Object -First 1

if (-not $pipeline) {
  throw "Pipeline '$deploymentPipelineName' not found"
}

$stages = Invoke-RestMethod `
  -Headers $headers `
  -Uri "$baseUrl/deploymentPipelines/$($pipeline.id)/stages" `
  -Method GET

$source = $stages.value |
  Where-Object { $_.displayName -eq $sourceStageName } |
  Select-Object -First 1

$target = $stages.value |
  Where-Object { $_.displayName -eq $targetStageName } |
  Select-Object -First 1

if (-not $source -or -not $target) {
  throw "Could not resolve stages '$sourceStageName' and '$targetStageName'"
}

$body = @{
  sourceStageId = $source.id
  targetStageId = $target.id
  note          = $deploymentNote
} | ConvertTo-Json -Depth 10

$response = Invoke-WebRequest `
  -Headers $headers `
  -Uri "$baseUrl/deploymentPipelines/$($pipeline.id)/deploy" `
  -Method POST `
  -Body $body

$operationId = $response.Headers["x-ms-operation-id"]
if ($operationId -is [array]) { $operationId = $operationId[0] }

if (-not $operationId) {
  throw "Fabric did not return a deployment operation ID"
}

$retryAfter = $response.Headers["Retry-After"]
if ($retryAfter -is [array]) { $retryAfter = $retryAfter[0] }
if (-not $retryAfter) { $retryAfter = 5 }

do {
  Start-Sleep -Seconds ([int]$retryAfter)
  $operation = Invoke-RestMethod `
    -Headers $headers `
    -Uri "$baseUrl/operations/$operationId" `
    -Method GET

  Write-Host "Deployment status = $($operation.status)"
}
while ($operation.status -in @("NotStarted", "Running"))

if ($operation.status -ne "Succeeded") {
  throw "Deployment failed: $($operation.error | ConvertTo-Json -Depth 10)"
}

Write-Host "Deployment $sourceStageName → $targetStageName completed"