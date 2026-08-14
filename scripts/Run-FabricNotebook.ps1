param(
  [Parameter(Mandatory = $true)]
  [string]$deploymentPipelineName,

  [Parameter(Mandatory = $true)]
  [string]$targetStageName,

  [Parameter(Mandatory = $true)]
  [string]$notebookName
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

$stage = $stages.value |
  Where-Object { $_.displayName -eq $targetStageName } |
  Select-Object -First 1

if (-not $stage -or [string]::IsNullOrWhiteSpace($stage.workspaceId)) {
  throw "Stage '$targetStageName' was not found or has no assigned workspace"
}

$workspaceId = $stage.workspaceId

$items = Invoke-RestMethod `
  -Headers $headers `
  -Uri "$baseUrl/workspaces/$workspaceId/items?type=Notebook" `
  -Method GET

$notebook = $items.value |
  Where-Object { $_.displayName -eq $notebookName } |
  Select-Object -First 1

if (-not $notebook) {
  throw "Notebook '$notebookName' not found in stage '$targetStageName'"
}

Write-Host "Starting '$notebookName' in '$targetStageName'"

$response = Invoke-WebRequest `
  -Headers $headers `
  -Uri "$baseUrl/workspaces/$workspaceId/items/$($notebook.id)/jobs/instances?jobType=RunNotebook" `
  -Method POST

$jobLocation = $response.Headers["Location"]
if ($jobLocation -is [array]) { $jobLocation = $jobLocation[0] }

if (-not $jobLocation) {
  throw "Fabric did not return a notebook-job Location header"
}

$retryAfter = $response.Headers["Retry-After"]
if ($retryAfter -is [array]) { $retryAfter = $retryAfter[0] }
if (-not $retryAfter) { $retryAfter = 10 }

do {
  Start-Sleep -Seconds ([int]$retryAfter)
  $job = Invoke-RestMethod `
    -Headers $headers `
    -Uri $jobLocation `
    -Method GET

  Write-Host "Notebook status = $($job.status)"
}
while ($job.status -in @("NotStarted", "InProgress"))

if ($job.status -ne "Completed") {
  throw "Notebook '$notebookName' finished with status '$($job.status)'"
}

Write-Host "Notebook '$notebookName' completed in '$targetStageName'"