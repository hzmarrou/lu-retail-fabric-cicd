# Option 3 From-Scratch Tutorial: Retail Sales CI/CD

This hands-on tutorial builds a moderately realistic Microsoft Fabric solution in a brand-new tenant and promotes it with **Fabric Deployment Pipelines**, **GitHub Actions**, and a **service principal (SPN)**.

You will build this flow:

```text
Feature work merged to main
        ↓
GitHub Actions starts automatically
        ↓
SPN triggers Fabric Dev → Acc deployment
        ↓
SPN runs two notebooks in Acc, in order
        ↓
Tester validates the Acc tables and report
        ↓
Human approves Prod in GitHub
        ↓
SPN triggers Fabric Acc → Prod deployment
        ↓
SPN runs the same two notebooks in Prod
```

The exercise creates a small **Retail Sales Mart**:

- Three CSV source files: customers, products, and orders.
- A Lakehouse called `RetailLakehouse`.
- Notebook `01-build-sales-mart` to ingest, join, and aggregate the data.
- Notebook `02-validate-sales-mart` to run data-quality checks.
- Delta tables for detailed sales, country/category summaries, and validation results.
- A semantic model and Power BI report called `Retail Sales Report`.
- Dev, Acc, and Prod workspaces.
- Automatic Acc deployment and processing.
- Human approval before Prod.

---

## 1. Mental model

There are three distinct processes:

```text
Git
  versions item definitions and starts GitHub Actions

Fabric Deployment Pipeline
  copies supported item definitions between workspaces

Notebook Job API
  executes selected notebooks in each target workspace
```

Deployment does **not** copy Lakehouse table data or files. The CSV files must already be available in each environment, or be loaded from a shared external source. The deployed notebooks independently create the Acc and Prod tables.

---

## 2. Naming convention

Use environment-specific names for workspaces, but identical names for corresponding items.

| Object | Dev | Acc | Prod |
|---|---|---|---|
| Workspace | `LU-Retail-Dev` | `LU-Retail-Acc` | `LU-Retail-Prod` |
| Lakehouse | `RetailLakehouse` | `RetailLakehouse` | `RetailLakehouse` |
| Build notebook | `01-build-sales-mart` | same | same |
| Validation notebook | `02-validate-sales-mart` | same | same |
| Semantic model | `Retail Sales Model` | same | same |
| Report | `Retail Sales Report` | same | same |

Matching item names help Fabric pair corresponding items between pipeline stages. Environment-specific settings belong in connections, parameters, deployment rules, or variable libraries—not in item names.

---

## Phase A — Tenant and workspace preparation

## 3. Confirm tenant prerequisites

You need:

- A Microsoft Fabric tenant.
- A Fabric capacity or trial capacity that is active.
- Permission to create workspaces.
- Permission to create an Entra app registration, or help from an Entra administrator.
- A GitHub repository.
- Fabric tenant-admin assistance for the service-principal API setting if you are not an admin.

Keep the capacity active while testing. A paused capacity can produce errors such as `WorkloadUnavailable` during deployment or notebook execution.

## 4. Create three Fabric workspaces

Create:

1. `LU-Retail-Dev`
2. `LU-Retail-Acc`
3. `LU-Retail-Prod`

Assign all three to an active Fabric capacity.

Recommended workspace access:

- A platform-admin Entra group: Admin.
- Developers: Contributor on Dev only.
- Testers: Viewer or app/report access in Acc.
- End users: app/report access in Prod.

Avoid making one developer the only workspace administrator.

---

## Phase B — Build the Dev solution

## 5. Create the source CSV files

Create these three files locally.

### `customers.csv`

```csv
customer_id,customer_name,country,segment
C001,Alice Martin,France,Enterprise
C002,Bruno Leroy,France,Small Business
C003,Carla Garcia,Spain,Enterprise
C004,Diego Ruiz,Spain,Consumer
C005,Emma Dubois,France,Consumer
C006,Fernando Lopez,Spain,Small Business
C007,Greta Schmidt,Germany,Enterprise
C008,Hannah Jones,United Kingdom,Small Business
```

### `products.csv`

```csv
product_id,product_name,category,unit_price
P001,Laptop Pro,Computers,1400
P002,USB-C Dock,Accessories,180
P003,Analytics Course,Training,600
P004,AI Workshop,Training,900
P005,Monitor 27,Displays,350
```

### `orders.csv`

```csv
order_id,order_date,customer_id,product_id,quantity,status
O001,2026-07-01,C001,P001,2,Completed
O002,2026-07-02,C002,P002,3,Completed
O003,2026-07-03,C003,P003,1,Completed
O004,2026-07-04,C004,P004,2,Completed
O005,2026-07-05,C005,P005,1,Cancelled
O006,2026-07-06,C006,P001,1,Completed
O007,2026-07-07,C007,P004,1,Completed
O008,2026-07-08,C008,P002,4,Completed
O009,2026-07-09,C001,P003,2,Completed
O010,2026-07-10,C003,P005,2,Completed
```

## 6. Create the Dev Lakehouse

In `LU-Retail-Dev`:

1. Create a Lakehouse named `RetailLakehouse`.
2. Open its `Files` area.
3. Create or upload to this folder structure:

```text
Files/source/customers.csv
Files/source/products.csv
Files/source/orders.csv
```

## 7. Create notebook `01-build-sales-mart`

Create a notebook named `01-build-sales-mart` and attach `RetailLakehouse` as its default Lakehouse.

Paste this PySpark code:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Files/source/customers.csv")
)

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Files/source/products.csv")
    .withColumn("unit_price", F.col("unit_price").cast(DecimalType(12, 2)))
)

orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Files/source/orders.csv")
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
)

completed_orders = orders.filter(F.col("status") == "Completed")

sales_detail = (
    completed_orders
    .join(customers, "customer_id", "inner")
    .join(products, "product_id", "inner")
    .withColumn("sales_amount", F.col("quantity") * F.col("unit_price"))
    .select(
        "order_id",
        "order_date",
        "customer_id",
        "customer_name",
        "country",
        "segment",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "sales_amount"
    )
)

sales_summary = (
    sales_detail
    .groupBy("country", "category")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("sales_amount"), 2).alias("total_sales")
    )
)

(
    sales_detail.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("retail_sales_detail")
)

(
    sales_summary.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("retail_sales_summary")
)

display(sales_summary.orderBy(F.desc("total_sales")))
```

Run the notebook and refresh the Lakehouse explorer. Confirm these tables exist:

```text
dbo.retail_sales_detail
dbo.retail_sales_summary
```

## 8. Create notebook `02-validate-sales-mart`

Create a second notebook named `02-validate-sales-mart`, also attached to `RetailLakehouse`.

Paste:

```python
from pyspark.sql import functions as F

detail = spark.table("retail_sales_detail")
summary = spark.table("retail_sales_summary")

checks = [
    ("detail_has_rows", detail.count() > 0, str(detail.count())),
    ("summary_has_rows", summary.count() > 0, str(summary.count())),
    ("no_null_order_ids", detail.filter(F.col("order_id").isNull()).count() == 0,
     str(detail.filter(F.col("order_id").isNull()).count())),
    ("sales_are_positive", detail.filter(F.col("sales_amount") <= 0).count() == 0,
     str(detail.filter(F.col("sales_amount") <= 0).count())),
    ("only_completed_orders_loaded", detail.count() == 9, str(detail.count()))
]

validation = spark.createDataFrame(
    [(name, bool(passed), observed) for name, passed, observed in checks],
    ["check_name", "passed", "observed_value"]
)

(
    validation.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("retail_validation_results")
)

display(validation)

failed_count = validation.filter(F.col("passed") == False).count()

if failed_count > 0:
    raise Exception(f"Data-quality validation failed: {failed_count} check(s) failed")
```

Run it and confirm every row in `dbo.retail_validation_results` has `passed=true`.

The exception is intentional: GitHub Actions must stop before Prod when Acc validation fails.

## 9. Create the semantic model and report

Create a semantic model named `Retail Sales Model` using `retail_sales_summary` and optionally `retail_sales_detail`.

Create a report named `Retail Sales Report` with:

- Total Sales card.
- Sales by Country bar chart.
- Sales by Category column chart.
- Country and Category slicers.

Save both items in `LU-Retail-Dev`.

---

## Phase C — Create Git source control

## 10. Create the GitHub repository

Create a repository, for example:

```text
lu-retail-fabric-cicd
```

In `LU-Retail-Dev`, configure Fabric Git integration to that repository and initially connect to `main`.

Commit the Fabric item definitions to Git.

Important:

- Git stores supported item definitions.
- Git does not store Lakehouse table data.
- The CSV source files uploaded to the Lakehouse are not promoted by the deployment pipeline.

## 11. Prepare source files in Acc and Prod

Create `RetailLakehouse` through deployment later, but the source files still need to exist in each target environment.

For this training exercise, after the first Lakehouse deployment, upload the same three CSV files to:

```text
LU-Retail-Acc / RetailLakehouse / Files/source/
LU-Retail-Prod / RetailLakehouse / Files/source/
```

In a production system, replace manual uploads with shared cloud storage, OneLake shortcuts, or an ingestion pipeline.

---

## Phase D — Create the Fabric deployment pipeline

## 12. Create and assign stages

Create a Fabric deployment pipeline named:

```text
lu-retail-deployment-pipeline
```

Create stages:

```text
Dev → Acc → Prod
```

Assign:

| Stage | Workspace |
|---|---|
| Dev | `LU-Retail-Dev` |
| Acc | `LU-Retail-Acc` |
| Prod | `LU-Retail-Prod` |

Perform an initial manual deployment Dev → Acc → Prod so you understand the pipeline and create the corresponding target items.

Afterward, upload the CSV files to the target Lakehouses and manually run both notebooks once in Acc and Prod. This proves the item bindings and Spark settings work before automation.

> **Critical Lakehouse-binding check:** A deployed notebook must use the target workspace's `RetailLakehouse`. Automatic remapping works best when the Lakehouse is deployed as a related item through the same pipeline and retains its lineage. Before unattended execution, open each target notebook and verify its default Lakehouse. An incorrect binding can silently read from or write to the wrong environment.

---

## Phase E — Create the service principal

## 13. Register the Entra application

In Microsoft Entra ID:

1. Open **App registrations → New registration**.
2. Name it `sp-lu-retail-fabric-cicd`.
3. Choose single tenant.
4. Leave Redirect URI empty.
5. Register.
6. Record:
   - Directory (tenant) ID.
   - Application (client) ID.
7. Open **Certificates & secrets**.
8. Create a client secret for this exercise.
9. Copy the secret **Value** immediately.

For production, prefer GitHub OIDC/workload identity federation over a long-lived client secret.

## 14. Enable Fabric API access and grant roles

In the Fabric Admin portal, enable:

```text
Service principals can call Fabric public APIs
```

Prefer scoping the setting to an Entra security group containing approved Fabric automation SPNs.

Grant `sp-lu-retail-fabric-cicd`:

- Contributor on `LU-Retail-Dev`.
- Contributor on `LU-Retail-Acc`.
- Contributor on `LU-Retail-Prod`.
- Admin on `lu-retail-deployment-pipeline` under **Manage access**.

The SPN needs access to both sides of every deployment hop.

---

## Phase F — Configure GitHub

## 15. Create repository secrets

Open **Settings → Secrets and variables → Actions → Secrets**.

Create repository secrets:

| Name | Value |
|---|---|
| `FABRIC_TENANT_ID` | Entra Directory (tenant) ID |
| `FABRIC_CLIENT_ID` | Entra Application (client) ID |
| `FABRIC_CLIENT_SECRET` | Client-secret Value |

## 16. Create repository variables

Under **Actions → Variables**, create repository variables:

| Name | Value |
|---|---|
| `DEPLOYMENT_PIPELINE_NAME` | `lu-retail-deployment-pipeline` |
| `DEV_STAGE_NAME` | `Dev` |
| `ACC_STAGE_NAME` | `Acc` |
| `PROD_STAGE_NAME` | `Prod` |
| `BUILD_NOTEBOOK_NAME` | `01-build-sales-mart` |
| `VALIDATION_NOTEBOOK_NAME` | `02-validate-sales-mart` |

These notebook variables identify which deployed notebooks must be executed. They do not control which items are deployed. The Fabric deployment pipeline promotes supported stage content; GitHub explicitly runs only the operational items that require execution.

## 17. Create the Prod approval environment

Open **Settings → Environments** and create:

```text
Prod
```

Enable **Required reviewers**.

For a real team, use a release-manager GitHub team or multiple reviewers. Do not make a single developer the only approver.

Do not create an Acc approval environment: Acc deployment and processing should be automatic.

---

## Phase G — Add automation scripts

## 18. Add `DeploymentPipelines-DeployAll.ps1`

Create:

```text
scripts/DeploymentPipelines-DeployAll.ps1
```

```powershell
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
```

## 19. Add `Run-FabricNotebook.ps1`

Create:

```text
scripts/Run-FabricNotebook.ps1
```

```powershell
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
```

The empty POST body means Fabric runs the notebook with its saved defaults. The API also supports execution data for notebook parameters and default-Lakehouse overrides if you later require per-environment runtime values.

---

## Phase H — Add the GitHub Actions workflow

## 20. Create the workflow

Create:

```text
.github/workflows/fabric-retail-promotion.yml
```

```yaml
name: Fabric Retail Promotion

on:
  push:
    branches:
      - main
  workflow_dispatch:

concurrency:
  group: fabric-retail-promotion-main
  cancel-in-progress: false

jobs:
  deploy-acc:
    name: Deploy, build, and validate Acc
    runs-on: windows-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Acquire Fabric token
        id: fabric-auth
        shell: pwsh
        run: |
          $body = @{
            client_id     = "${{ secrets.FABRIC_CLIENT_ID }}"
            client_secret = "${{ secrets.FABRIC_CLIENT_SECRET }}"
            scope         = "https://api.fabric.microsoft.com/.default"
            grant_type    = "client_credentials"
          }

          $response = Invoke-RestMethod `
            -Uri "https://login.microsoftonline.com/${{ secrets.FABRIC_TENANT_ID }}/oauth2/v2.0/token" `
            -Method POST `
            -Body $body `
            -ContentType "application/x-www-form-urlencoded"

          if (-not $response.access_token) {
            throw "Failed to acquire Fabric token"
          }

          Write-Host "::add-mask::$($response.access_token)"
          "token=$($response.access_token)" >> $env:GITHUB_OUTPUT

      - name: Deploy Dev to Acc
        shell: pwsh
        env:
          FABRIC_TOKEN: ${{ steps.fabric-auth.outputs.token }}
        run: |
          & ./scripts/DeploymentPipelines-DeployAll.ps1 `
            -deploymentPipelineName "${{ vars.DEPLOYMENT_PIPELINE_NAME }}" `
            -sourceStageName "${{ vars.DEV_STAGE_NAME }}" `
            -targetStageName "${{ vars.ACC_STAGE_NAME }}" `
            -deploymentNote "Automatic Dev to Acc after main update"

      - name: Build sales mart in Acc
        shell: pwsh
        env:
          FABRIC_TOKEN: ${{ steps.fabric-auth.outputs.token }}
        run: |
          & ./scripts/Run-FabricNotebook.ps1 `
            -deploymentPipelineName "${{ vars.DEPLOYMENT_PIPELINE_NAME }}" `
            -targetStageName "${{ vars.ACC_STAGE_NAME }}" `
            -notebookName "${{ vars.BUILD_NOTEBOOK_NAME }}"

      - name: Validate sales mart in Acc
        shell: pwsh
        env:
          FABRIC_TOKEN: ${{ steps.fabric-auth.outputs.token }}
        run: |
          & ./scripts/Run-FabricNotebook.ps1 `
            -deploymentPipelineName "${{ vars.DEPLOYMENT_PIPELINE_NAME }}" `
            -targetStageName "${{ vars.ACC_STAGE_NAME }}" `
            -notebookName "${{ vars.VALIDATION_NOTEBOOK_NAME }}"

  deploy-prod:
    name: Approve, deploy, build, and validate Prod
    needs: deploy-acc
    runs-on: windows-latest
    environment: Prod

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Acquire Fabric token
        id: fabric-auth
        shell: pwsh
        run: |
          $body = @{
            client_id     = "${{ secrets.FABRIC_CLIENT_ID }}"
            client_secret = "${{ secrets.FABRIC_CLIENT_SECRET }}"
            scope         = "https://api.fabric.microsoft.com/.default"
            grant_type    = "client_credentials"
          }

          $response = Invoke-RestMethod `
            -Uri "https://login.microsoftonline.com/${{ secrets.FABRIC_TENANT_ID }}/oauth2/v2.0/token" `
            -Method POST `
            -Body $body `
            -ContentType "application/x-www-form-urlencoded"

          if (-not $response.access_token) {
            throw "Failed to acquire Fabric token"
          }

          Write-Host "::add-mask::$($response.access_token)"
          "token=$($response.access_token)" >> $env:GITHUB_OUTPUT

      - name: Deploy Acc to Prod
        shell: pwsh
        env:
          FABRIC_TOKEN: ${{ steps.fabric-auth.outputs.token }}
        run: |
          & ./scripts/DeploymentPipelines-DeployAll.ps1 `
            -deploymentPipelineName "${{ vars.DEPLOYMENT_PIPELINE_NAME }}" `
            -sourceStageName "${{ vars.ACC_STAGE_NAME }}" `
            -targetStageName "${{ vars.PROD_STAGE_NAME }}" `
            -deploymentNote "Approved Acc to Prod promotion"

      - name: Build sales mart in Prod
        shell: pwsh
        env:
          FABRIC_TOKEN: ${{ steps.fabric-auth.outputs.token }}
        run: |
          & ./scripts/Run-FabricNotebook.ps1 `
            -deploymentPipelineName "${{ vars.DEPLOYMENT_PIPELINE_NAME }}" `
            -targetStageName "${{ vars.PROD_STAGE_NAME }}" `
            -notebookName "${{ vars.BUILD_NOTEBOOK_NAME }}"

      - name: Validate sales mart in Prod
        shell: pwsh
        env:
          FABRIC_TOKEN: ${{ steps.fabric-auth.outputs.token }}
        run: |
          & ./scripts/Run-FabricNotebook.ps1 `
            -deploymentPipelineName "${{ vars.DEPLOYMENT_PIPELINE_NAME }}" `
            -targetStageName "${{ vars.PROD_STAGE_NAME }}" `
            -notebookName "${{ vars.VALIDATION_NOTEBOOK_NAME }}"
```

The two jobs intentionally check out the repository and acquire separate tokens because GitHub runs them on separate, disposable runners.

---

## Phase I — Test the first automated promotion

## 21. Commit the automation through a pull request

From a local clone:

```powershell
git switch -c feature/add-fabric-automation
git add .github/workflows/fabric-retail-promotion.yml scripts
git commit -m "ci: automate Fabric retail promotion"
git push -u origin feature/add-fabric-automation
```

Create and merge a pull request into `main`.

The merge starts GitHub Actions automatically.

Expected sequence:

```text
Dev → Acc deployment                 automatic
01-build-sales-mart in Acc           automatic
02-validate-sales-mart in Acc        automatic
Prod approval                        waiting
Acc → Prod deployment                after approval
01-build-sales-mart in Prod          automatic
02-validate-sales-mart in Prod       automatic
```

Before approving Prod:

1. Open `LU-Retail-Acc`.
2. Check `retail_validation_results` for failures.
3. Open `Retail Sales Report` and validate totals/charts.
4. Return to GitHub Actions.
5. Select **Review deployments → Prod → Approve and deploy**.

---

## Phase J — Practice a business change

## 22. Change the transformation requirement

The original build notebook includes all completed orders. Change the business rule so the mart includes only:

```text
France and Spain
```

Change:

```python
completed_orders = orders.filter(F.col("status") == "Completed")
```

to:

```python
allowed_countries = ["France", "Spain"]

completed_orders = (
    orders
    .filter(F.col("status") == "Completed")
    .join(customers.select("customer_id", "country"), "customer_id", "inner")
    .filter(F.col("country").isin(allowed_countries))
    .drop("country")
)
```

Update the validation notebook because the expected detail-row count changes. Do not hard-code a new count without calculating it first in Dev.

Run both notebooks in Dev and verify:

- Germany and United Kingdom disappear from the output.
- France and Spain remain.
- Validation passes.
- The report reflects the new scope.

## 23. Promote the change through Git

For this controlled exercise:

1. Create a feature branch using Fabric Git integration or the branch-out workflow.
2. Commit the two changed notebook definitions.
3. Open a pull request to `main`.
4. Confirm the live Dev workspace still contains exactly the reviewed version.
5. Do not make another Dev change until Dev → Acc finishes.
6. Merge the pull request.

The merge automatically starts the workflow.

Validate Acc, then approve Prod.

---

## 24. Critical Option 3 limitation

Option 3 does not deploy directly from the Git commit:

```text
Merge to main starts the workflow,
but the Fabric deployment pipeline copies the live Dev workspace.
```

Therefore, the training process requires the live Dev workspace to remain identical to the reviewed change until Acc deployment completes.

For a production-grade multi-developer setup, add a dedicated integration workspace connected to `main` and synchronize it from Git before deployment. Use that integration workspace as the deployment pipeline's Dev stage.

A hardened architecture is:

```text
Developer feature workspace/branch
        ↓ pull request
main
        ↓ automatic Git synchronization
Integration workspace (pipeline Dev stage)
        ↓
Acc
        ↓ approval
Prod
```

That extra synchronization is beyond this introductory exercise, but it is essential if you need to prove that the deployed workspace exactly matches a reviewed commit.

---

## 25. Failure behavior

| Failure | Expected result |
|---|---|
| Capacity paused | Deployment or notebook run fails; resume capacity and rerun failed jobs |
| Dev → Acc deployment fails | Acc notebooks do not run; Prod cannot start |
| Build notebook fails in Acc | Validation and Prod do not start |
| Validation fails in Acc | Prod does not become eligible for release |
| Prod approval is withheld | Prod remains unchanged |
| Acc → Prod deployment fails | Prod notebooks do not run |
| Prod validation fails | GitHub run is red; investigate before declaring success |

## 26. Troubleshooting checklist

### `WorkloadUnavailable`

- Confirm the Fabric capacity is running.
- Wait several minutes after resuming it.
- Open Dev and Acc items to confirm workloads are available.
- Use **Re-run failed jobs** in GitHub.

### `InsufficientPrivileges`

- SPN must be Contributor on source and target workspaces.
- SPN must be Admin on the deployment pipeline.
- Allow time for permissions to propagate.

### Notebook cannot find CSV files

Confirm these exist in the target Lakehouse:

```text
Files/source/customers.csv
Files/source/products.csv
Files/source/orders.csv
```

### Notebook writes to the wrong workspace

Verify its default Lakehouse and deployment lineage. Never approve Prod until the Acc table location has been checked.

### Prod does not wait for approval

Confirm:

```yaml
environment: Prod
```

exists on the Prod job and the GitHub `Prod` environment has a required-reviewer rule.

### Workflow runs twice

- Check whether both a direct push and a manual run occurred.
- Keep the `concurrency` block.
- Avoid approving an obsolete waiting run after a newer run has completed Acc.

---

## 27. Production-hardening improvements

After the exercise works:

1. Protect `main` and require pull requests.
2. Require automated checks before merging.
3. Use GitHub teams for Prod reviewers.
4. Replace client secrets with OIDC/federated credentials.
5. Use a dedicated integration workspace synchronized from `main`.
6. Automate source-data ingestion instead of uploading CSV files manually.
7. Parameterize environment-specific connections and runtime values.
8. Add report/semantic-model refresh steps if required.
9. Add deployment-history and notebook-run audit monitoring.
10. Define SPN credential ownership and rotation.
11. Keep at least two human admins for GitHub, Fabric workspaces, and the deployment pipeline.
12. Stop or scale down capacity outside exercise hours only when no deployment/run is active.

---

## 28. Completion checklist

- [ ] Three environment workspaces exist.
- [ ] Corresponding Fabric items have identical names.
- [ ] The two Dev notebooks run successfully.
- [ ] The report works in Dev.
- [ ] Git integration is configured.
- [ ] The deployment pipeline has Dev, Acc, and Prod stages.
- [ ] The SPN can call Fabric APIs.
- [ ] The SPN has all required workspace and pipeline roles.
- [ ] GitHub secrets and variables are configured.
- [ ] A merge to `main` starts the workflow.
- [ ] Dev → Acc is automatic.
- [ ] Build and validation notebooks run automatically in Acc.
- [ ] Prod waits for human approval.
- [ ] Acc → Prod is automatic after approval.
- [ ] Build and validation notebooks run automatically in Prod.
- [ ] Acc and Prod tables are separate and correct.
- [ ] The business-rule change travels through the complete process.

---

## Final mental model

```text
Developer changes Fabric definitions
        ↓
Feature branch and pull request
        ↓
Merge to main triggers GitHub Actions
        ↓
GitHub authenticates as the SPN
        ↓
SPN asks Fabric Deployment Pipelines to promote Dev → Acc
        ↓
SPN asks Fabric to run build and validation notebooks in Acc
        ↓
Tester validates tables and report
        ↓
Release reviewer approves Prod in GitHub
        ↓
SPN asks Fabric to promote Acc → Prod
        ↓
SPN runs build and validation notebooks in Prod
```

Humans author, review, test, and approve. The SPN performs the technical deployment and execution actions.
