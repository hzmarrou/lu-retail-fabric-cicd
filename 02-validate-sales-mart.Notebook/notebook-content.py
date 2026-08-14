# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e1e00300-9c49-4ca2-bd8d-85cca7aab414",
# META       "default_lakehouse_name": "RetailLakehouse",
# META       "default_lakehouse_workspace_id": "6946af0a-1a9a-4d04-a54c-8e0112a14e69",
# META       "known_lakehouses": [
# META         {
# META           "id": "e1e00300-9c49-4ca2-bd8d-85cca7aab414"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# Load the tables created by 01-build-sales-mart
detail = spark.table("retail_sales_detail")
summary = spark.table("retail_sales_summary")

# Calculate reusable validation values
detail_count = detail.count()
summary_count = summary.count()

null_order_count = (
    detail
    .filter(F.col("order_id").isNull())
    .count()
)

non_positive_sales_count = (
    detail
    .filter(F.col("sales_amount") <= 0)
    .count()
)

countries = {
    row["country"]
    for row in detail.select("country").distinct().collect()
}

# Define the data-quality checks
checks = [
    (
        "detail_has_rows",
        detail_count > 0,
        str(detail_count)
    ),
    (
        "summary_has_rows",
        summary_count > 0,
        str(summary_count)
    ),
    (
        "no_null_order_ids",
        null_order_count == 0,
        str(null_order_count)
    ),
    (
        "sales_are_positive",
        non_positive_sales_count == 0,
        str(non_positive_sales_count)
    ),
    (
        "expected_detail_rows",
        detail_count == 7,
        str(detail_count)
    ),
    (
        "only_france_and_spain",
        countries == {"France", "Spain"},
        str(sorted(countries))
    )
]

# Convert the validation results into a DataFrame
validation = spark.createDataFrame(
    [
        (name, bool(passed), observed)
        for name, passed, observed in checks
    ],
    [
        "check_name",
        "passed",
        "observed_value"
    ]
)

# Save the validation results as a Delta table
(
    validation.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("retail_validation_results")
)

# Display the results
display(validation)

# Stop the CI/CD process if any validation fails
failed_count = (
    validation
    .filter(F.col("passed") == False)
    .count()
)

if failed_count > 0:
    raise Exception(
        f"Data-quality validation failed: "
        f"{failed_count} check(s) failed"
    )

print("All data-quality checks passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
