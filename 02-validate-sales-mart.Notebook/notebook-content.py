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

# Welcome to your new notebook
# Type here in the cell editor to add code!
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
