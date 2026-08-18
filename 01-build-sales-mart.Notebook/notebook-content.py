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

# allowed_countries = ["France", "Spain"]
allowed_countries = ["France"]

completed_orders = (
    orders
    .filter(F.col("status") == "Completed")
    .join(customers.select("customer_id", "country"), "customer_id", "inner")
    .filter(F.col("country").isin(allowed_countries))
    .drop("country")
)

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
        F.round(F.sum("sales_amount"), 2).alias("total_sales"),
        F.round(F.avg("sales_amount"), 2).alias("avg_order_value")   # <-- NEW
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
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .format("delta")
    .saveAsTable("retail_sales_summary")
)

display(sales_summary.orderBy(F.desc("total_sales")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
