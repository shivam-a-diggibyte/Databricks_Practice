from pyspark import pipelines as dp
from pyspark.sql import functions as F

ORDERS_PATH = "/Volumes/workspace/sdp_lab/lab_files/landing/orders/"

# BRONZE: Ingest raw orders
@dp.table(comment="Bronze layer - raw orders")
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles") # represents auto loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(ORDERS_PATH)
    )

# silver : Validate and clean orders

@dp.table(comment="Silver layer - validated orders")
@dp.expect("valid_order_id", "order_id IS NOT NULL")
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("positive_amount", "amount > 0")
@dp.expect("valid_order_timestamp", "order_ts IS NOT NULL")
def orders_silver():
    return (
        spark.readStream.table("orders_bronze")
        .withColumn("order_date", F.to_date("order_ts"))
        .dropDuplicates(["order_id"])
    )

# GOLD: Aggregate trusted orders
@dp.materialized_view(comment="Gold layer - daily sales")
def daily_sales_gold():
    return (
        spark.read.table("orders_silver")
        .groupBy("order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("total_revenue")
        )
        .orderBy("order_date")
    )