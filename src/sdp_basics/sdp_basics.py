from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/workspace/sdp_lab/lab_files/landing/orders/"

# Streaming table: incrementally reads new JSON files
@dp.table(comment="Raw orders")
def orders_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING)
    )

# Materialized view: calculates revenue for each day
@dp.materialized_view(comment="Daily revenue")
def daily_revenue():
    return (
        spark.read.table("orders_raw")
        .withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date")
        .agg(F.sum("amount").alias("revenue"))
    )