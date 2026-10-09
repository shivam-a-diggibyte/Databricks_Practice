from pyspark import pipelines as dp
from pyspark.sql import functions as F

ORDERS_PATH = "/Volumes/workspace/sdp_lab/lab_files/landing/orders/"
CUSTOMERS_PATH = "/Volumes/workspace/sdp_lab/lab_files/landing/customers/"


# 1. Read raw orders
@dp.table(comment="Raw orders for dependency lab")
def orders_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(ORDERS_PATH)
    )

# 2. Clean orders: depends on orders_raw
@dp.table(comment="Cleaned orders")
def orders_clean():
    return (
        spark.readStream.table("orders_raw")
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("amount") > 0)
        .withColumn("order_date", F.to_date("order_ts"))
        .select(
            "order_id",
            "customer_id",
            "product",
            "amount",
            "order_ts",
            "order_date"
        )
    )

# 3. Read raw customers
@dp.table(comment="Raw customers for dependency lab")
def customers_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(CUSTOMERS_PATH)
    )

# 4. Clean customers: depends on customers_raw
@dp.table(comment="Cleaned customers")
def customers_clean():
    return (
        spark.readStream.table("customers_raw")
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

# 5. Join both cleaned datasets
@dp.materialized_view(comment="Sales summary with customer details")
def sales_summary():
    orders = spark.read.table("orders_clean")
    customers = spark.read.table("customers_clean")

    return (
        orders.join(customers, on="customer_id", how="left")
        .groupBy("city", "product", "order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("total_revenue")
        )
    )