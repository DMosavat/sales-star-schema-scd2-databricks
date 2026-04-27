# Databricks notebook source

import sys
sys.path.append(".")

from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    lit,
    current_date,
    monotonically_increasing_id
)

from src.config import Paths, JobConfig
from src.utils import (
    read_delta,
    write_delta,
    write_delta_partitioned,
    log_dataframe_count
)

paths = Paths()
config = JobConfig()

silver_df = read_delta(spark, paths.SILVER_SALES_PATH)

log_dataframe_count(silver_df, "Silver sales data")

# --------------------------------------------------
# Create / update dim_customer with SCD Type 2
# --------------------------------------------------

new_customers = silver_df.select(
    "customer_id",
    "customer_name",
    "city",
    "country"
).dropDuplicates(["customer_id"])

try:
    current_dim_customer = read_delta(spark, paths.DIM_CUSTOMER_PATH)
    dim_customer_exists = True
except Exception:
    dim_customer_exists = False

if not dim_customer_exists:
    dim_customer = new_customers \
        .withColumn("customer_key", monotonically_increasing_id()) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))

else:
    current_records = current_dim_customer.filter(col("is_current") == True)

    changed_customers = new_customers.alias("new") \
        .join(
            current_records.alias("old"),
            on="customer_id",
            how="inner"
        ) \
        .filter(
            (col("new.customer_name") != col("old.customer_name")) |
            (col("new.city") != col("old.city")) |
            (col("new.country") != col("old.country"))
        ) \
        .select(
            col("new.customer_id"),
            col("new.customer_name"),
            col("new.city"),
            col("new.country")
        )

    expired_records = current_dim_customer.alias("dim") \
        .join(
            changed_customers.select("customer_id"),
            on="customer_id",
            how="inner"
        ) \
        .filter(col("dim.is_current") == True) \
        .select("dim.*") \
        .withColumn("is_current", lit(False)) \
        .withColumn("end_date", current_date())

    new_versions = changed_customers \
        .withColumn("customer_key", monotonically_increasing_id()) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))

    new_only_customers = new_customers.alias("new") \
        .join(
            current_dim_customer.select("customer_id").distinct().alias("old"),
            on="customer_id",
            how="left_anti"
        )

    new_records = new_only_customers \
        .withColumn("customer_key", monotonically_increasing_id()) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))

    unchanged_records = current_dim_customer.alias("dim") \
        .join(
            changed_customers.select("customer_id").alias("chg"),
            on="customer_id",
            how="left_anti"
        )

    dim_customer = unchanged_records \
        .unionByName(expired_records) \
        .unionByName(new_versions) \
        .unionByName(new_records)

write_delta(
    df=dim_customer,
    path=paths.DIM_CUSTOMER_PATH,
    mode="overwrite"
)

# --------------------------------------------------
# Create dim_product
# --------------------------------------------------

dim_product = silver_df.select(
    "product_id",
    "product_name",
    "category"
).dropDuplicates(["product_id"]) \
 .withColumn("product_key", monotonically_increasing_id())

write_delta(
    df=dim_product,
    path=paths.DIM_PRODUCT_PATH,
    mode="overwrite"
)

# --------------------------------------------------
# Create dim_date
# --------------------------------------------------

dim_date = silver_df.select("order_date") \
    .dropDuplicates() \
    .withColumn("year", year(col("order_date"))) \
    .withColumn("month", month(col("order_date"))) \
    .withColumn("day", dayofmonth(col("order_date"))) \
    .withColumn("date_key", monotonically_increasing_id())

write_delta(
    df=dim_date,
    path=paths.DIM_DATE_PATH,
    mode="overwrite"
)

# --------------------------------------------------
# Create fact_sales
# --------------------------------------------------

dim_customer_current = read_delta(spark, paths.DIM_CUSTOMER_PATH) \
    .filter(col("is_current") == True)

fact_base = silver_df.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "total_amount"
)

fact_sales = fact_base \
    .join(
        dim_customer_current.select("customer_id", "customer_key"),
        on="customer_id",
        how="left"
    ) \
    .join(
        dim_product.select("product_id", "product_key"),
        on="product_id",
        how="left"
    ) \
    .join(
        dim_date.select("order_date", "date_key"),
        on="order_date",
        how="left"
    ) \
    .select(
        "order_id",
        "customer_key",
        "product_key",
        "date_key",
        "quantity",
        "unit_price",
        "total_amount"
    )

write_delta_partitioned(
    df=fact_sales,
    path=paths.FACT_SALES_PATH,
    partition_col=config.FACT_PARTITION_COLUMN,
    mode="overwrite"
)

print("[INFO] Gold layer created successfully.")