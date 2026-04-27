# Databricks notebook source

import sys
sys.path.append(".")

from pyspark.sql.functions import col

from src.config import Paths
from src.utils import read_delta, validate_unique_key, log_dataframe_count

paths = Paths()

silver_df = read_delta(spark, paths.SILVER_SALES_PATH)
dim_customer = read_delta(spark, paths.DIM_CUSTOMER_PATH)
dim_product = read_delta(spark, paths.DIM_PRODUCT_PATH)
dim_date = read_delta(spark, paths.DIM_DATE_PATH)
fact_sales = read_delta(spark, paths.FACT_SALES_PATH)

silver_count = log_dataframe_count(silver_df, "Silver sales data")
fact_count = log_dataframe_count(fact_sales, "Fact sales")

if silver_count != fact_count:
    raise Exception(
        f"Row count validation failed: silver={silver_count}, fact={fact_count}"
    )

print("[PASS] Row count validation passed.")

null_fk_count = fact_sales.filter(
    col("customer_key").isNull() |
    col("product_key").isNull() |
    col("date_key").isNull()
).count()

if null_fk_count > 0:
    raise Exception(
        f"Foreign key validation failed: {null_fk_count} rows contain null keys"
    )

print("[PASS] Foreign key validation passed.")

validate_unique_key(
    dim_product,
    ["product_id"],
    "dim_product"
)

print("[PASS] dim_product uniqueness validation passed.")

validate_unique_key(
    dim_date,
    ["order_date"],
    "dim_date"
)

print("[PASS] dim_date uniqueness validation passed.")

current_customer_duplicates = dim_customer.filter(
    col("is_current") == True
).groupBy(
    "customer_id"
).count().filter(
    col("count") > 1
)

current_customer_duplicate_count = current_customer_duplicates.count()

if current_customer_duplicate_count > 0:
    current_customer_duplicates.show(truncate=False)
    raise Exception(
        "SCD2 validation failed: some customers have more than one current record"
    )

print("[PASS] SCD2 current-record validation passed.")

invalid_expired_records = dim_customer.filter(
    (col("is_current") == False) &
    (col("end_date").isNull())
)

invalid_expired_count = invalid_expired_records.count()

if invalid_expired_count > 0:
    invalid_expired_records.show(truncate=False)
    raise Exception(
        "SCD2 validation failed: expired records must have end_date"
    )

print("[PASS] SCD2 expired-record validation passed.")

invalid_current_records = dim_customer.filter(
    (col("is_current") == True) &
    (col("end_date").isNotNull())
)

invalid_current_count = invalid_current_records.count()

if invalid_current_count > 0:
    invalid_current_records.show(truncate=False)
    raise Exception(
        "SCD2 validation failed: current records should not have end_date"
    )

print("[PASS] SCD2 current end_date validation passed.")

print("[INFO] All validations passed successfully.")