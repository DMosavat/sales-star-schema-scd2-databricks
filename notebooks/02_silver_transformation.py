# Databricks notebook source

import sys
sys.path.append(".")

from pyspark.sql.functions import col, to_date, current_timestamp, row_number
from pyspark.sql.window import Window

from src.config import Paths, JobConfig
from src.utils import (
    read_delta,
    write_delta,
    validate_not_null,
    validate_positive_values,
    log_dataframe_count
)

paths = Paths()
config = JobConfig()

bronze_df = read_delta(spark, paths.BRONZE_SALES_PATH)

log_dataframe_count(bronze_df, "Bronze sales data")

silver_df = bronze_df.withColumn(
    "order_date",
    to_date(col("order_date"), config.DATE_FORMAT)
).withColumn(
    "quantity",
    col("quantity").cast("int")
).withColumn(
    "unit_price",
    col("unit_price").cast("double")
).withColumn(
    "ingestion_time",
    current_timestamp()
)

silver_df = silver_df.withColumn(
    "total_amount",
    col("quantity") * col("unit_price")
)

validate_not_null(
    silver_df,
    ["order_id", "customer_id", "product_id", "order_date"],
    "Silver sales data"
)

validate_positive_values(
    silver_df,
    ["quantity", "unit_price"],
    "Silver sales data"
)

window_spec = Window.partitionBy(
    "order_id",
    "product_id"
).orderBy(
    col("ingestion_time").desc()
)

silver_df = silver_df.withColumn(
    "row_num",
    row_number().over(window_spec)
).filter(
    col("row_num") == 1
).drop("row_num")

log_dataframe_count(silver_df, "Silver sales data after deduplication")

write_delta(
    df=silver_df,
    path=paths.SILVER_SALES_PATH,
    mode="overwrite"
)

print("[INFO] Silver transformation completed successfully.")