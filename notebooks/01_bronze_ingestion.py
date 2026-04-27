# Databricks notebook source

import sys
sys.path.append(".")

from src.config import Paths
from src.utils import write_delta, log_dataframe_count

paths = Paths()

raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load(paths.RAW_DATA_PATH)

log_dataframe_count(raw_df, "Raw sales data")

write_delta(
    df=raw_df,
    path=paths.BRONZE_SALES_PATH,
    mode="overwrite"
)

print("[INFO] Bronze ingestion completed successfully.")