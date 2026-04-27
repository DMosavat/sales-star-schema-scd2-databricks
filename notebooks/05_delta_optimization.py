# Databricks notebook source

import sys
sys.path.append(".")

from src.config import Paths

paths = Paths()

spark.sql(
    f"OPTIMIZE delta.`{paths.FACT_SALES_PATH}` "
    "ZORDER BY (product_key, customer_key)"
)

spark.sql(f"OPTIMIZE delta.`{paths.DIM_CUSTOMER_PATH}`")
spark.sql(f"OPTIMIZE delta.`{paths.DIM_PRODUCT_PATH}`")
spark.sql(f"OPTIMIZE delta.`{paths.DIM_DATE_PATH}`")

print("[INFO] Delta optimization completed successfully.")