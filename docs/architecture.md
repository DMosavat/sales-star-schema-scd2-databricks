# Architecture

## Overview

This project implements an end-to-end sales data engineering pipeline using PySpark, Delta Lake, and Databricks.

The pipeline follows the Medallion Architecture pattern:

```text
Raw CSV
  ↓
Bronze Layer
  ↓
Silver Layer
  ↓
Gold Layer
  ↓
Validation
  ↓
Delta Optimization
Bronze Layer

The Bronze layer stores raw sales data in Delta format.

Purpose
Preserve source data
Avoid business transformations
Provide a reliable ingestion layer
Convert raw CSV into Delta format
Input
data/raw/sales_raw.csv
Output
data/bronze/sales_raw
Silver Layer

The Silver layer contains cleaned and standardized sales data.

Transformations
Parse order_date
Cast quantity to integer
Cast unit_price to double
Create total_amount
Remove duplicate records
Validate required business keys
Validate positive numeric values
Output
data/silver/sales_clean
Gold Layer

The Gold layer contains analytics-ready tables modeled as a Star Schema.

Tables
fact_sales
dim_customer
dim_product
dim_date
Star Schema

The fact table stores measurable sales events.

The dimension tables store descriptive business context.

dim_customer ┐
dim_product  ├── fact_sales
dim_date     ┘
Fact Table
fact_sales

Contains:

order_id
customer_key
product_key
date_key
quantity
unit_price
total_amount

The fact table does not store descriptive attributes such as customer name, city, product name, or category.

Dimension Tables
dim_customer

Tracks customer attributes using Slowly Changing Dimension Type 2.

Columns:

customer_key
customer_id
customer_name
city
country
start_date
end_date
is_current
dim_product

Stores product information.

Columns:

product_key
product_id
product_name
category
dim_date

Stores date attributes.

Columns:

date_key
order_date
year
month
day
Slowly Changing Dimension Type 2

dim_customer uses SCD Type 2 to preserve customer history.

When a customer attribute changes:

The old record is expired.
is_current is set to false.
end_date is populated.
A new record is inserted.
The new record has is_current = true.

This allows historical analysis without losing previous customer information.

Validation Layer

The validation step checks:

Row count consistency between Silver and Fact
Null foreign keys in fact_sales
Unique product keys
Unique date keys
Only one current customer record per customer_id
Expired SCD records must have end_date
Current SCD records must not have end_date
Delta Optimization

The Gold layer can be optimized using Databricks Delta features.

Partitioning

fact_sales is partitioned by:

date_key
OPTIMIZE

Used to compact small files.

ZORDER

Recommended for common filter columns:

product_key
customer_key

Example:

OPTIMIZE delta.`data/gold/fact_sales`
ZORDER BY (product_key, customer_key);
Databricks Workflow

Recommended task order:

01_bronze_ingestion
  ↓
02_silver_transformation
  ↓
03_gold_star_schema
  ↓
04_validation
  ↓
05_delta_optimization
Scheduling

Recommended schedule:

Daily at 02:00 Europe/Stockholm
Retry Policy

Recommended retry configuration:

2 retries
5 minutes interval
Notes

Delta OPTIMIZE and ZORDER require Databricks Runtime.

Local Spark execution may not support those commands.
```
