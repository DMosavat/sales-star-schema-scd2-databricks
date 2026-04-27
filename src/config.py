from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    RAW_DATA_PATH: str = "data/raw/sales_raw.csv"

    BRONZE_SALES_PATH: str = "data/bronze/sales_raw"
    SILVER_SALES_PATH: str = "data/silver/sales_clean"

    DIM_CUSTOMER_PATH: str = "data/gold/dim_customer"
    DIM_PRODUCT_PATH: str = "data/gold/dim_product"
    DIM_DATE_PATH: str = "data/gold/dim_date"
    FACT_SALES_PATH: str = "data/gold/fact_sales"


@dataclass(frozen=True)
class JobConfig:
    APP_NAME: str = "Sales Data Engineering Pipeline"
    DATE_FORMAT: str = "yyyy-MM-dd"

    CUSTOMER_BUSINESS_KEY: str = "customer_id"
    PRODUCT_BUSINESS_KEY: str = "product_id"

    FACT_PARTITION_COLUMN: str = "date_key"