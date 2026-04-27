import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for tests."""
    spark_session = SparkSession.builder \
        .appName("Data Quality Tests") \
        .master("local[*]") \
        .getOrCreate()

    yield spark_session

    spark_session.stop()


def test_sales_raw_has_required_columns(spark):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load("data/raw/sales_raw.csv")

    required_columns = {
        "order_id",
        "order_date",
        "customer_id",
        "customer_name",
        "city",
        "country",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price"
    }

    assert required_columns.issubset(set(df.columns))


def test_sales_raw_has_no_null_business_keys(spark):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load("data/raw/sales_raw.csv")

    invalid_count = df.filter(
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("product_id").isNull()
    ).count()

    assert invalid_count == 0


def test_sales_raw_has_positive_quantity_and_price(spark):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load("data/raw/sales_raw.csv")

    df = df.withColumn("quantity", col("quantity").cast("int")) \
           .withColumn("unit_price", col("unit_price").cast("double"))

    invalid_count = df.filter(
        (col("quantity") <= 0) |
        (col("unit_price") <= 0)
    ).count()

    assert invalid_count == 0


def test_order_product_combination_is_unique(spark):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load("data/raw/sales_raw.csv")

    duplicate_count = df.groupBy("order_id", "product_id") \
        .count() \
        .filter(col("count") > 1) \
        .count()

    assert duplicate_count == 0