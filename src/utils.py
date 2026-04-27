from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def read_delta(spark, path: str) -> DataFrame:
    """Read a Delta table from the given path."""
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a DataFrame as Delta."""
    df.write.format("delta").mode(mode).save(path)


def write_delta_partitioned(
    df: DataFrame,
    path: str,
    partition_col: str,
    mode: str = "overwrite"
) -> None:
    """Write a partitioned Delta table."""
    df.write.format("delta") \
        .mode(mode) \
        .partitionBy(partition_col) \
        .save(path)


def validate_not_null(df: DataFrame, columns: list[str], dataset_name: str) -> None:
    """Validate that selected columns do not contain null values."""
    condition = None

    for column in columns:
        current_condition = col(column).isNull()
        condition = current_condition if condition is None else condition | current_condition

    invalid_count = df.filter(condition).count()

    if invalid_count > 0:
        raise Exception(
            f"{dataset_name} validation failed: {invalid_count} rows contain null values in {columns}"
        )


def validate_positive_values(df: DataFrame, columns: list[str], dataset_name: str) -> None:
    """Validate that selected numeric columns contain positive values."""
    condition = None

    for column in columns:
        current_condition = col(column) <= 0
        condition = current_condition if condition is None else condition | current_condition

    invalid_count = df.filter(condition).count()

    if invalid_count > 0:
        raise Exception(
            f"{dataset_name} validation failed: {invalid_count} rows contain non-positive values in {columns}"
        )


def validate_unique_key(df: DataFrame, key_columns: list[str], dataset_name: str) -> None:
    """Validate uniqueness based on one or more key columns."""
    duplicate_count = df.groupBy(key_columns) \
        .count() \
        .filter(col("count") > 1) \
        .count()

    if duplicate_count > 0:
        raise Exception(
            f"{dataset_name} validation failed: {duplicate_count} duplicate key values found for {key_columns}"
        )


def log_dataframe_count(df: DataFrame, dataset_name: str) -> int:
    """Log and return DataFrame row count."""
    count_value = df.count()
    print(f"[INFO] {dataset_name} count: {count_value}")
    return count_value