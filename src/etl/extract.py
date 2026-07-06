"""Extract layer: read raw sources into Spark DataFrames with explicit schemas."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_ts", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]
)


def read_orders(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=ORDERS_SCHEMA)


def read_customers(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=CUSTOMERS_SCHEMA)
