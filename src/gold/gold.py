import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, hour, sum

logger = logging.getLogger(__name__)

__all__ = ["gold_run"]

PATH_REVENUE_BY_DAY = "s3a://nyc-taxi/gold/revenue_by_day"
PATH_REVENUE_BY_VENDOR = "s3a://nyc-taxi/gold/revenue_by_vendor"
PATH_HOURLY_TRIPS = "s3a://nyc-taxi/gold/hourly_trips"

_PG_HOST = os.getenv("POSTGRES_GOLD_HOST", "localhost")
_PG_PORT = os.getenv("POSTGRES_GOLD_PORT", "5432")
_PG_DB = os.getenv("POSTGRES_GOLD_DB", "gold")
_PG_USER = os.getenv("POSTGRES_GOLD_USER", "gold")
_PG_PASSWORD = os.getenv("POSTGRES_GOLD_PASSWORD", "gold")
_JDBC_URL = f"jdbc:postgresql://{_PG_HOST}:{_PG_PORT}/{_PG_DB}"
_JDBC_PROPS = {
    "user": _PG_USER,
    "password": _PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def _write_delta(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def _write_postgres(df: DataFrame, table: str) -> None:
    df.write.jdbc(url=_JDBC_URL, table=table, mode="overwrite", properties=_JDBC_PROPS)


def gold_run(spark: SparkSession, df: DataFrame) -> None:
    """Generate aggregated Gold tables."""

    df = df.cache()

    # Total revenue per day
    revenue_by_day = (
        df.groupBy(col("tpep_pickup_datetime").cast("date").alias("date"))
        .agg(sum("total_amount").alias("total_revenue"))
        .orderBy("date")
    )

    # Total revenue per vendor
    revenue_by_vendor = (
        df.groupBy(col("VendorID"))
        .agg(sum("total_amount").alias("total"))
        .orderBy("VendorID")
    )

    # Number of trips per hour
    hourly_trips = (
        df.groupBy(hour("tpep_pickup_datetime").alias("hour"))
        .agg(count("*").alias("trip_count"))
        .orderBy("hour")
    )

    _write_delta(revenue_by_day, PATH_REVENUE_BY_DAY)
    _write_postgres(revenue_by_day, "revenue_by_day")
    logger.info("Gold table revenue_by_day written")

    _write_delta(revenue_by_vendor, PATH_REVENUE_BY_VENDOR)
    _write_postgres(revenue_by_vendor, "revenue_by_vendor")
    logger.info("Gold table revenue_by_vendor written")

    _write_delta(hourly_trips, PATH_HOURLY_TRIPS)
    _write_postgres(hourly_trips, "hourly_trips")
    logger.info("Gold table hourly_trips written")

    df.unpersist()
