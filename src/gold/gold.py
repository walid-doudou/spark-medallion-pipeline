from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, count, hour, col
import logging

logger = logging.getLogger(__name__)

__all__ = ["gold_run"]

PATH_REVENUE_BY_DAY = "s3a://nyc-taxi/gold/revenue_by_day"
PATH_REVENUE_BY_VENDOR = "s3a://nyc-taxi/gold/revenue_by_vendor"
PATH_HOURLY_TRIPS = "s3a://nyc-taxi/gold/hourly_trips"


def _write_delta(df: DataFrame, path: str) -> None:
    """Write a DataFrame to a Delta table, creating or overwriting."""

    df.write.format("delta").mode("overwrite").save(path)


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
    logger.info("Gold table revenue_by_day written")

    _write_delta(revenue_by_vendor, PATH_REVENUE_BY_VENDOR)
    logger.info("Gold table revenue_by_vendor written")

    _write_delta(hourly_trips, PATH_HOURLY_TRIPS)
    logger.info("Gold table hourly_trips written")

    df.unpersist()
