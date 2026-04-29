import logging

from config.postgres import JDBC_PROPS, JDBC_URL
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, hour, sum

logger = logging.getLogger(__name__)

__all__ = ["gold_run"]

PATH_REVENUE_BY_DAY = "s3a://nyc-taxi/gold/revenue_by_day"
PATH_REVENUE_BY_VENDOR = "s3a://nyc-taxi/gold/revenue_by_vendor"
PATH_HOURLY_TRIPS = "s3a://nyc-taxi/gold/hourly_trips"


def _merge_delta(spark: SparkSession, df: DataFrame, path: str, key_col: str) -> None:
    try:
        delta_table = DeltaTable.forPath(spark, path)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), f"target.{key_col} = source.{key_col}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException:
        df.write.format("delta").mode("overwrite").save(path)


def _upsert_postgres(df: DataFrame, table: str) -> None:
    # truncate=true avoids DROP TABLE so Metabase never sees the table disappear
    df.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode="overwrite",
        properties={**JDBC_PROPS, "truncate": "true"},
    )


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

    _merge_delta(spark, revenue_by_day, PATH_REVENUE_BY_DAY, "date")
    _upsert_postgres(revenue_by_day, "revenue_by_day")
    logger.info("Gold table revenue_by_day written")

    _merge_delta(spark, revenue_by_vendor, PATH_REVENUE_BY_VENDOR, "VendorID")
    _upsert_postgres(revenue_by_vendor, "revenue_by_vendor")
    logger.info("Gold table revenue_by_vendor written")

    _merge_delta(spark, hourly_trips, PATH_HOURLY_TRIPS, "hour")
    _upsert_postgres(hourly_trips, "hourly_trips")
    logger.info("Gold table hourly_trips written")

    df.unpersist()
