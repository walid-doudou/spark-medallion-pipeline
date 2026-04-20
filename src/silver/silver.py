from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
import logging

__all__ = ["silver_run"]

PATH = "s3a://nyc-taxi/silver/trips"

logger = logging.getLogger(__name__)


def silver_run(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Clean and validate data [Silver]"""

    df = (
        df.dropna(
            subset=[
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "trip_distance",
                "fare_amount",
                "passenger_count",
            ]
        )
        .filter(df.trip_distance > 0)
        .filter(df.fare_amount > 0)
        .filter(df.passenger_count >= 1)
        .filter(df.tpep_dropoff_datetime > df.tpep_pickup_datetime)
        .filter(df.year.isin("2024"))
        .filter(df.month.isin([1, 2, 3]))
    )

    try:
        delta_table = DeltaTable.forPath(spark, PATH)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.tpep_pickup_datetime = source.tpep_pickup_datetime "
                "AND target.VendorID = source.VendorID "
                "AND target.tpep_dropoff_datetime = source.tpep_dropoff_datetime",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Silver table has been updated")

    except AnalysisException:
        (df.write.format("delta").mode("overwrite").partitionBy("month").save(PATH))
        logger.info("Silver table has been created")

    return df
