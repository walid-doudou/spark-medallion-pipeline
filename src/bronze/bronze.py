from pyspark.sql.functions import month, year
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
import logging

__all__ = ["bronze_run"]

logger = logging.getLogger(__name__)

PATH = "s3a://nyc-taxi/bronze/trips"


def bronze_run(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Generate Raw Data [Bronze]"""

    df = (
        df.withColumn("year", year("tpep_pickup_datetime"))
        .withColumn("month", month("tpep_pickup_datetime"))
        .dropDuplicates(["tpep_pickup_datetime", "VendorID", "tpep_dropoff_datetime"])
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
        logger.info("Bronze table has been updated")

    except AnalysisException:
        (df.write.format("delta").mode("overwrite").partitionBy("month").save(PATH))
        logger.info("Bronze table has been created")

    return df
