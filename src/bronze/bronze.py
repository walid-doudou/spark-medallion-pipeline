from pyspark.sql.functions import month
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException

__all__ = ["bronze_run"]

PATH = "s3a://nyc-taxi/bronze/trips"


def bronze_run(spark: SparkSession, df: DataFrame) -> None:
    """Generate Raw Data [Bronze]"""

    df = df.withColumn("month", month("tpep_pickup_datetime"))

    try:
        delta_table = DeltaTable.forPath(spark, PATH)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.tpep_pickup_datetime = source.tpep_pickup_datetime",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Bronze table has been updated")

    except AnalysisException:
        (df.write.format("delta").mode("overwrite").partitionBy("month").save(PATH))
        print("Bronze table has been created")
