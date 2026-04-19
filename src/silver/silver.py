from pyspark.sql.functions import month
from pyspark.sql import SparkSession, DataFrame

__all__ = ["silver_run"]


def silver_run(spark: SparkSession, df: DataFrame) -> None:
    """Generate Raw Data"""
    df = df.withColumn("month", month("tpep_pickup_datetime"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("month")
        .save("s3a://nyc-taxi/bronze/trips")
    )

    print(f"✓ Bronze layer written — {df.count()} rows")
