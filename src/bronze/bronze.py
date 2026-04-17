from pyspark.sql.functions import month
from pyspark.sql import SparkSession

__all__ = ["bronze_run"]


def bronze_run(spark: SparkSession) -> None:
    """Generate Raw Data"""

    df = spark.read.parquet("data/raw/*.parquet")

    df = df.withColumn("month", month("tpep_pickup_datetime"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("month")
        .save("s3a://nyc-taxi/bronze/trips")
    )

    print(f"✓ Bronze layer written — {df.count()} rows")
