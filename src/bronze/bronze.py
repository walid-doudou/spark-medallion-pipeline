from config.spark_session import get_spark_session
from pyspark.sql.functions import month

__all__ = ["bronze_run"]


def bronze_run() -> None:
    """Generate Raw Data"""
    spark = get_spark_session()
    df = spark.read.parquet("data/raw/*.parquet")

    df = df.withColumn("month", month("tpep_pickup_datetime"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("month")
        .save("s3a://nyc-taxi/bronze/trips")
    )

    print(f"✓ Bronze layer written — {df.count()} rows")
