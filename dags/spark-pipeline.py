from airflow.decorators import dag, task
from datetime import datetimemais

@dag(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def nyc_taxi_pipeline():

    @task
    def bronze():
        from bronze import bronze_run
        from config import get_spark_session
        spark = get_spark_session()
        df = spark.read.parquet("data/raw/*.parquet")
        bronze_run(spark, df)

    @task
    def silver():
        from silver import silver_run
        from config import get_spark_session
        spark = get_spark_session()
        silver_run(spark)

    @task
    def gold():
        from gold import gold_run
        from config import get_spark_session
        spark = get_spark_session()
        gold_run(spark)

    bronze() >> silver() >> gold()

nyc_taxi_pipeline()