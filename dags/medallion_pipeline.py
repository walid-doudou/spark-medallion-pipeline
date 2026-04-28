from __future__ import annotations

import sys

sys.path.insert(0, "/opt/airflow/src")

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

RAW_PATH = "/opt/airflow/data/raw/*.parquet"
BRONZE_PATH = "s3a://nyc-taxi/bronze/trips"
SILVER_PATH = "s3a://nyc-taxi/silver/trips"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_bronze() -> None:
    from bronze import bronze_run
    from config import get_spark_session

    spark = get_spark_session()
    df = spark.read.parquet(RAW_PATH)
    bronze_run(spark, df)
    spark.stop()


def run_silver() -> None:
    from config import get_spark_session
    from silver import silver_run

    spark = get_spark_session()
    df = spark.read.format("delta").load(BRONZE_PATH)
    silver_run(spark, df)
    spark.stop()


def run_gold() -> None:
    from config import get_spark_session
    from gold import gold_run

    spark = get_spark_session()
    df = spark.read.format("delta").load(SILVER_PATH)
    gold_run(spark, df)
    spark.stop()


with DAG(
    dag_id="medallion_pipeline",
    description="NYC Taxi medallion pipeline: Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["medallion", "spark", "nyc-taxi"],
) as dag:
    start = EmptyOperator(task_id="start")

    bronze = PythonOperator(task_id="bronze", python_callable=run_bronze)

    silver = PythonOperator(task_id="silver", python_callable=run_silver)

    gold = PythonOperator(task_id="gold", python_callable=run_gold)

    end = EmptyOperator(task_id="end")

    start >> bronze >> silver >> gold >> end
