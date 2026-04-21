from bronze import bronze_run
from silver import silver_run
from gold import gold_run

from config import get_spark_session
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")


class Perform:
    """Run the process"""

    def __init__(self) -> None:
        self.spark = get_spark_session()
        self.df = self.spark.read.parquet("data/raw/*.parquet")
        self.run()

    def run(self) -> None:
        df_bronze = bronze_run(self.spark, self.df)
        df_silver = silver_run(self.spark, df_bronze)
        gold_run(self.spark, df_silver)


Perform()
