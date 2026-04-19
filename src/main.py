from bronze import bronze_run

# from silver import silver_run
from config import get_spark_session


class Perform:
    """Run the process"""

    def __init__(self) -> None:
        self.spark = get_spark_session()
        self.df = self.spark.read.parquet("data/raw/*.parquet")
        self.run()

    def run(self) -> None:
        bronze_run(self.spark, self.df)
        # silver_run(self.spark, self.df)
        # gold = gold_run(self.spark)


Perform()
