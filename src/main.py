from bronze import bronze_run
from config import get_spark_session


class Perform():
    """Run the process 
    """
    
    def __init__(self):
        self.spark = get_spark_session()
        self.run()
        
    def run(self):
        bronze = bronze_run(self.spark)
        #silver = silver_run(self.spark)
        #gold = gold_run(self.spark)
    