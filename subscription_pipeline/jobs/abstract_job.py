from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class AbstractJob(ABC):
    
    def __init__(self, spark: SparkSession, env: str, job_name: str):
        self.spark = spark
        self.env = env
        self.job_name = job_name
        # other common configurations
    
    def execute(self, *args, **kwargs):
        # Do things before the execution of the job, e.g., 
        print(f"Executing job {self.job_name} with arguments:", args, kwargs)
        
        self._run(*args, **kwargs)
        
        # Do things after the execution of the job, e.g.,
        print("Job end") 
        
        
    @abstractmethod
    def _run(self, *args, **kwargs):
        """
        Abstract method that implements the logic of the pipeline Job
        """
        pass
    
    # Here we could define common methods like logging methods or the usage of some DQ frameworks (not implemented for the exercise)