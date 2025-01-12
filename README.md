# Sample BI Datapipeline
This repository demonstrates a typical data pipeline written in PySpark. With this pipeline, we process subscription events represented by raw JSON files. In particular:  
- We clean and check (DQ) events and save them in a Delta table format (silver layer).  
- We model data for the business case. For the exercise, we decided to use a star schema (refer to the documentation for details on the data model).  


## Repository Structure
- 📁 `subscprition_pipeline` - The Python source code with pipeline entry points that could be launched by an orchestrator (AWS Glue, Airflow, Databricks Jobs). In this example, the 2 entry points are:  
    - `subscprition_pipeline/subscription/staging2silver.py`  
    - `subscprition_pipeline/subscription/silver2gold.py`  
- 📁 `tests` - It should contain unit and integration tests with sample data. The `conftest.py` file contains a fixture to provide `SparkSession` in tests, which is very useful for integration tests with Spark.  
- 📁 `lakehouse_ddl` - Definition of lakehouse tables and views, divided by type:  
    - `warehouse` - Fine-grained data modeled with a star schema.  
    - `mart` - Specific business case queries (typically views on the warehouse). 

### Pipeline
The pipeline could be triggered by different orchestration tools. All pipeline jobs must inherit from the `AbstractJob` class, and the orchestrator must launch the `execute` method of `AbstractJob`. This guarantees common features to be included in the process, such as logging, the usage of some DQ frameworks, Spark session initialization if needed, and so on. The initialization of `AbstractJob` requires some common configurations. Job parameters should instead be passed directly to the `execute` method.  
For this example, the parameters of the jobs are `from_month` and `to_month`; they define which data to process or reprocess. They imply the constraint that raw data must be partitioned by `year_month` (through S3 path). These parameters allow us to run the same pipeline in both cases:  
- Historical processing  
- Day-to-day processing  

It's enough to increase the Spark cluster size if you want to run the historical processing.


## Configure env locally
This guide is for Linux. If you're on Windows, it's strongly recommended to use WSL since the repository uses Apache Spark.  

- Create a Python `venv`.  
- Activate it.  
- Install required packages.  

E.g.:  
```shell
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests with the pytest command.

## CI/CD
This section should contain information on how this repository is configured for CI/CD (not implemented for the exercise).