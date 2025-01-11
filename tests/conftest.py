import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Fixture to create a Spark session."""
    spark = SparkSession.builder \
        .appName("Pytest-Spark") \
        .master("local[2]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    yield spark
    spark.stop()
