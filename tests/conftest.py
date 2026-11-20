import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Plain local Spark (no Delta needed): tests cover the pure logic."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
