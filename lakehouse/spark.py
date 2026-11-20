"""Creates a local Spark session with Delta Lake enabled."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "bank-lakehouse") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")  # small data, few partitions
        .config("spark.sql.ansi.enabled", "true")     # strict typing: never hide bad data
        .config("spark.sql.session.timeZone", "UTC")
    )
    # Downloads the Delta jars from Maven the first time it runs.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
