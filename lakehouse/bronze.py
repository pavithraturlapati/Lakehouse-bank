"""Bronze layer: land raw CDC events in Delta Lake, quarantining bad records.

How it works:
  * Spark Structured Streaming watches the landing folder for new files.
  * A checkpoint remembers which files were already read, so re-running the job
    never loads the same file twice.
  * trigger(availableNow=True) processes everything new, then stops, so this
    behaves like a batch job you can schedule, with streaming's bookkeeping.
  * Each line is parsed. Good events go to the bronze table; bad ones go to a
    quarantine table with a named reason, instead of crashing the pipeline.

Usage:
    python -m lakehouse.bronze
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from lakehouse.config import BRONZE_CHECKPOINT, BRONZE_PATH, LANDING_DIR, QUARANTINE_PATH
from lakehouse.spark import get_spark

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("full_name", StringType()),
        StructField("email", StringType()),
        StructField("city", StringType()),
        StructField("segment", StringType()),
        StructField("updated_at", StringType()),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField(
            "source",
            StructType([StructField("db", StringType()), StructField("table", StringType())]),
        ),
        StructField("before", CUSTOMER_SCHEMA),
        StructField("after", CUSTOMER_SCHEMA),
    ]
)


def split_records(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse raw lines and split them into (good, quarantined).

    `raw` needs columns: value (the JSON line) and _source_file.
    Kept free of any I/O so it can be unit-tested.
    """
    e = F.col("event")
    parsed = raw.withColumn("event", F.from_json("value", ENVELOPE_SCHEMA)).withColumn(
        "customer_id", F.coalesce(e["after"]["customer_id"], e["before"]["customer_id"])
    )

    reason = (
        F.when(e.isNull() | e["op"].isNull(), F.lit("malformed_json"))
        .when(~e["op"].isin("c", "u", "d"), F.lit("unknown_op"))
        .when(e["ts_ms"].isNull(), F.lit("missing_ts"))
        .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
        .when(e["op"].isin("c", "u") & e["after"].isNull(), F.lit("missing_after"))
    )
    checked = parsed.withColumn("_quarantine_reason", reason).withColumn(
        "_ingested_at", F.current_timestamp()
    )

    good = checked.where(F.col("_quarantine_reason").isNull()).select(
        "customer_id",
        e["op"].alias("op"),
        F.timestamp_millis(e["ts_ms"]).alias("event_ts"),
        e["before"].alias("before"),
        e["after"].alias("after"),
        "_source_file",
        "_ingested_at",
    )
    bad = checked.where(F.col("_quarantine_reason").isNotNull()).select(
        F.col("value").alias("raw_line"),
        "_quarantine_reason",
        "_source_file",
        "_ingested_at",
    )
    return good, bad


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Called once per micro-batch. txnAppId/txnVersion make each write
    idempotent: if a batch is retried after a crash, Delta skips the duplicate."""
    batch_df.persist()
    good, bad = split_records(batch_df)
    for df, path, app in [
        (good, BRONZE_PATH, "bronze_customers"),
        (bad, QUARANTINE_PATH, "bronze_customers_quarantine"),
    ]:
        (
            df.write.format("delta")
            .mode("append")
            .option("txnAppId", app)
            .option("txnVersion", batch_id)
            .save(str(path))
        )
    print(f"  micro-batch {batch_id}: {good.count()} good, {bad.count()} quarantined")
    batch_df.unpersist()


def main() -> None:
    spark = get_spark("bronze")
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    raw = (
        spark.readStream.format("text")
        .load(str(LANDING_DIR))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    query = (
        raw.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", str(BRONZE_CHECKPOINT))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print("bronze: done")


if __name__ == "__main__":
    main()
