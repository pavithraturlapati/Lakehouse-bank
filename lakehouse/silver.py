"""Silver layer: turn CDC events into a customer table with full history (SCD Type 2).

SCD Type 2 means we never overwrite a customer's old values. When something
changes, the current row is closed (valid_to set, is_current = false) and a new
row is inserted. So you can answer "what was this customer's city on 3 March?".

    customer_id | city    | valid_from | valid_to   | is_current
    C000001     | Austin  | 2026-01-01 | 2026-03-10 | false
    C000001     | Tampa   | 2026-03-10 | null       | true

The job is idempotent: running it twice on the same bronze data changes nothing,
because a row is only closed when the incoming event is newer AND different.

Usage:
    python -m lakehouse.silver
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from lakehouse.config import BRONZE_PATH, SILVER_PATH
from lakehouse.spark import get_spark

ATTRIBUTES = ["full_name", "email", "city", "segment"]

SILVER_SCHEMA = StructType(
    [StructField("customer_id", StringType(), nullable=False)]
    + [StructField(a, StringType()) for a in ATTRIBUTES]
    + [
        StructField("row_hash", StringType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
        StructField("closed_by_delete", BooleanType()),
    ]
)


def latest_events(bronze: DataFrame) -> DataFrame:
    """Keep only the newest event per customer, and hash its attributes.

    Simplification: if one run sees several changes for the same customer, only
    the last one becomes a history row. (Production upgrade: apply them in order.)
    """
    flat = bronze.select(
        "customer_id",
        "op",
        "event_ts",
        "_ingested_at",
        *[F.col(f"after.{a}").alias(a) for a in ATTRIBUTES],
    )
    newest_first = Window.partitionBy("customer_id").orderBy(
        F.col("event_ts").desc(), F.col("_ingested_at").desc()
    )
    return (
        flat.withColumn("_rn", F.row_number().over(newest_first))
        .where("_rn = 1")
        .drop("_rn", "_ingested_at")
        .withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(a), F.lit("")) for a in ATTRIBUTES]), 256),
        )
    )


def build_staged(updates: DataFrame, current: DataFrame) -> DataFrame:
    """The classic SCD2 trick: one MERGE can either update OR insert per source
    row, but a change needs both (close old row + insert new row). So changed
    customers appear twice in the staged data:
      * merge_key = customer_id -> matches the current row and closes it
      * merge_key = NULL        -> never matches, so it is inserted as the new version
    """
    u, c = updates.alias("u"), current.alias("c")
    changed = (
        u.join(c, F.col("u.customer_id") == F.col("c.customer_id"))
        .where(
            (F.col("u.op") != "d")
            & (F.col("u.event_ts") > F.col("c.valid_from"))
            & (F.col("u.row_hash") != F.col("c.row_hash"))
        )
        .select("u.*")
        .withColumn("merge_key", F.lit(None).cast("string"))
    )
    keyed = updates.withColumn("merge_key", F.col("customer_id"))
    return keyed.unionByName(changed)


def main() -> None:
    spark = get_spark("silver")

    (
        DeltaTable.createIfNotExists(spark)
        .location(str(SILVER_PATH))
        .addColumns(SILVER_SCHEMA)
        .execute()
    )
    silver = DeltaTable.forPath(spark, str(SILVER_PATH))

    updates = latest_events(spark.read.format("delta").load(str(BRONZE_PATH)))
    current = silver.toDF().where("is_current")
    staged = build_staged(updates, current)

    (
        silver.alias("t")
        .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
        # Close the current row when the event is newer and either a delete or a real change.
        .whenMatchedUpdate(
            condition="s.event_ts > t.valid_from AND (s.op = 'd' OR t.row_hash <> s.row_hash)",
            set={
                "is_current": "false",
                "valid_to": "s.event_ts",
                "closed_by_delete": "s.op = 'd'",
            },
        )
        # Insert brand-new customers and new versions of changed ones.
        .whenNotMatchedInsert(
            condition="s.op <> 'd'",
            values={
                "customer_id": "s.customer_id",
                **{a: f"s.{a}" for a in ATTRIBUTES},
                "row_hash": "s.row_hash",
                "valid_from": "s.event_ts",
                "valid_to": "CAST(NULL AS TIMESTAMP)",
                "is_current": "true",
                "closed_by_delete": "false",
            },
        )
        .execute()
    )

    result = spark.read.format("delta").load(str(SILVER_PATH))
    total = result.count()
    current_rows = result.where("is_current").count()
    print(f"silver: {total} rows total, {current_rows} current customers, "
          f"{total - current_rows} historical versions")


if __name__ == "__main__":
    main()
