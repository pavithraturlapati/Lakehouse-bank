"""Data quality checks on bronze and silver.

Each check has a severity:
  * error -> the data is wrong (e.g. two "current" rows for one customer). Exit code 1.
  * warn  -> worth looking at, but the pipeline can continue (e.g. a few bad emails).

Results are also appended to a Delta table, so quality becomes data you can
trend over time, not just console output.

Usage:
    python -m lakehouse.quality
"""
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import BRONZE_PATH, QUALITY_PATH, QUARANTINE_PATH, SILVER_PATH
from lakehouse.spark import get_spark

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
MAX_QUARANTINE_RATE = 0.05


def run_checks(bronze: DataFrame, quarantine: DataFrame, silver: DataFrame) -> list[dict]:
    current = silver.where("is_current")
    n_good, n_bad = bronze.count(), quarantine.count()
    quarantine_rate = n_bad / (n_good + n_bad) if (n_good + n_bad) else 0.0

    def check(name, severity, failed, detail=""):
        return {"check": name, "severity": severity, "failed_rows": int(failed),
                "passed": failed == 0, "detail": detail}

    return [
        check(
            "silver_one_current_row_per_customer", "error",
            current.groupBy("customer_id").count().where("count > 1").count(),
        ),
        check(
            "silver_customer_id_not_null", "error",
            silver.where(F.col("customer_id").isNull()).count(),
        ),
        check(
            "silver_valid_to_after_valid_from", "error",
            silver.where(F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))).count(),
        ),
        check(
            "silver_current_rows_have_no_valid_to", "error",
            current.where(F.col("valid_to").isNotNull()).count(),
        ),
        check(
            "silver_email_format", "warn",
            current.where(~F.col("email").rlike(EMAIL_PATTERN) | F.col("email").isNull()).count(),
        ),
        check(
            "bronze_quarantine_rate", "warn",
            n_bad if quarantine_rate > MAX_QUARANTINE_RATE else 0,
            f"{n_bad} of {n_good + n_bad} events quarantined ({quarantine_rate:.1%})",
        ),
    ]


def save_results(spark: SparkSession, results: list[dict]) -> None:
    run_ts = datetime.now(timezone.utc)
    rows = [{**r, "run_ts": run_ts} for r in results]
    spark.createDataFrame(rows).write.format("delta").mode("append").save(str(QUALITY_PATH))


def main() -> None:
    spark = get_spark("quality")
    load = lambda p: spark.read.format("delta").load(str(p))  # noqa: E731
    results = run_checks(load(BRONZE_PATH), load(QUARANTINE_PATH), load(SILVER_PATH))
    save_results(spark, results)

    for r in results:
        status = "PASS" if r["passed"] else ("FAIL" if r["severity"] == "error" else "WARN")
        print(f"[{status:4}] {r['check']:40} failed_rows={r['failed_rows']:<5} {r['detail']}")

    if any(not r["passed"] and r["severity"] == "error" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
