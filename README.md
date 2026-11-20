# bank-lakehouse

A local lakehouse for a synthetic retail bank: CDC events flow through a
Medallion architecture (bronze → silver) on Delta Lake, with quarantine,
SCD Type 2 history, and data quality checks. Runs on a laptop, no cloud needed.

```
landing/ (CDC JSON files)
   │  Spark Structured Streaming, checkpointed
   ▼
BRONZE  customers_cdc ──── bad lines ──▶ customers_quarantine (with reason)
   │  latest event per customer + Delta MERGE INTO
   ▼
SILVER  customers (SCD Type 2: valid_from / valid_to / is_current)
   │
   ▼
QUALITY checks (error / warn) ──▶ results saved as a Delta table
```

## Setup

You need **Python 3.10–3.12** and **Java 17 or 21** (`java -version` to check).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

The first Spark run downloads the Delta Lake jars from Maven, so it needs internet.

## Run it

```bash
make demo        # or run the steps below one by one
make test        # unit tests
```

Without `make` (e.g. on Windows):

```bash
python -m lakehouse.generate_data                 # batch 1: initial load
python -m lakehouse.bronze
python -m lakehouse.silver
python -m lakehouse.generate_data --changes 50    # batch 2: changes
python -m lakehouse.bronze                        # only reads the NEW file
python -m lakehouse.silver                        # closes old rows, adds new versions
python -m lakehouse.quality
```

## Things to try (and talk about in interviews)

- **Run `bronze` twice in a row.** The second run loads nothing: the checkpoint
  remembers which files were processed.
- **Run `silver` twice in a row.** Row counts don't change: the MERGE only acts
  when an event is newer *and* different (compared by hash). This is idempotency.
- **Look at a customer's history** after batch 2:
  ```python
  from lakehouse.spark import get_spark
  spark = get_spark()
  df = spark.read.format("delta").load("data/lake/silver/customers")
  df.where("is_current = false").show()   # closed (historical) versions
  ```
- **Inspect the quarantine:** `data/lake/bronze/customers_quarantine` holds each
  bad line with a reason (`malformed_json`, `unknown_op`, `missing_customer_id`).
- **Delta time travel:** `spark.read.format("delta").option("versionAsOf", 0).load(...)`.

## Design decisions

- **Quarantine instead of fail-fast**: one bad record shouldn't stop a bank's
  pipeline, but it must never disappear silently either.
- **ANSI mode on**: Spark raises errors on bad casts instead of quietly
  returning null.
- **Idempotent writes**: `txnAppId`/`txnVersion` on bronze writes, hash
  comparison in the silver MERGE, so retries never duplicate data.
- **Known simplification**: if one silver run sees several changes for the same
  customer, only the latest becomes a history row.

## Roadmap

- [x] Phase 1: CDC generator, bronze streaming + quarantine, silver SCD2, quality checks
- [ ] Phase 2: second table (transactions), export silver to Parquet, dbt + DuckDB marts
- [ ] Phase 3: metrics table for every run + LLM incident summary + RAG over docs
- [ ] Phase 4: Kafka (Redpanda) transport, GitHub Actions CI
