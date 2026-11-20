from datetime import datetime

from lakehouse.silver import SILVER_SCHEMA, build_staged, latest_events

T1, T2, T3 = (datetime(2026, 1, d) for d in (1, 2, 3))


def bronze(spark, rows):
    """rows: (customer_id, op, event_ts, city)"""
    data = [(cid, op, ts, ts, None if op == "d" else
             {"full_name": "A", "email": "a@x.com", "city": city, "segment": "retail"})
            for cid, op, ts, city in rows]
    return spark.createDataFrame(
        data,
        "customer_id string, op string, event_ts timestamp, _ingested_at timestamp, "
        "after struct<full_name:string,email:string,city:string,segment:string>",
    )


def current_from(spark, updates_df):
    """Pretend silver currently holds exactly these updates as current rows."""
    rows = [(r.customer_id, r.full_name, r.email, r.city, r.segment, r.row_hash,
             r.event_ts, None, True, False) for r in updates_df.collect()]
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def test_latest_event_wins(spark):
    out = latest_events(bronze(spark, [("C1", "c", T1, "Austin"), ("C1", "u", T2, "Tampa")])).collect()
    assert len(out) == 1 and out[0]["city"] == "Tampa"


def test_new_customer_staged_once(spark):
    updates = latest_events(bronze(spark, [("C1", "c", T1, "Austin")]))
    empty = spark.createDataFrame([], SILVER_SCHEMA)
    staged = build_staged(updates, empty).collect()
    assert [r["merge_key"] for r in staged] == ["C1"]


def test_changed_customer_staged_twice(spark):
    current = current_from(spark, latest_events(bronze(spark, [("C1", "c", T1, "Austin")])))
    updates = latest_events(bronze(spark, [("C1", "u", T2, "Tampa")]))
    keys = sorted(build_staged(updates, current).collect(), key=lambda r: r["merge_key"] or "")
    assert [r["merge_key"] for r in keys] == [None, "C1"]


def test_rerun_with_same_data_stages_no_new_version(spark):
    updates = latest_events(bronze(spark, [("C1", "c", T1, "Austin")]))
    staged = build_staged(updates, current_from(spark, updates)).collect()
    assert all(r["merge_key"] is not None for r in staged)


def test_delete_never_inserts_new_version(spark):
    current = current_from(spark, latest_events(bronze(spark, [("C1", "c", T1, "Austin")])))
    updates = latest_events(bronze(spark, [("C1", "d", T3, None)]))
    staged = build_staged(updates, current).collect()
    assert all(r["merge_key"] is not None for r in staged)
