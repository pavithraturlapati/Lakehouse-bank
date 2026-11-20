import json

from lakehouse.bronze import split_records

AFTER = {"customer_id": "C1", "full_name": "A", "email": "a@x.com",
         "city": "Tampa", "segment": "retail", "updated_at": "2026-01-01T00:00:00+00:00"}


def event(op="c", ts=1_000, after=AFTER, before=None):
    return json.dumps({"op": op, "ts_ms": ts, "source": {"db": "bank", "table": "customers"},
                       "before": before, "after": after})


def run(spark, lines):
    raw = spark.createDataFrame([(l, "f.json") for l in lines], ["value", "_source_file"])
    good, bad = split_records(raw)
    return good.collect(), {r["_quarantine_reason"] for r in bad.collect()}


def test_good_event_passes(spark):
    good, bad = run(spark, [event()])
    assert len(good) == 1 and good[0]["customer_id"] == "C1" and not bad


def test_delete_uses_before_for_customer_id(spark):
    good, _ = run(spark, [event(op="d", after=None, before=AFTER)])
    assert good[0]["customer_id"] == "C1"


def test_bad_records_are_quarantined_with_reasons(spark):
    no_id = {k: v for k, v in AFTER.items() if k != "customer_id"}
    lines = [event()[:20], event(op="x"), event(after=no_id), event(op="u", after=None, before=AFTER)]
    good, reasons = run(spark, lines)
    assert not good
    assert reasons == {"malformed_json", "unknown_op", "missing_customer_id", "missing_after"}
