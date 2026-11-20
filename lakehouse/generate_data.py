"""Generate fake CDC (change data capture) events for a bank's `customers` table.

Each line in the output file is one change event in a Debezium-style envelope:
    {"op": "c|u|d", "ts_ms": ..., "source": {...}, "before": {...}, "after": {...}}
    op: c = create (insert), u = update, d = delete

First run  -> initial load: every customer arrives as a "c" event.
Later runs -> a mix of updates, new customers, and a few deletes.
About 2% of lines are deliberately corrupted so the bronze quarantine has work to do.

Usage:
    python -m lakehouse.generate_data                  # initial load (200 customers)
    python -m lakehouse.generate_data --changes 50     # next batch of changes
"""
import argparse
import copy
import json
import random
from datetime import datetime, timezone

from faker import Faker

from lakehouse.config import LANDING_DIR, STATE_FILE

SEGMENTS = ["retail", "premier", "private"]
START_CLOCK_MS = 1_767_225_600_000  # 2026-01-01 00:00:00 UTC; simulated clock


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"batch": 0, "next_id": 1, "clock_ms": START_CLOCK_MS, "customers": {}}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def tick(state: dict, rng: random.Random) -> int:
    """Advance the simulated clock by a few seconds and return the new time."""
    state["clock_ms"] += rng.randint(1_000, 60_000)
    return state["clock_ms"]


def iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def new_customer(fake: Faker, rng: random.Random, customer_id: str, ts_ms: int) -> dict:
    return {
        "customer_id": customer_id,
        "full_name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "segment": rng.choice(SEGMENTS),
        "updated_at": iso(ts_ms),
    }


def envelope(op: str, before, after, ts_ms: int) -> dict:
    return {
        "op": op,
        "ts_ms": ts_ms,
        "source": {"db": "bank", "table": "customers"},
        "before": before,
        "after": after,
    }


def corrupt(event: dict, rng: random.Random) -> str:
    """Break an event in one of three realistic ways."""
    kind = rng.choice(["truncated", "missing_id", "bad_op"])
    if kind == "truncated":
        line = json.dumps(event)
        return line[: len(line) // 2]  # half a JSON object: unparseable
    broken = copy.deepcopy(event)
    if kind == "missing_id":
        for side in ("before", "after"):
            if broken[side]:
                broken[side].pop("customer_id", None)
    else:
        broken["op"] = "x"
    return json.dumps(broken)


def make_events(state: dict, fake: Faker, rng: random.Random, initial: int, changes: int):
    customers = state["customers"]
    events = []

    def create():
        cid = f"C{state['next_id']:06d}"
        state["next_id"] += 1
        ts = tick(state, rng)
        row = new_customer(fake, rng, cid, ts)
        customers[cid] = row
        events.append(envelope("c", None, row, ts))

    if not customers:
        for _ in range(initial):
            create()
        return events

    for _ in range(changes):
        roll = rng.random()
        if roll < 0.75 and customers:  # update an existing customer
            cid = rng.choice(sorted(customers))
            before = customers[cid]
            after = dict(before)
            field = rng.choice(["email", "city", "segment"])
            after[field] = {
                "email": fake.email,
                "city": fake.city,
                "segment": lambda: rng.choice(SEGMENTS),
            }[field]()
            ts = tick(state, rng)
            after["updated_at"] = iso(ts)
            customers[cid] = after
            events.append(envelope("u", before, after, ts))
        elif roll < 0.95:  # brand-new customer
            create()
        elif customers:  # delete (customer closes their relationship)
            cid = rng.choice(sorted(customers))
            before = customers.pop(cid)
            events.append(envelope("d", before, None, tick(state, rng)))
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--initial", type=int, default=200, help="customers in first load")
    parser.add_argument("--changes", type=int, default=50, help="events in later batches")
    parser.add_argument("--corrupt-rate", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    state = load_state()
    state["batch"] += 1
    batch_seed = args.seed + state["batch"]  # deterministic, but different per batch
    rng = random.Random(batch_seed)
    fake = Faker()
    Faker.seed(batch_seed)

    events = make_events(state, fake, rng, args.initial, args.changes)

    lines, n_bad = [], 0
    for event in events:
        if rng.random() < args.corrupt_rate:
            lines.append(corrupt(event, rng))
            n_bad += 1
        else:
            lines.append(json.dumps(event))

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    out = LANDING_DIR / f"customers_batch_{state['batch']:04d}.json"
    out.write_text("\n".join(lines) + "\n")
    save_state(state)
    print(f"batch {state['batch']}: wrote {len(lines)} events ({n_bad} corrupted) -> {out.name}")


if __name__ == "__main__":
    main()
