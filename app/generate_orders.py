#!/usr/bin/env python3
"""Deterministic OrderFlow event generator (stdlib only, no Spark needed).

Writes one JSON-lines file per day under data/raw/orders/ :
    data/raw/orders/dt=2026-10-01/events.jsonl

Each line is one event in an order's life:
    {"event_id": "...", "order_id": "...", "tenant_id": "acme",
     "event_type": "created|paid|shipped|cancelled",
     "amount": 129.99, "currency": "USD", "event_ts": "2026-10-01T09:14:03Z"}

The data is deliberately realistic, not clean. Flags switch on the messiness that
later days teach you to handle:

    --skew        one tenant ("megacorp") gets ~60% of all events   (Day 4/5 skew)
    --dupes       ~2% of events are emitted twice                    (Day 6 dedupe)
    --bad         ~1% of lines are corrupt / have missing fields      (Day 6 bad records)
    --late        ~3% of events carry an event_ts days in the past    (Day 6/7 late data)

Day 1 uses the defaults (all flags off) so the data is boring on purpose.
"""
import argparse
import datetime as dt
import json
import os
import random

TENANTS = ["acme", "globex", "initech", "umbrella", "hooli"]
TYPES = ["created", "paid", "shipped", "cancelled"]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]


def make_events(day, n_orders, rng, skew, dupes, bad, late):
    lines = []
    for i in range(n_orders):
        if skew and rng.random() < 0.60:
            tenant = "megacorp"
        else:
            tenant = rng.choice(TENANTS)
        order_id = f"{day:%Y%m%d}-{i:06d}"
        currency = rng.choice(CURRENCIES)
        amount = round(rng.uniform(5, 500), 2)
        base = dt.datetime.combine(day, dt.time()) + dt.timedelta(seconds=rng.randint(0, 86399))
        # Every order is created; most get paid; some shipped; a few cancelled.
        path = ["created"]
        if rng.random() < 0.9:
            path.append("paid")
            if rng.random() < 0.8:
                path.append("shipped")
        if rng.random() < 0.05:
            path.append("cancelled")
        for step, etype in enumerate(path):
            ts = base + dt.timedelta(minutes=step * rng.randint(1, 90))
            if late and rng.random() < 0.03:
                ts -= dt.timedelta(days=rng.randint(2, 5))
            ev = {
                "event_id": f"{order_id}-{etype}",
                "order_id": order_id,
                "tenant_id": tenant,
                "event_type": etype,
                "amount": amount,
                "currency": currency,
                "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            line = json.dumps(ev, separators=(",", ":"))
            if bad and rng.random() < 0.01:
                kind = rng.choice(["truncate", "null_amount", "no_tenant"])
                if kind == "truncate":
                    line = line[: len(line) // 2]
                elif kind == "null_amount":
                    ev["amount"] = None
                    line = json.dumps(ev, separators=(",", ":"))
                else:
                    ev.pop("tenant_id")
                    line = json.dumps(ev, separators=(",", ":"))
            lines.append(line)
            if dupes and rng.random() < 0.02:
                lines.append(line)
    rng.shuffle(lines)  # arrival order is not event order
    return lines


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw/orders")
    p.add_argument("--start", default="2026-10-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--orders-per-day", type=int, default=20000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--skew", action="store_true")
    p.add_argument("--dupes", action="store_true")
    p.add_argument("--bad", action="store_true")
    p.add_argument("--late", action="store_true")
    a = p.parse_args()

    start = dt.date.fromisoformat(a.start)
    total = 0
    for d in range(a.days):
        day = start + dt.timedelta(days=d)
        rng = random.Random(f"{a.seed}-{day.isoformat()}")  # per-day seed: stable and independent
        lines = make_events(day, a.orders_per_day, rng, a.skew, a.dupes, a.bad, a.late)
        folder = os.path.join(a.out, f"dt={day.isoformat()}")
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, "events.jsonl"), "w") as f:
            f.write("\n".join(lines) + "\n")
        total += len(lines)
    print(f"wrote {total} events over {a.days} days to {a.out}")


if __name__ == "__main__":
    main()
