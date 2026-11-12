# Day 9 — Airflow done right: idempotency, retries, backfills, catchup

**Goal:** by the end of today you can write DAGs that are *safe to re-run* — because in
Airflow you re-run things constantly (retries, backfills, clears, bug-fix reruns), and a
workflow that isn't safe to re-run is a data-corruption incident waiting to happen. You also
understand the **logical-date** model that confuses every beginner once.

**Assumed:** Day 8. **Not assumed:** idempotency, backfill, `catchup`, logical/execution date,
sensors, SLAs.

Three labs today, each a classic production failure.

---

## 1. Idempotency — the one property that makes everything else safe

**Idempotent** = running it N times leaves the same result as running it once. Airflow's whole
operational model — retries, backfills, "clear and re-run", re-running after a bug fix —
assumes your tasks are idempotent. If they aren't, every one of those tools corrupts data.

**The enemy is *append* / blind *insert*.** A task that appends to a file or `INSERT`s into a
table adds the same rows again on every re-run.

**Lab 08 (`labs/08-idempotent/`)** has a load task with `retries: 2` that **appends** per-day
counts to a shared `totals.csv`. The verifier runs day 1 three times and day 2 once.

**Measured on this machine:** the appending version produced **20** lines (10 distinct,
duplicated); the fix — give each day its own file and **overwrite** it — produced exactly
**10**, no matter how many times each day ran:
```python
with open(f"{OUT}/dt={ds}.csv", "w") as out:    # one file per day, truncated each run
    ...
```

**The idempotent-load patterns** (memorise these — they are the heart of pipeline
engineering):
- Write to a path/partition keyed by the run's date and **overwrite it** (`mode("overwrite")`,
  `INSERT OVERWRITE`, temp-file-then-rename). Our Spark `app/pipeline.py` already does this:
  each run overwrites `dt=<ds>` only.
- Warehouse tables: `MERGE`/upsert on a business key, or delete-then-insert in one transaction.
- Never blind-`INSERT` or append to a shared file/table from a task that can retry.
- Side effects (emails, payments) need an idempotency key or must live outside retried tasks.

Do the lab now.

```bash
cd labs/08-idempotent && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 2. Retries — free, but only if idempotent

`default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}` makes a failed task
retry. Retries handle *transient* failures (a flaky network, a briefly-unavailable service)
beautifully — **provided the task is idempotent**, because a retry re-runs the whole task,
including whatever it already did before failing. A retry on an appending task is exactly how
Lab 08's numbers doubled.

Guidance:
- Set retries on tasks whose failures are often transient (network, external services).
- Keep `retry_delay` long enough to let the transient cause clear.
- A task with non-idempotent side effects should have `retries: 0` until you make it safe —
  or better, make it safe.

---

## 3. The logical-date trap (the one that bites everyone once)

Airflow hands every task the run's **logical date** (historically "execution date"), exposed
as `{{ ds }}` / `{{ data_interval_start }}` / `{{ data_interval_end }}`. The trap:

> For a scheduled `@daily` run, the run that *executes* at **2026-10-02 00:00** processes the
> interval **2026-10-01 → 2026-10-02**, and its logical date (`ds`) is **2026-10-01** — the
> interval **start**. Airflow runs a period *after it has ended.*

So "the run on the 2nd processes the 2nd" is wrong by one day. The run on the 2nd processes the
**1st**. Internalise: **the logical date is the start of the data interval the run covers, not
the wall-clock time it runs.**

**The failure this causes** (Lab 09, `labs/09-logical-date/`): a task uses
`datetime.utcnow()` ("today") to decide which day to process, instead of `ds`. Scheduled on
time they happen to agree; on a **backfill**, a retry the next morning, or a manual re-run they
diverge — the task processes "now", not the day it was *for*.

**Measured on this machine:** the `utcnow()` version, run for three different logical dates,
wrote three files all named for *today* (`processed_2026-11-04.txt` ×3, overwriting). The fix
— take the day from the context — wrote the right three:
```python
def process_day(ds, **_):      # ds = logical date 'YYYY-MM-DD'
    day = ds
```
produced `processed_2026-10-01.txt`, `..._10-02`, `..._10-03`. Always pass
`ds` / `data_interval_start` / `data_interval_end` into the job (how Day 10 wires it into the
Spark partition), never read the wall clock inside a task. Do the lab now.

```bash
cd labs/09-logical-date && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 4. `catchup` and backfills — running the past

- **`catchup=True`** (the Airflow default): when you unpause a DAG with `start_date` in the
  past, the scheduler creates a run for **every** missed interval since `start_date` and runs
  them. Powerful, and a foot-gun if `start_date` is a year ago.
- **`catchup=False`** (what Day 8's DAG uses): only run from *now* forward; don't auto-fill
  history. Safer default for a new DAG.
- **Backfill** — explicitly re-run a date range on demand:
  ```bash
  airflow dags backfill -s 2026-10-03 -e 2026-10-05 d08_orderflow_daily
  ```
  This creates and runs a DAG run per day in the range. **A backfill is only safe if the DAG
  is idempotent** — it re-processes days that may already have output, which is precisely what
  the overwrite-by-partition pattern (§1) makes safe. (On this course's `SequentialExecutor`,
  prefer `airflow dags test <date>` per day to run a few specific days; `backfill` drives the
  scheduler and is slower locally.)

Backfills exist *because* of late data (Day 6 §6): when stragglers arrive, you re-run the last
few days. Non-idempotent + backfill = double-counting.

---

## 5. Sensors — waiting for something to exist

A **sensor** is a task that *waits* for a condition (a file lands, a partition appears, an
external DAG finishes) before downstream tasks run. `FileSensor`, `ExternalTaskSensor`,
`S3KeySensor`, etc.

The trap (Lab 09 family — stuck DAGs): a sensor in default **`poke`** mode **holds a worker
slot** the entire time it waits. A handful of sensors poking for hours can starve the whole
executor — the DAG looks "stuck" but it is actually *out of slots*. Fixes:
- `mode="reschedule"` — the sensor releases its slot between checks (sleeps as a scheduled
  re-poke) instead of holding it. Use this for anything that may wait minutes+.
- Always set a `timeout` so a sensor that will never succeed fails instead of waiting forever.

```python
from airflow.sensors.filesystem import FileSensor
wait = FileSensor(task_id="wait_for_input", filepath="...", mode="reschedule",
                  poke_interval=60, timeout=60 * 60)
```

---

## 6. SLAs and alerting — being told when it breaks

A red task in the UI is only seen by whoever opens the UI. Make failure *reach* someone:
- `on_failure_callback` — a function Airflow calls when a task fails (post to Slack, page).
- `sla=timedelta(hours=2)` on a task — Airflow records an SLA miss if it isn't done in time
  (an SLA miss is a *lateness* signal, separate from failure).
- Email on failure via `email_on_failure` + an SMTP connection.

And the subtler failure Day 10 covers: a task that *succeeds* while producing *zero rows*.
SLAs and failure callbacks don't catch that — a **validation task** that fails on bad output
does.

---

## 7. End-of-day checklist

1. Define idempotent. Why does Airflow's whole model depend on it?
2. A task with `retries: 2` appends to a table and fails after writing. What's in the table
   after it succeeds on retry 1? What's the fix?
3. A `@daily` run executes at 2026-10-02 00:00. What is its `ds`, and what interval does it
   cover?
4. A task uses `datetime.utcnow()` to pick the day. Name two situations where that produces
   the wrong day, and the fix.
5. What is `catchup=True` vs `False`, and why is a backfill dangerous on a non-idempotent DAG?
6. A DAG with several `FileSensor`s looks "stuck". What is the likely real cause and the fix?

If 1–5 are solid, Day 10 wires Part 1 and Part 2 together: submit the Spark job from a DAG,
pass the data interval as the partition, and catch the "green DAG, empty output" failure.

## Cleanup
`airflow_home/` and the labs' `out/` dirs are gitignored.
