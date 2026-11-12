# Day 8 — Airflow architecture and your first DAG

**Goal:** by the end of today you can name Airflow's components and what each does, explain
why a DAG file is executed *constantly* (not just when it runs), and write + run a DAG that
orchestrates the Spark pipeline from Part 1.

**Assumed:** Part 1 (you can run `app/pipeline.py`). **Not assumed:** anything about Airflow —
DAG, operator, task, scheduler, executor, metadata DB are all defined below.

**Setup note.** This course runs Airflow locally with **no Docker needed**, using a SQLite
metadata DB and the `SequentialExecutor`. `scripts/airflow-env.sh` sets it up in
`.venv-airflow` and `airflow_home/`. A Docker Compose file (`docker/airflow-compose.yml`) is
provided if you want the real web UI; the labs use `airflow dags test`, which needs no UI.

```bash
source scripts/airflow-env.sh      # activates .venv-airflow, sets AIRFLOW_HOME, inits the DB
airflow version                    # 2.9.3 on this machine
```

---

## 1. What Airflow is for

Part 1 gave you a Spark job that processes one day. In production you need to run it **every
day**, in order, with retries, only after its input exists, alerting someone when it breaks,
and able to re-run a past day safely. That orchestration is Airflow's job. Airflow does **not**
process data — it *schedules and monitors* the things that do (your Spark job, a SQL query, an
API call).

Key vocabulary, defined once:

- **DAG** (Directed Acyclic Graph) — a *workflow*: a set of tasks with dependencies, no
  cycles. "Do A, then B and C in parallel, then D." Defined in a Python file.
- **Task** — one node in the DAG: one unit of work (run this Spark job, run this SQL).
- **Operator** — the *template* for a task. `BashOperator` runs a shell command,
  `PythonOperator` calls a Python function, `SparkSubmitOperator` submits a Spark job, etc.
  A task is an operator with its arguments filled in.
- **DAG run** — one execution of the whole DAG for one *logical date* (Day 9 §3).
- **Task instance** — one task in one DAG run (task × date).

---

## 2. The four components

```
          ┌──────────────┐     parses DAG files, decides what should run now,
          │  SCHEDULER   │ ──► creates DAG runs & task instances, queues them
          └──────┬───────┘
                 │ reads/writes state
          ┌──────▼───────┐     the source of truth: every DAG, run, task state,
          │ METADATA DB  │ ◄─► connection, variable, XCom. (SQLite here; Postgres in prod)
          └──────┬───────┘
                 │
          ┌──────▼───────┐     takes queued task instances and RUNS them
          │  EXECUTOR    │ ──► SequentialExecutor (1 at a time, here),
          └──────────────┘     LocalExecutor (parallel procs), Celery/Kubernetes (distributed)
          ┌──────────────┐
          │  WEBSERVER   │     the UI: read state from the metadata DB, trigger/clear runs.
          └──────────────┘     Optional for the labs (we use the CLI).
```

- **Scheduler** — the brain. Continuously parses DAG files, works out which task instances are
  due, checks their dependencies, and queues the ready ones.
- **Metadata DB** — the single source of truth. **Airflow has no memory outside this DB.** A
  task's state, a run's history, connections, variables — all here. (SQLite for this course;
  always Postgres/MySQL in production — SQLite can't do real concurrency.)
- **Executor** — *how* queued tasks actually run. `SequentialExecutor` (ours) runs one task at
  a time; `LocalExecutor` runs several processes; `Celery`/`Kubernetes` executors distribute
  across machines. The executor is a config choice, not a code change.
- **Webserver** — the UI. Reads the same metadata DB. Nice to have, not required to run DAGs.

---

## 3. The thing that surprises everyone: a DAG file runs *constantly*

A DAG `.py` file is **not** run only when the workflow runs. The **scheduler imports every DAG
file over and over** — every few seconds to minutes — just to discover what DAGs exist and
what their current shape is. So **any code at the top level of a DAG file runs on every
parse**, forever, on the scheduler.

Consequences you must internalise now:
- A network/DB call, a slow import (pandas, pyspark), or a `time.sleep` at module level is
  paid *every parse*, by the scheduler, slowing *all* DAG processing — and if the remote thing
  is down, parsing errors.
- Top-level code should be **declarations only**: the DAG, its tasks, their dependencies. Do
  real work **inside a task's callable**, which runs only when the task runs.

**Lab 07 (`labs/07-parse-time/`)** proves it: a DAG with a `time.sleep(3)` at module level
takes **3.2 s to parse**; moving the work out of the top level drops it to **0.2 s** (measured
on this machine). The scheduler pays that difference on every parse. Do the lab now.

```bash
cd labs/07-parse-time && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 4. Your first DAG

`dags/d08_first_dag.py` is a 3-task DAG that orchestrates the Day 6 pipeline: check the input
exists → run the Spark job → validate the output. Read it:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="d08_orderflow_daily",
    start_date=datetime(2026, 10, 1),       # the first logical date this DAG covers
    schedule="@daily",                       # run once per day
    catchup=False,                           # don't auto-run every day since start_date (Day 9)
    default_args={"retries": 1},             # each task retries once on failure
    tags=["ramp-up"],
) as dag:
    check_input = BashOperator(
        task_id="check_input",
        bash_command="test -f .../data/raw/orders/dt={{ ds }}/events.jsonl && echo present",
    )
    run_spark = BashOperator(
        task_id="run_spark",
        bash_command="cd ... && python app/pipeline.py --ds {{ ds }}",
    )
    validate_out = PythonOperator(task_id="validate", python_callable=validate)

    check_input >> run_spark >> validate_out      # dependency: left runs before right
```

Three things to notice:
- **`{{ ds }}`** is a *template*: Airflow substitutes the run's logical date (e.g.
  `2026-10-01`) at runtime. This is how a task knows *which day* it is processing (Day 9 §3).
- **`>>`** sets dependencies: `check_input` must succeed before `run_spark` starts, and so on.
- **`default_args={"retries": 1}`** — a failed task is retried. This is why idempotency
  (Day 9) matters: a retry re-runs the task.

### Run it for one day — for real, no UI needed

`airflow dags test` executes the whole DAG for one logical date, inline, printing each task's
logs. No scheduler, no webserver, no Docker.

```bash
source scripts/airflow-env.sh
airflow dags test d08_orderflow_daily 2026-10-02
```

**What it does:** runs `check_input → run_spark → validate` for the 2026-10-02 data.

**You should see (this machine, trimmed):**
```
[... check_input ...] input present for 2026-10-02
[... run_spark   ...] PIPELINE_OK ds=2026-10-02 rows_in=53334 rows_clean=53334 tenants=5
[... validate    ...] validated 1 aggregate file(s) for 2026-10-02
Marking task as SUCCESS. dag_id=d08_orderflow_daily, task_id=validate ...
DagRun Finished: ... state=success ...
```

**What it tells you:** all three tasks ran in dependency order and the run succeeded. The
Spark job really ran (`PIPELINE_OK`), writing `data/curated/daily_agg/dt=2026-10-02/`.
`airflow dags test` is your primary tool for the rest of the course — it is the "run this one
day and show me" button.

**When you'll use it:** developing or debugging any DAG. It is the Airflow equivalent of
running your Spark script by hand.

### See it in the UI (optional)

```bash
docker compose -f docker/airflow-compose.yml up        # starts webserver + scheduler + Postgres
# open http://localhost:8080  (admin / admin)
```
The DAG `d08_orderflow_daily` appears; the Grid and Graph views show the three tasks. The UI
reads the same metadata DB the CLI uses.

---

## 5. The CLI you will actually use

```bash
airflow dags list                       # all DAGs the scheduler found
airflow tasks list d08_orderflow_daily  # the tasks in one DAG
airflow dags test <dag_id> <YYYY-MM-DD> # run one logical date inline (your main tool)
airflow tasks test <dag_id> <task_id> <YYYY-MM-DD>   # run ONE task inline (finest-grained debug)
```

`tasks test` runs a single task with no dependency checks and no DB writes — the fastest way
to debug one operator.

---

## 6. End-of-day checklist

1. Name the four Airflow components and one sentence on each.
2. Where does Airflow keep *all* of its state? What does that imply about running two
   schedulers against SQLite?
3. Why does a DAG file's top-level code run far more often than the workflow does? Give one
   thing you must therefore never do at module level.
4. What does `{{ ds }}` become at runtime, and why does a task need it?
5. What does `check_input >> run_spark` mean?
6. What does `airflow dags test <dag> <date>` do, and why is it your main development tool?

If 1–4 are solid, Day 9 is the hard-won operational wisdom: idempotency, retries, backfills,
`catchup`, and the logical-date trap that bites every Airflow beginner exactly once.

## Cleanup
`airflow_home/` (SQLite DB, logs) is gitignored. If you started the compose stack, `docker
compose -f docker/airflow-compose.yml down`.
