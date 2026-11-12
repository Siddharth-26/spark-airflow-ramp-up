# Curriculum — Spark & Airflow ramp-up for Senior Software Engineers

Self-paced, ~2h/day. Each day = a read (`docs/dayNN.md`) + a hands-on lab against
real data on your own laptop (no cluster, no cloud bill, until the optional capstone).

**Audience:** SSEs, not data engineers. You will write, run and debug Spark jobs and
Airflow DAGs for a real feature — not operate a data platform, not tune a 500-node
cluster, not design a lakehouse. Assumes Python and general programming comfort;
assumes nothing about Spark, Airflow, distributed systems, or big-data concepts.

**Golden rule:** every concept is read, then run against real data on your machine,
then you read the real tool output (Spark UI, `explain()`, Airflow UI) to see it,
not just the result.

**Structure:** Spark first (Days 1–7), fully on its own. Then Airflow on its own
(Days 8–9). Then the two combined (Day 10). This is deliberate — mixing them from
day one makes it hard to tell "this is a Spark problem" from "this is an Airflow
problem" when something breaks.

**Status:** ✅ = ready (all 12 days written; every lab's `verify.sh` fails when broken and passes with the reference solution).

---

## Part 1 — Apache Spark (Days 1–7)

### Day 1 — Why distributed processing, and your first real job ✅ (`docs/day01.md`)
- Why a single machine stops being enough; batch vs streaming; the shape of the
  OrderFlow sample dataset you'll use for the whole course.
- Files and formats: why Parquet beats CSV/JSON for analytics, columnar storage,
  partitioning (`dt=2026-10-01/` directories).
- **Hands-on:** build your Python env by hand, run `Lab 00` (first real Spark job),
  read `.explain()` for the first time.

### Day 2 — Spark's architecture ✅ (`docs/day02.md`)
- Driver, executors, cluster manager. Jobs → stages → tasks, and why that hierarchy
  exists. `local[2]` vs a real cluster — what's actually different.
- **Hands-on:** open the Spark UI on a running job; map what you see to the concepts.

### Day 3 — DataFrames, laziness, and the query plan ✅ (`docs/day03.md`)
- Transformations vs actions; why nothing runs until an action; the Catalyst
  optimizer and why you should trust it more than you'll want to.
- **Hands-on:** `.explain(True)` on increasingly complex queries; predict the plan
  before you see it.

### Day 4 — Shuffles, joins, and partitioning ✅ (`docs/day04.md`)
- What a shuffle actually moves and why it's expensive; broadcast join vs
  sort-merge join and when Spark picks each; `repartition` vs `coalesce`.
- **Hands-on:** force both join strategies on the same data and compare.

### Day 5 — Reading the Spark UI to debug a slow job ✅ (`docs/day05.md`)
- **Labs 04a–04c:** a job that's slow because it is all task overhead, a job that
  OOMs an executor, a job with severe data skew (the `megacorp` tenant).
- Labs 01–03 (one task, scan once, broadcast) are used in Days 2–4.

### Day 6 — Data is never clean: edge cases ✅ (`docs/day06.md`)
- Schema drift, null handling, corrupt/truncated records, the small-files problem,
  duplicate and late-arriving events.
- **Labs 05a–05c:** a job that silently drops bad records, a job defeated by 1,000
  tiny files, a dedupe that doesn't actually dedupe.

### Day 7 — Structured Streaming basics ✅ (`docs/day07.md`)
- Micro-batches, watermarks, checkpoints, exactly-once vs at-least-once.
- **Hands-on:** a streaming job reading from a folder as files land in it (`labs/06-checkpoint`).

---

## Part 2 — Apache Airflow (Days 8–9)

### Day 8 — Airflow architecture and your first DAG ✅ (`docs/day08.md`)
- Scheduler, executor, metadata DB, webserver; DAG parsing and why a DAG file runs
  constantly, not just when triggered.
- **Hands-on:** local Airflow in a venv (`scripts/airflow-env.sh`, `airflow dags test`); Docker Compose (`docker/airflow-compose.yml`) is optional for the UI. Lab 07: parse-time cost.

### Day 9 — Airflow done right ✅ (`docs/day09.md`)
- Idempotency, retries, backfills, `catchup`, sensors, SLAs, the execution-date trap.
- **Labs 08–09:** a retry that appends duplicates (not idempotent), and a task using
  wall-clock time instead of the logical date.

---

## Part 3 — Putting them together (Days 10–12)

### Day 10 — Orchestrating Spark from Airflow ✅ (`docs/day10.md`)
- Submitting a Spark job from a DAG task; passing the data-interval as a partition;
  testing a pipeline end to end.
- **Lab 10:** a DAG whose task fails silently while Airflow shows green.

### Day 11 — Production concerns ✅ (`docs/day11.md`)
- Monitoring and alerting on both systems, cost/tuning cheatsheet, what to check
  when reviewing someone else's pipeline PR.

### Day 12 — Capstone (graded) + optional cloud run ✅ (`docs/day12.md`, `assessment/`)
- Build and debug an OrderFlow pipeline end to end under a time box.
- Optional: the same pipeline on EMR Serverless or MWAA, torn down the same day.

---

## Assessment model
- **Formative:** each lab's `verify.sh` is PASS/FAIL, same as k8s-ramp-up.
- **Summative:** Day 12 capstone + `assessment/exam.sh`.
