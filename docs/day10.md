# Day 10 — Orchestrating Spark from Airflow

**Goal:** by the end of today you can submit the Part 1 Spark pipeline from an Airflow DAG,
pass the run's **logical date** through as the data partition, test the whole thing end to
end, and catch the "green DAG, empty output" failure where a job reports success while
producing nothing.

**Assumed:** Days 1–9. **Not assumed:** how operators decide success, passing the interval to
a job, the silent-success failure.

---

## 1. The join between the two halves

Part 1 built `app/pipeline.py`: process one day, overwrite that day's partition, exit
non-zero on missing input. Part 2 built DAGs that schedule work per logical date. Today is the
seam: **the DAG's logical date becomes the Spark job's `--ds`, which becomes the output
partition.** That single thread — `{{ ds }}` → `--ds <date>` → `dt=<date>/` — is what makes
the pipeline idempotent and backfillable end to end.

```
Airflow run (logical date 2026-10-01)
   └─ task: run app/pipeline.py --ds 2026-10-01
         └─ Spark reads data/raw/orders/dt=2026-10-01/
         └─ Spark overwrites data/curated/daily_agg/dt=2026-10-01/   ← idempotent per day
```

---

## 2. Two ways to submit Spark from a DAG

**(a) `BashOperator` calling the job** — what this course uses, because it has no cluster:
```python
BashOperator(
    task_id="run_pipeline",
    bash_command=f"cd {ROOT} && {SPARK_PYTHON} app/pipeline.py --ds {{{{ ds }}}} --out data/curated",
)
```
The job runs in the Spark venv (`SPARK_PYTHON` points at `.venv/bin/python`). `{{ ds }}`
templates the logical date in. Simple, explicit, and the exit code decides success (§3).

**(b) `SparkSubmitOperator`** — the production form, when there is a real cluster:
```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
SparkSubmitOperator(
    task_id="run_pipeline",
    application=f"{ROOT}/app/pipeline.py",
    application_args=["--ds", "{{ ds }}", "--out", "s3://bucket/curated"],
    conn_id="spark_default",          # an Airflow connection pointing at the cluster
)
```
It wraps `spark-submit`, reads the cluster from an Airflow **connection**, and surfaces the
job's state. The provider package (`apache-airflow-providers-apache-spark`) supplies it. The
*concept* is identical to the Bash form; only the submit mechanism and the cluster wiring
differ — which is the Day 12 cloud step.

For the labs we use (a): no cluster, exit code is the contract.

---

## 3. How an operator decides success — and the silent-success trap

**A `BashOperator` succeeds iff the LAST command in its `bash_command` exits 0.** This is the
single most common way an Airflow pipeline lies about its state.

**Lab 10 (`labs/10-silent-failure/`)** has:
```bash
cd ... && python app/pipeline.py --ds {{ ds }} --out data/lab10 ; echo pipeline step finished
```
The `;` runs `echo` **regardless** of the Spark job's exit code. `echo` exits 0, so the task
is green even when the Spark job exited 2 (no input). Result: the dashboard for that day is
empty, the DAG is green, nobody is alerted.

**Verified on this machine:** with the broken command, a day with **no input** still reported
SUCCESS. The fix — drop the ` ; echo ...` so the job's own exit code is last:
```bash
cd ... && python app/pipeline.py --ds {{ ds }} --out data/lab10
```
makes a no-input day **fail** and a day-with-data **succeed** (its output exists). Use `&&`
(not `;`) to chain commands so a failure stops the chain, and `set -euo pipefail` for
multi-line scripts.

Two companion rules the pipeline already follows:
- The **job exits non-zero** on missing input / invalid output (`app/pipeline.py` exits 2).
- Add a **validation task** after the load (row count > 0, freshness, null-rate bounds) and
  let *it* fail the run. "Success with zero rows" is the nastiest failure mode; `BashOperator`
  exit codes alone won't catch a job that cheerfully writes an empty file.

Do the lab now.

```bash
cd labs/10-silent-failure && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 4. Passing the exact interval, not just the day

`{{ ds }}` is the interval **start** as `YYYY-MM-DD` — enough for a daily partition. For
sub-daily or windowed processing, pass the precise bounds:

| template | value | use |
|---|---|---|
| `{{ ds }}` | `2026-10-01` | daily partition key |
| `{{ data_interval_start }}` | `2026-10-01T00:00:00+00:00` | exact window start |
| `{{ data_interval_end }}` | `2026-10-02T00:00:00+00:00` | exact window end |
| `{{ ds_nodash }}` | `20261001` | path/table names without dashes |

Give the job the *interval*, and the job decides what to read — never let the job read the
wall clock (Day 9 §3). This is what makes a backfill of October actually reprocess October.

---

## 5. Testing a pipeline end to end

Your main tool stays `airflow dags test`. The full OrderFlow DAG (`d08_orderflow_daily`) runs
check → spark → validate for one day:
```bash
source scripts/airflow-env.sh
airflow dags test d08_orderflow_daily 2026-10-02    # a day WITH data → success, output written
airflow dags test d08_orderflow_daily 2027-01-01    # a day with NO data → run fails (as it must)
```
**Verified on this machine:** the first writes `data/curated/daily_agg/dt=2026-10-02/` and the
run is green; the second fails because `check_input`/the pipeline finds no input. That pair —
*succeeds with data, fails without* — is the end-to-end correctness check for the whole
pipeline.

**A test habit for pipelines:** test one day with data, one day with no data, and (if you can)
one day run **twice** to confirm idempotency (output unchanged on the second run — the Spark
job's overwrite guarantees this).

---

## 6. End-of-day checklist

1. Trace the single thread from a DAG's logical date to the output partition. Why does that
   thread make the pipeline idempotent and backfillable?
2. `BashOperator` with `spark-job ; echo done` is always green. Why, and what's the fix?
3. What's the difference between `BashOperator`-calling-the-job and `SparkSubmitOperator`, and
   what changes between them when you move to a cluster?
4. When would you pass `{{ data_interval_start }}` instead of `{{ ds }}`?
5. Why is a validation task needed even when the Spark job exits non-zero on missing input?
6. Give the three `airflow dags test` runs you'd use to check a daily pipeline's correctness.

Day 11 is the production layer: monitoring both systems, a cost/tuning cheatsheet, and the
checklist for reviewing someone else's pipeline PR. Day 12 is the graded capstone.

## Cleanup
`data/lab10`, `airflow_home/` are gitignored.
