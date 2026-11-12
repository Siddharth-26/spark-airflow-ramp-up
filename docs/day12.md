# Day 12 — Capstone (graded) + optional cloud run

**Goal:** prove the whole course on your own, under a time box, with no step-by-step hints.
You build and debug an OrderFlow pipeline end to end, then grade yourself with
`assessment/exam.sh`. The optional cloud section runs the same pipeline on AWS and tears it
down the same day.

**Assumed:** Days 1–11. This is assessment, not instruction — the only new thing is that
nobody holds your hand.

---

## 1. The brief

You are handed messy OrderFlow data and must produce trustworthy daily tenant revenue, run by
Airflow, safe to re-run. Build it from scratch in `assessment/solution/` (don't copy the labs;
you may read your own Part 1–2 docs).

Generate the capstone data (skew + dupes + bad + late, 5 days):
```bash
source scripts/setup-venv.sh
python app/generate_orders.py --skew --dupes --bad --late --days 5 --seed 2026 --out data/capstone
```

### Requirements (each is graded)

**Spark job** `assessment/solution/pipeline.py --ds <date> --raw data/capstone --out data/capstone_out`:
1. Reads one day, **quarantines** bad records (count them; the accounting identity is
   `valid + bad = input` **before** you dedupe — duplicates are valid rows, not bad ones, so
   check the sum before `dropDuplicates`, not after). Does not silently drop data. (Day 6 §2)
2. **Dedupes** on `event_id`. (Day 6 §5)
3. Writes **cleaned Parquet** partitioned by `dt`, and a **daily aggregate** (orders &
   revenue per tenant for `event_type='created'`), each **overwriting only that day's
   partition** — idempotent. (Day 9 §1)
4. **Exits non-zero** on missing input or if validation fails (zero rows out). (Day 10 §3)
5. Handles the known **skew** on the aggregate/join without one task doing all the work.
   (Day 5 §5)

**Airflow DAG** `dags/capstone_dag.py`:
6. `check_input → run_pipeline → validate`, `@daily`, `catchup=False`, `retries: 1`.
7. Passes the **logical date** to `--ds` (never the wall clock). (Day 9 §3)
8. The task's success reflects the **job's exit code** (no swallowed `; echo`). (Day 10 §3)
9. A `validate` task that **fails** if the day's aggregate is missing or empty.

**Prove it**
10. Running the same day **twice** leaves the output unchanged (idempotency).
11. A day **with no input** makes the DAG run **fail**.

### Time box
Aim for **2–3 hours**. If you're stuck on a requirement for more than ~20 min, open the matching
day's doc, fix it, and note what you missed — that note is the real output of the capstone.

---

## 2. Grade yourself

```bash
cd assessment
./exam.sh        # checks requirements 1–11 against your assessment/solution/ + dags/capstone_dag.py
```
`exam.sh` is PASS/FAIL per requirement, like every lab's `verify.sh`. It generates the
capstone data if missing, runs your pipeline for a day with data and a day without, runs it
twice to check idempotency, and inspects your DAG for the logical-date and exit-code rules. A
green `exam.sh` means you built a correct, re-runnable pipeline unaided — the course goal.

Read `assessment/rubric.md` for how each requirement is scored and the common ways each one is
failed (they map to the labs you did).

---

## 3. Optional: run it on a real cloud cluster

Everything so far ran on your laptop. Moving to a cluster changes **only** how the job is
submitted and where data lives — the code is the same. This section is **optional, costs real
money, and must be torn down the same day.** Do it only if you want to see the submit path;
skip it with no loss to the course goal.

> **Cost & teardown warning.** EMR Serverless and MWAA bill by usage/uptime. A forgotten MWAA
> environment is tens of dollars a day. **Set a teardown reminder before you start, and run the
> teardown step the same day.** Use a throwaway/sandbox AWS account, never production.

**Two realistic shapes:**

- **EMR Serverless (Spark only)** — package `pipeline.py` + the generated Parquet to S3,
  create an EMR Serverless application, submit the job with `--ds` and S3 in/out paths.
  Teardown: delete the application and the S3 objects.
  ```
  aws emr-serverless create-application --type SPARK --release-label emr-7.x ...
  aws emr-serverless start-job-run --application-id <id> \
      --execution-role-arn <role> \
      --job-driver '{"sparkSubmit":{"entryPoint":"s3://.../pipeline.py",
                     "entryPointArguments":["--ds","2026-10-01","--raw","s3://.../capstone",
                                            "--out","s3://.../out"]}}'
  # teardown:
  aws emr-serverless delete-application --application-id <id>
  ```
- **MWAA (Managed Airflow)** — upload `capstone_dag.py` (using `SparkSubmitOperator` or an
  EMR operator) to the environment's S3 DAGs bucket; the managed scheduler runs it. Teardown:
  delete the MWAA environment (the expensive part) and the bucket objects.

**What to take away:** the DAG and the Spark job you wrote on your laptop are the *same
artifacts* you deploy. The cloud changes the submit mechanism (`spark-submit` →
EMR/`SparkSubmitOperator`), the storage (`data/` → `s3://`), and the cluster manager — not
your logic. That is the whole payoff of learning it locally first.

**Do not leave anything running.** Confirm with `aws emr-serverless list-applications` /
`aws mwaa list-environments` that it's gone.

---

## 4. You're done when

- `assessment/exam.sh` is all green, built from scratch in `assessment/solution/`.
- You can answer any day's end-of-day checklist without the doc.
- You can review a pipeline PR with Day 11 §3 from memory.

That is the course's definition of "well versed in Spark and able to write, run, debug and
review data pipelines" — the outcome you asked for.

## Cleanup
`data/capstone`, `data/capstone_out`, `assessment/solution/` outputs are gitignored. If you
did the cloud run, delete the EMR Serverless app / MWAA environment and the S3 objects **today**.
