# Capstone rubric

`exam.sh` checks these automatically. Each maps to a day you did; the "commonly failed by"
note is the exact bug the matching lab taught.

| # | Requirement | Day | Commonly failed by |
|---|---|---|---|
| 1 | Bad records quarantined, counted; `good + bad = input` | 6 §2 | `filter(isNotNull)` after PERMISSIVE silently drops rows |
| 2 | Dedupe on `event_id` | 6 §5 | `distinct()` with an audit/uuid column removes nothing |
| 3 | Cleaned Parquet + daily aggregate, **overwrite per `dt`** | 9 §1 | append / blind insert → not idempotent |
| 4 | Exit non-zero on missing input / empty output | 10 §3 | job prints a warning but exits 0 |
| 5 | Skew handled on the hot tenant | 5 §5 | one task does 60% of the work |
| 6 | DAG: check → run → validate, `@daily`, `catchup=False`, `retries:1` | 8–9 | — |
| 7 | Logical date passed as `--ds` | 9 §3 | `datetime.utcnow()` inside the task |
| 8 | Task success = job exit code | 10 §3 | `... ; echo done` swallows the failure |
| 9 | `validate` fails on missing/empty aggregate | 10 §3 | no validation → "green DAG, empty output" |
| 10 | Running a day twice → unchanged output | 9 §1 | non-idempotent write |
| 11 | A no-input day → DAG run fails | 10 §3 | silent success |

Scoring: 11/11 green = pass. Below that, the failing line names the day to revisit.
Build your solution in `assessment/solution/pipeline.py` and `dags/capstone_dag.py`.
A reference implementation is NOT shipped — the labs already contain every technique; the
capstone is assembling them yourself.
