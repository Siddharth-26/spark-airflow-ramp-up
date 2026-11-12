# Solution — Lab 08

**Cause:** the task *appends* to a shared file. Appending is not idempotent: every extra
execution (retry, manual re-run, backfill, "clear" in the UI) adds the same rows again.

**Fix:** make each run own its output and **overwrite** it.
```python
with open(f"{OUT}/dt={ds}.csv", "w") as out:      # one file per day, truncated each run
    ...
```
Run it 1 time or 50 times for 2026-10-01: same file contents.

**The general patterns for idempotent loads**
- Write to a partition/path keyed by the run's date and *overwrite it* (`mode("overwrite")`,
  `INSERT OVERWRITE`, temp-file-then-rename).
- Warehouse tables: `MERGE`/upsert on a business key, or delete-then-insert in one transaction.
- Never `INSERT` blindly or `append` to a shared file/table from a task that can retry.
- Side effects (emails, payments) need an idempotency key, or must move out of retried tasks.

**Why it matters more than any other Airflow rule:** retries, backfills, clears and
reruns after a bug-fix are all "run the same logical day again". If that is unsafe, every
operational tool Airflow gives you becomes dangerous.
