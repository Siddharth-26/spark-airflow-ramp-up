# Solution — Lab 09

**Cause:** `datetime.utcnow()` is *when the code runs*, not *which data it is meant for*.
Scheduled on time they happen to agree; on a backfill, retry the next morning, or a manual
re-run they diverge. Wall-clock time inside a task makes the result depend on when you ran it.

**Fix:** take the day from the run's context.
```python
def process_day(ds, **_):         # ds = logical date as 'YYYY-MM-DD'
    day = ds
```
or in templated fields: `{{ ds }}`, and for the exact window `{{ data_interval_start }}` /
`{{ data_interval_end }}`.

**The trap you must know:** for a scheduled `@daily` run, the run that executes at
2026-10-02 00:00 processes the interval **2026-10-01 -> 2026-10-02**, and its logical date
(`ds`) is **2026-10-01** (the interval *start*). Airflow runs a period after it ends. If
your mental model is "the 10-02 run processes 10-02", you will be one day off. Always pass
`data_interval_start/end` or `ds` explicitly into the job (Day 10) and test one day on
purpose.
