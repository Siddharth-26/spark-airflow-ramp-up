# Lab 04b — "OutOfMemoryError: Java heap space"

## The incident
`job.py` should print, per tenant, the number of events and total payload characters. It
dies with `java.lang.OutOfMemoryError: Java heap space`. A teammate's PR comment says
"just bump the memory". Don't. Understand it, then fix the query.

## Run
```bash
./break.sh
PYTHONPATH=app python labs/04b-oom/job.py 2>&1 | grep -E "ROW|OutOfMemory" | head   # from the repo root
```
## Your task
Make it produce the same 5 `ROW` lines **without touching the memory setting**. `./verify.sh`.

<details><summary>Hint</summary>
Ask: what is the biggest single object this job builds in one task? Day 5 §5: which
aggregate functions must hold every value for a group in memory, and which keep a number?
</details>
