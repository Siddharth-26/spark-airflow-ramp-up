# Lab 05c — "I deduplicated it and the count didn't change"

## The incident
About 2% of events were delivered twice. `job.py` calls `.distinct()` and still counts the
duplicates. The business rule: **one row per `event_id`**.

## Run
```bash
./break.sh
PYTHONPATH=app python labs/05c-dedupe/job.py       # from the repo root
```
## Your task
Make `UNIQUE` equal the number of distinct `event_id`s among valid rows. Keep the audit
column. `./verify.sh`.

<details><summary>Hint</summary>`distinct()` compares *entire rows*. Which column was added just before it?</details>
