# Lab 09 — "We backfilled October and got October-in-November"

## The incident
`lab09_day` processes "a day" of data. To fix a bug the team backfilled 2026-10-01 to
2026-10-03. Afterwards every run had processed the same day (the day the backfill ran), and
the real days were still missing.

## Run
```bash
./break.sh
./verify.sh      # simulates three runs for three different days
```
## Your task
Each run must process the day it was **run for**, not the day it happened to run. Keep
`catchup=True`. `./verify.sh`.

<details><summary>Hint</summary>Day 9 §3: Airflow hands every task the run's logical date. Look for `ds`.</details>
