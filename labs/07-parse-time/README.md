# Lab 07 — "The scheduler is slow and nobody changed the scheduler"

## The incident
Task starts lag, the Airflow UI is sluggish, and the scheduler's "DAG processing time" for
`lab07_fanout` is 3+ seconds. The DAG only has 5 trivial tasks.

## Run
```bash
./break.sh
./verify.sh           # shows PARSE_SECONDS
```
## Your task
Make the file parse in under 1.5 s while keeping the same 5 tasks. `./verify.sh`.

<details><summary>Hint</summary>Day 8 §4: a DAG file is imported by the scheduler over and over. What runs at import?</details>
