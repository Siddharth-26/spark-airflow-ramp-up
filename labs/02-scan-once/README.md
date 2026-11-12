# Lab 02 — "Three numbers, three full scans"

## The incident
`job.py` prints three counts from the same filtered data. It takes 3x longer than it
should and the Spark UI shows three jobs that each read the whole input (~66 MB each).
Make the input get read **once**, without changing the three printed numbers.

## Setup / run
```bash
./break.sh
PYTHONPATH=app python labs/02-scan-once/job.py    # from the repo root
```
Look at `jobs=` in `UI_STATS` and at the plan printed before the second count.

## Your task
Edit `job.py` so the later counts read a cached copy (the second plan printed should
contain `InMemoryTableScan`, not `FileScan`), then `./verify.sh`.

<details><summary>Hint</summary>
Day 3 §3: each action re-runs the whole lineage unless you tell Spark to keep a result.
</details>
