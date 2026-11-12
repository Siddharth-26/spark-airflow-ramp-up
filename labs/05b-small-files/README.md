# Lab 05b — "One day of data became 1,000 files"

## The incident
`job.py` writes one day of events (53k rows, ~6 MB of Parquet) partitioned by tenant. The
output folder holds ~1,000 files of a few KB each. Every downstream job now spends more time
opening files than reading them. Fix the write.

## Run
```bash
./break.sh
PYTHONPATH=app python labs/05b-small-files/job.py ; find labs/05b-small-files/out -name '*.parquet' | wc -l
```
## Your task
Same rows, **10 or fewer files**. `./verify.sh`.

<details><summary>Hint</summary>Day 6 §4. Why does 200 in-memory partitions x 5 tenant folders make 1,000 files?</details>
