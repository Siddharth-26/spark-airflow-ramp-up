# Lab 04a — "2,000 tasks to add up 5 numbers"

## The incident
`job.py` sums revenue per tenant for ONE day (53k rows, 5 groups). It is slower than
the one-line query deserves (~10 s vs ~6 s on this laptop; far worse on a real cluster), and the Spark UI's aggregation stage shows **2,000
tasks**, each doing almost nothing. Fix it.

## Run
```bash
./break.sh
PYTHONPATH=app python labs/04a-too-many-tasks/job.py     # from the repo root
```
## Your task
Cut the biggest stage to 50 tasks or fewer. Keep results identical. `./verify.sh`.
There is more than one valid fix; pick the one you would defend in review.

<details><summary>Hint</summary>Day 4 §6 (partitions) and Day 5 §3 (reading the Stages tab).</details>
