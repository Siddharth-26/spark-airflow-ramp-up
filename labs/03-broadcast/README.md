# Lab 03 — "Joining 373k events to a 5-row table shuffles everything"

## The incident
`job.py` joins the events to a 5-row `tenants` table. `explain()` shows a `SortMergeJoin`
with an `Exchange` on **both** sides: all 373k events get shuffled across the network just
to attach a tenant name. Make Spark send the 5 rows to the events instead.

## Run
```bash
./break.sh
PYTHONPATH=app python labs/03-broadcast/job.py     # from the repo root
```
## Your task
Get `BroadcastHashJoin` into the plan and remove `SortMergeJoin`. Warning: removing
the suspicious config line is *not* enough. Find out why, then fix it properly. Then `./verify.sh`.

<details><summary>Hint</summary>Day 4 §4. Search the job for the word "optimised".</details>
