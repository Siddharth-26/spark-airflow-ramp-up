# Lab 04c — "7 tasks finished in a second, 1 task is still running"

## The incident
Tenant `megacorp` produces ~60% of all events. A join of events to the tenant table is
slow: in the Spark UI one task of the join stage reads ~96k records while the others read
13-25k or none. The tenant table is "too big to broadcast" (pretend), so you cannot just
use `broadcast()`.

## Run
```bash
./break.sh
PYTHONPATH=app python labs/04c-skew/job.py          # from the repo root
```
`SKEW max_task_share=0.60` means one task handled 60% of the stage's rows.

## Your task
Keep the result identical but bring `max_task_share` under 0.35 without broadcasting.
`./verify.sh`.

<details><summary>Hint</summary>
Day 5 §6 (salting): if one key is too heavy for one task, make the key *less* unique.
</details>
