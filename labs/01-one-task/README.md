# Lab 01 — "I asked for 8 partitions and got 1 task"

## The incident
A teammate wrote `job.py`. It sets `spark.sql.shuffle.partitions = 8`, so the shuffle for
`groupBy` should produce **8 tasks** in the reduce stage. In the Spark UI the stage has
**1 task**. They think Spark is ignoring their config. Find out why and make the stage
really run 8 tasks.

## Setup
```bash
./break.sh                       # writes job.py
PYTHONPATH=app python job.py     # run it from the repo root: python labs/01-one-task/job.py
```
The last line printed, `UI_STATS ... stage_tasks=[...]`, lists the task count of every
completed stage.

## Your task
Change **only the Spark config** in `job.py` (not the query) so one stage runs 8 tasks and
the result stays correct. Then `./verify.sh`.

<details><summary>Hint</summary>
Run `df.groupBy("tenant_id").count().explain()` and look for the word `Adaptive`. Day 2 §6
explains who is allowed to change a stage's task count after you set it.
</details>
