# Solution — Lab 01

**Cause:** Adaptive Query Execution (AQE, on by default since Spark 3.2) looks at the real
size of the shuffle output at runtime. The whole `groupBy` output here is 984 bytes, so AQE
*coalesces* the 8 small shuffle partitions into 1 task. Spark did not ignore your config; it
read it as a starting point and improved on it.

**Fix (config only):**
```python
.config("spark.sql.adaptive.enabled", "false")
```
Now the shuffle really has 8 partitions and the reduce stage runs 8 tasks:
`UI_STATS jobs=... stage_tasks=[3,3,8,8,...]`.

**Judgement:** on real data you almost never want this fix. Tiny shuffles *should* be
coalesced. The lesson is how to *read* the UI: "tasks != shuffle.partitions" usually means
AQE, not a bug. Only turn AQE off to make an experiment deterministic.
