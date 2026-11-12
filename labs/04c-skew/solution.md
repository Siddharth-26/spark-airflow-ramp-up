# Solution — Lab 04c

**Cause:** a shuffle sends all rows with the same join key to the same task.
`hash("megacorp") mod 8` always lands on the same partition, so one task gets 60% of the
rows regardless of how many executors you have. This is **data skew**: not a config bug,
a property of the data.

**Fix: salting.** Split the hot key into N sub-keys on the big side, and replicate the
other side N times so every sub-key still finds its match.
```python
N = 8
events_s = events.withColumn("salt", (F.rand() * N).cast("int"))
dim_s = dim.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))
joined = events_s.join(dim_s, ["tenant_id", "salt"])
```
`megacorp` rows now spread across 8 sub-keys. Measured: the heaviest task drops from 60% of the rows to 28%. The cost: the small side is
duplicated N times (trivial here) and the code is uglier.

**Alternatives you should know exist:** AQE skew-join (`spark.sql.adaptive.skewJoin.*`) does
this automatically but needs enough map-side partitions to split; with 3 input files it
cannot split our heavy partition, which is why the manual fix is needed here. And if the
dimension is truly small, `broadcast()` removes the shuffle entirely (Lab 03).
