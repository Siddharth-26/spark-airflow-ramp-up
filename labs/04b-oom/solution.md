# Solution — Lab 04b

**Cause:** `collect_list` builds, for each tenant, one list holding **every payload string**
for that tenant (~75k strings of 3 KB each = ~225 MB per tenant, in one task, in one JVM
object graph). Then the job only *measures* those lists. The data it needs is two numbers,
but the query materialises the whole population first.

**Fix:** compute the numbers with aggregates that keep a running value, not the values:
```python
per_tenant = (df.groupBy("tenant_id")
                .agg(F.count("*").alias("events"),
                     F.sum(F.length("payload")).alias("chars")))
```
`count` and `sum` hold one number per group and support partial (map-side) aggregation, so
memory stays flat regardless of data size.

**Why "bump the memory" is the wrong first answer:** it works until the data grows 3x, then
fails again, and it hides the real design bug. Rule: reach for more memory only when you
have shown the working set is genuinely that large and cannot be streamed.

**How you'd see it in the UI:** the failed stage shows one task with huge "Shuffle Read
Size" / long GC time, and "ExecutorLostFailure ... OutOfMemory" in the task errors.
