# Solution — Lab 05c

**Cause:** `distinct()` treats two rows as equal only if **every** column matches. The audit
column `uuid()` is different on every row, so no two rows are ever equal and nothing is
removed. Any column that varies per delivery (ingest timestamp, uuid, offset, file name)
defeats whole-row dedupe.

**Fix:** dedupe on the business key.
```python
deduped = df.dropDuplicates(["event_id"])
```
**Which duplicate survives?** `dropDuplicates` keeps an *arbitrary* one. If duplicates can
differ (a corrected re-send), pick deterministically instead:
```python
from pyspark.sql import Window
w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
deduped = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```
Also note `uuid()` is non-deterministic: if Spark recomputes a partition (retry, cache
eviction) the same row can get a different uuid. Generate audit ids *after* dedupe, or
derive them from a hash of the business key.
