# Lab 00 — Solution

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab00").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.json("data/raw/orders/dt=2026-10-01/events.jsonl")
df.groupBy("tenant_id").count().orderBy("count", ascending=False).show()

spark.stop()
```

Real output from this machine (yours will differ slightly — different random seed day):

```
+---------+-----+
|tenant_id|count|
+---------+-----+
|   globex|10926|
|    hooli|10716|
|     acme|10623|
|  initech|10564|
| umbrella|10523|
+---------+-----+
```

**Why `setLogLevel("WARN")`:** by default Spark logs every stage transition at INFO,
which buries your actual output. This has nothing to do with your *application* logs
(print/logging in your own code) — it only quiets Spark's internal chatter.

**Why `master("local[2]")` and not `local[*]`:** `local[*]` uses every core on your
laptop, which is realistic for throughput but makes job output ordering less
predictable while you're learning to read it. You'll switch to `local[*]` from Day 3
once you're reading plans comfortably.
