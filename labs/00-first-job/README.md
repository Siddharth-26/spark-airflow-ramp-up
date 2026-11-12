# Lab 00 — Your first Spark job

**No incident here.** This lab just gets a job running end to end so Day 2 onward has
something real to point at.

## Setup
```bash
./break.sh
```
This generates the sample OrderFlow data under `data/raw/orders/` if it isn't there yet
(one JSON-lines file per day, ~53k events/day).

## Your job
Write `labs/00-first-job/my_job.py` that:
1. Creates a `SparkSession` with `master("local[2]")` (2 threads — your laptop, not a cluster).
2. Reads `data/raw/orders/dt=2026-10-01/events.jsonl` as JSON.
3. Counts events per `tenant_id`, sorted descending by count.
4. Prints the result with `.show()`.

Run it with:
```bash
source .venv/bin/activate
python labs/00-first-job/my_job.py
```

## Grade yourself
```bash
./verify.sh
```

<details>
<summary>Hint (open if stuck)</summary>

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab00").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # Spark's own logs are noisy at INFO

df = spark.read.json("data/raw/orders/dt=2026-10-01/events.jsonl")
df.groupBy("tenant_id").count().orderBy("count", ascending=False).show()

spark.stop()
```
</details>
