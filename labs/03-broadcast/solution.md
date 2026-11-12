# Solution — Lab 03

**Cause (two layers):**
1. Someone set `spark.sql.autoBroadcastJoinThreshold = -1`, which switches automatic
   broadcasting off.
2. Even after you delete that line it is *still* a sort-merge join. The tenants table comes
   from `createDataFrame` on a Python list, which shows up in the plan as
   `Scan ExistingRDD` and carries **no size statistics**. Spark only auto-broadcasts a side
   whose size it *knows* is under the threshold (default 10 MB); an unknown size is treated
   as "maybe huge".

**Fix: tell Spark explicitly.**
```python
from pyspark.sql.functions import broadcast
joined = events.join(broadcast(tenants), "tenant_id")
```
The plan becomes `BroadcastHashJoin ... BuildRight` and the `Exchange` on the events side
disappears. (Removing the `-1` line is still right, but on its own it is not enough here.)

**Why this matters later:** tables read from Parquet/Delta/Hive tables have sizes and
broadcast automatically; tables built in Python, or the output of a complex subquery, often
do not. "Why did Spark not broadcast my small table?" is nearly always "it did not know it
was small".

**Review rule:** a hint is a promise that the table stays small. If `tenants` grows to 2 GB
a forced broadcast OOMs. Use a hint only when you *know* the size.
