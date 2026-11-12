# Solution — Lab 04a

**Cause:** the job sets `spark.sql.shuffle.partitions=2000` and turns AQE off. The shuffle
therefore creates 2,000 partitions for ~1 KB of partial results. Each task has scheduling
and serialization overhead (tens of ms), so ~2,000 x that overhead dominates the real work.

**Fix options**
1. Set `shuffle.partitions` to something sized to the data (8 here; rule of thumb ~128 MB
   of shuffle data per partition).
2. Delete both overrides. AQE (default on) coalesces small shuffle partitions automatically,
   so the stage runs with 1 task. **Preferred**: it keeps working when the data size changes.

**What to say in review:** a global `shuffle.partitions=2000` copied from a big-job config
into a small job is a very common cause of "slow tiny job". Config should follow data size,
or be left to AQE.
