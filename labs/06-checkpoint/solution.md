# Solution — Lab 06

**Cause:** with no checkpoint, the query has no durable memory of what it processed.
Spark creates a throwaway temporary checkpoint, so every run starts from "no files seen" and
re-reads everything in `data/stream_in`.

**Fix:**
```python
q = (stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", "labs/06-checkpoint/ckpt")
        .trigger(availableNow=True)
        .start())
```
The checkpoint stores the file-source offsets (which files are done) and batch ids. Run 2
finds nothing new and writes nothing.

**Caveats:** (1) The checkpoint gives *at-least-once* into a `foreachBatch` sink: if the job
dies after writing a batch but before committing it, that batch is re-run. For exactly-once,
make the write idempotent (e.g. key on `batch_id`, or use `MERGE`/overwrite-by-partition).
(2) Never delete a checkpoint casually: that is "replay from the beginning". (3) Changing the
query shape can make an old checkpoint incompatible.
