# Day 7 — Structured Streaming basics

**Goal:** by the end of today you can explain how Spark processes an *unbounded* input as a
series of small batches, what a **watermark** and a **checkpoint** are for, and the difference
between at-least-once and exactly-once delivery. You run a streaming job that reads a folder
as files land in it.

**Assumed:** Days 1–6 (DataFrames, aggregates, nulls, late data). **Not assumed:** streaming,
micro-batches, triggers, watermarks, checkpoints, output modes.

---

## 1. Streaming = the same DataFrame API on an input that never ends

Day 1's batch job: finite input, runs, finishes. A **streaming** job: unbounded input, never
finishes — it keeps processing new data as it arrives. The remarkable thing about Spark
**Structured Streaming** is that you use the **same DataFrame API**. The engine runs your
query repeatedly on new slices of input and keeps the running result consistent.

```
batch:    spark.read        .json(path)     → a fixed DataFrame, one run
stream:   spark.readStream  .json(path)     → an unbounded DataFrame, run forever
```

Under the hood it is the **micro-batch** model: Spark wakes on a **trigger**, treats whatever
new data arrived as a small batch, runs your query on it, updates the result, commits
progress, sleeps. (There is a lower-latency continuous mode; micro-batch is the default and
what you will use.)

---

## 2. Read a folder as a stream

The file source watches a directory and treats each new file as new data. Set
`maxFilesPerTrigger` so you can watch it batch, one file at a time.

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("d7").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = ("amount double, currency string, event_id string, event_ts string, event_type string,"
          " order_id string, tenant_id string")
stream = (spark.readStream.schema(schema)      # a stream MUST have an explicit schema
               .option("maxFilesPerTrigger", 1)
               .json("data/messy"))
print("is streaming:", stream.isStreaming)
```

**What it does:** defines an unbounded DataFrame over the `data/messy` folder, one file per
micro-batch. **A streaming read requires an explicit `schema`** — it cannot infer from an
unbounded input.

**You should see:** `is streaming: True`.

**When you'll use it:** any "process files as they land" pipeline (a drop folder, an S3
prefix). The same source abstraction covers Kafka, sockets, etc.

---

## 3. Triggers — when a micro-batch fires

| trigger | behaviour | use for |
|---|---|---|
| default (unspecified) | fire as soon as the previous batch finishes | lowest latency |
| `processingTime="30 seconds"` | fire on a fixed clock interval | steady cadence |
| `availableNow=True` | process all available data, then **stop** | a cron'd "catch-up" job |
| `once=True` (deprecated → `availableNow`) | one batch then stop | — |

`availableNow` is the one you will meet most as a backend engineer: a scheduled job that
drains whatever has arrived and exits — a batch job wearing a streaming API, so it gets
streaming's exactly-once bookkeeping (next section).

---

## 4. Checkpoints — how a stream remembers progress

A stream must survive restarts: after a crash it has to know *which input it already
processed* and *what the running aggregation state was*. That memory is the **checkpoint**: a
directory where Spark durably records **source offsets** (which files/Kafka offsets are done),
**batch ids**, and any **aggregation state**.

**No checkpoint = no memory.** Spark makes a throwaway temporary one, so every run starts from
"nothing processed" and **reprocesses everything**. That is the bug in Lab 06: a nightly
`availableNow` job with no checkpoint reran and doubled the data.

```python
def write_batch(batch_df, batch_id):
    batch_df.write.mode("append").parquet("out")

q = (stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", "ckpt")     # ← the fix: durable progress
        .trigger(availableNow=True)
        .start())
q.awaitTermination()
```

**Measured on this machine (Lab 06):** run the job twice against the same input. Without the
checkpoint, the output holds **320,152** rows (every line twice). With
`checkpointLocation="ckpt"`, run 2 finds nothing new and the output stays at **160,076** — one
copy of each input line.

**Rules to keep:**
- Never delete a checkpoint casually — that means "replay from the beginning".
- Changing the query shape can make an old checkpoint incompatible (Spark will tell you).
- The checkpoint gives *at-least-once* into a `foreachBatch` sink (next section).

**Lab 06 (`labs/06-checkpoint/`)** — make a re-run safe. Do it now.

```bash
cd labs/06-checkpoint && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 5. Watermarks — bounding late data in a stream

Day 6 showed late events are real (4,862 back-dated rows in `data/messy`). In a *stream* that
aggregates by event-time window ("count per day"), late data poses a question: how long do you
keep a window open waiting for stragglers? Forever means unbounded state. A **watermark** is
your answer: *"I will wait up to T for late data; past that, a window is closed and later
arrivals for it are dropped."*

```python
good = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json("data/messy")
             .withColumn("ts", F.to_timestamp("event_ts")).filter("ts is not null"))

windowed = (good.withWatermark("ts", "1 hour")        # tolerate 1 hour of lateness
                .groupBy(F.window("ts", "1 day")).count())
```

**Measured on this machine** (same data, `availableNow`, counting per 1-day window):

| watermark | total rows counted | effect |
|---|---|---|
| none | 162,783 | every late row counted; state kept for every window forever |
| `1 hour` | 161,212 | **1,571 rows too late for their window were dropped** |

That 1,571 is the price of bounding state: rows whose event-time was more than the watermark
behind the latest seen event, so their window had already closed. The trade-off is explicit —
**a bigger watermark drops fewer rows but holds more state in memory.** You choose T from your
data's real lateness (Day 6 showed lags up to 5 days here, so `1 hour` is deliberately tight
to make the drop visible).

**Output modes** (how the result table is emitted each batch) interact with watermarks:
- `append` — emit a row only once it is final (needs a watermark for aggregations, so Spark
  knows when a window is closed).
- `update` — emit rows that changed this batch (what the table above used).
- `complete` — emit the whole result table every batch (only for aggregations; unbounded for
  a growing key space).

---

## 6. Exactly-once vs at-least-once — the honest version

You will hear "Spark Structured Streaming gives exactly-once." The precise statement:

> Spark gives **exactly-once for its own state and for sinks it controls transactionally**
> (the file sink with its `_spark_metadata` log, Delta, Kafka with transactions). For an
> arbitrary sink inside `foreachBatch`, you get **at-least-once**: if the job dies after
> writing a batch but before committing the checkpoint, that batch re-runs and is written
> again.

So for a custom sink, *you* make the write idempotent to reach effective exactly-once:
- key the output path on `batch_id` and overwrite it, or
- `MERGE`/upsert on a business key, or
- overwrite-by-partition (the same idempotency pattern as the batch pipeline, Days 8–10).

This is the same lesson as Day 6 §6 and Day 8/9's idempotency, arriving from the streaming
side: **the delivery guarantee is only as strong as the sink's idempotency.**

---

## 7. Batch and streaming are one engine

Notice what carried over: DataFrames, aggregates, the plan, partitions, shuffles, null
handling, idempotency. Streaming added exactly three new ideas — **trigger** (when),
**checkpoint** (memory), **watermark** (how long to wait for late data) — plus output modes.
The hard parts you already learned in batch.

For a backend engineer, the realistic use is a scheduled `availableNow` drain job: it looks
like a cron'd batch job, but the checkpoint gives you Spark's offset bookkeeping for free, so
"process new files since last time" is handled without you tracking offsets by hand.

---

## 8. End-of-day checklist

1. What is the micro-batch model, and what is a *trigger*?
2. A streaming read fails without a schema. Why can a batch read infer one but a stream can't?
3. What three things does a checkpoint store, and what happens if there is none?
4. `availableNow` with no checkpoint, run twice — what goes wrong, and what did the numbers
   show (160,076 vs 320,152)?
5. What does a 1-hour watermark do to a window-count, and what did it cost on our data?
6. Does Structured Streaming give exactly-once into *any* sink? State the real guarantee.

That is the end of Part 1 (Spark). Day 8 starts Part 2: Airflow — the scheduler that will
*run* a pipeline like this one every day, safely.

## Cleanup
Delete any `ckpt`/`out` dirs you created outside the labs. The lab ones are gitignored.
