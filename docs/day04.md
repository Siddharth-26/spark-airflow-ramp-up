# Day 4 — Shuffles, joins, and partitioning

**Goal:** by the end of today you can explain what a shuffle physically moves and why it is
expensive, tell a broadcast join from a sort-merge join in a plan and know when Spark picks
each, and choose between `repartition` and `coalesce` with a reason. This is where the
majority of real Spark performance problems live.

**Assumed:** Days 1–3 (plan reading, stages = shuffle boundaries, tasks = partitions). **Not
assumed:** join strategies, broadcast, salting (Day 5), `repartition`/`coalesce`.

---

## 1. What a shuffle actually is

A **shuffle** is Spark moving rows across the network so that rows which must be processed
together end up in the same partition (and therefore the same task). It happens whenever the
computation needs rows from different partitions to meet: `groupBy`, `join`, `distinct`,
`repartition`, `orderBy`, window functions.

Physically, a shuffle is two halves with a disk-and-network gap between them:

```
MAP side (stage N)                        REDUCE side (stage N+1)
 each task:                                each task:
  read its partition                        fetch its slice from EVERY map task
  compute partial result                    (network!)
  split output by hash(key) % P  ──writes──► combine the fetched slices
  into P "shuffle files" on local disk       produce final rows
```

Every map task writes a piece destined for every reduce task. With `M` map tasks and `P`
reduce partitions that is up to `M × P` little transfers. That is why a shuffle is the
expensive operation: it serializes data, writes it to disk, and pulls it across the network,
all before the next stage can start.

**The number to watch** is **Shuffle Read/Write** in the UI's Stages tab. On Day 2 the
`groupBy` shuffle was under 1 KB (5 partial sums per partition) — trivial. The trouble starts
when it is gigabytes.

---

## 2. Partitioning: the knob that controls shuffle fan-out

Two different "partition" ideas, finally pulled apart (Day 1 warned this was coming):

- **In-memory partitions** — how many slices the data is cut into *right now*, in RAM. One
  task per partition. Controlled by the reader (input splits) and by `repartition`/`coalesce`
  and `spark.sql.shuffle.partitions` (the post-shuffle count, default 200).
- **On-disk partitioning** — the `dt=2026-10-01/` directory layout from Day 1. A storage
  layout for pruning, not an in-memory concept.

Today is about the first. Check how many in-memory partitions you have at any point:

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("d4").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.json("data/raw/orders/*/events.jsonl")
print("input partitions:", df.rdd.getNumPartitions())
```

**You should see (this machine):** `input partitions: 3` — the 7 JSON files were read as 3
splits. Each becomes one map task (Day 2 §5).

### `repartition` vs `coalesce`

| | `repartition(n)` / `repartition("col")` | `coalesce(n)` |
|---|---|---|
| does a shuffle? | **yes** (full shuffle) | **no** (just merges adjacent partitions) |
| can increase partitions? | yes | **no** — only decrease |
| result sizes | even | can be uneven/skewed |
| cost | high | cheap |
| use when | you need MORE partitions, or even sizes, or to co-locate a key | you have too MANY small partitions and want fewer, cheaply |

Rule: **`coalesce` to shrink without a shuffle; `repartition` when you need balance or more
partitions or to group a key together.** You will use both in Lab 05b (small files) and
Lab 04a.

```python
print("after repartition(8):", df.repartition(8).rdd.getNumPartitions())   # 8  (shuffled)
print("after coalesce(1):   ", df.coalesce(1).rdd.getNumPartitions())      # 1  (no shuffle)
```

**When you'll use it:** before a wide write (control file count, Lab 05b), or to spread work
when the reader gave you too few partitions for your core count.

---

## 3. A join is a shuffle (usually)

To join two tables on `tenant_id`, Spark must get every row with a given `tenant_id` from
*both* sides into the same task. The default way to guarantee that is to shuffle **both**
sides by the join key — a **sort-merge join**. Run the Day 3 prediction query again and read
it properly:

```python
events = spark.read.json("data/raw/orders/*/events.jsonl")
counts = events.groupBy("tenant_id").count()
events.join(counts, "tenant_id").explain()
```

**Real physical plan from this machine (trimmed):**
```
SortMergeJoin [tenant_id#14], [tenant_id#71], Inner
 :- Sort [tenant_id#14 ASC ...]
 :  +- Exchange hashpartitioning(tenant_id#14, 8)      ← shuffle LEFT side
 :     +- FileScan json [...all 7 columns...]
 +- Sort [tenant_id#71 ASC ...]
    +- HashAggregate(... count(1))
       +- Exchange hashpartitioning(tenant_id#71, 8)   ← shuffle RIGHT side
          +- HashAggregate(... partial_count(1))
             +- FileScan json [tenant_id#71]
```

**What it tells you:** a `SortMergeJoin` shuffles **and sorts both inputs** by the key. For
373k events joined to 5 rows, that is absurd — you are shuffling all the events to meet a
5-row lookup table. There is a far cheaper way when one side is small.

---

## 4. Broadcast join: send the small side to the big side

If one side is small enough to fit in each executor's memory, Spark can **broadcast** it: ship
a full copy of the small table to every executor and join locally, with **no shuffle of the
big side at all**. In the plan it is `BroadcastHashJoin`.

Spark does this automatically when it *knows* a side is under
`spark.sql.autoBroadcastJoinThreshold` (default **10 MB**). The catch you will hit: it only
auto-broadcasts a side whose **size it knows**. A DataFrame built from a Python list
(`createDataFrame`) shows up as `Scan ExistingRDD` with **no size statistics**, so Spark
treats it as "maybe huge" and falls back to sort-merge — even though it is 5 rows.

**Force it explicitly** when you know a table is small:
```python
from pyspark.sql.functions import broadcast
tenants = spark.createDataFrame(
    [("acme","Acme","enterprise"),("globex","Globex","smb"),("initech","Initech","smb"),
     ("umbrella","Umbrella","enterprise"),("hooli","Hooli","enterprise")],
    "tenant_id string, name string, tier string")
events.join(broadcast(tenants), "tenant_id").explain()
```

**Real plan from this machine (trimmed):**
```
BroadcastHashJoin [tenant_id#14], [tenant_id#22], Inner, BuildRight
 :- FileScan json [...]                                   ← big side: NOT shuffled
 +- BroadcastExchange HashedRelationBroadcastMode(...)
    +- Scan ExistingRDD[tenant_id#22, name#23, tier#24]   ← small side: broadcast
```

The `Exchange hashpartitioning` on the events is **gone**. That is the whole win: no shuffle
of the 373k rows.

**The review rule:** a `broadcast()` hint is a *promise* that the table stays small. If that
"small" dimension grows to 2 GB, a forced broadcast OOMs the driver (it collects the table)
and the executors (every one holds a copy). Use a hint only when you know the size bound;
otherwise let the threshold + table statistics decide.

**Lab 03 (`labs/03-broadcast/`)** is this exact scenario: someone disabled auto-broadcast,
and the dimension has no stats, so it is a sort-merge join. You make it a broadcast join two
ways. Do it now.

```bash
cd labs/03-broadcast && cat README.md && ./break.sh
# edit job.py, then ./verify.sh && cat solution.md
```

---

## 5. Choosing a join strategy — the short version

| Situation | Spark's choice | You should |
|---|---|---|
| one side < 10 MB **with stats** (Parquet/table) | auto `BroadcastHashJoin` | nothing — trust it |
| one side small but **no stats** (Python list, complex subquery) | `SortMergeJoin` (wrong!) | add `broadcast()` |
| both sides large | `SortMergeJoin` | accept the shuffle; make sure keys aren't skewed (Day 5) |
| tiny side you forced, that later grew | OOM | remove the hint; rely on threshold |

"Why did Spark not broadcast my small table?" is nearly always "it did not **know** it was
small." Check the plan for `Scan ExistingRDD` / missing row-count stats.

---

## 6. Too many / too few partitions (the task-count trap)

`spark.sql.shuffle.partitions` defaults to 200. For a small job that means 200 reduce tasks
doing almost nothing, each with scheduling + serialization overhead. For a huge job, 200 may
be far too few (giant partitions that OOM). The right number follows the **data size**, not a
copied config: aim for roughly **128 MB of shuffle data per partition**. AQE (Day 2 §6)
coalesces down automatically when enabled, which is why leaving AQE on is usually right.

**Lab 04a (`labs/04a-too-many-tasks/`)** has a one-day aggregate configured with 2,000
shuffle partitions and AQE off: 2,000 tasks to add up 5 numbers. On this machine that ran in
~10 s vs ~6 s fixed — a small absolute gap on a laptop, but it is the exact pattern that
makes a real cluster job spend its life in task scheduling. Fix it by sizing the partitions
to the data, or by just removing the overrides and letting AQE coalesce. Do it now.

```bash
cd labs/04a-too-many-tasks && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 7. End-of-day checklist

1. In one sentence, what does a shuffle physically move, and why is that expensive?
2. With `M` map tasks and `P` reduce partitions, roughly how many shuffle transfers happen?
3. `repartition(10)` vs `coalesce(10)` on a 200-partition DataFrame — which shuffles, which
   can't increase the count, which gives even sizes?
4. You join a 2 TB fact table to a 3-row lookup and see `SortMergeJoin` with an `Exchange` on
   both sides. What happened, and what one change fixes it?
5. Why might Spark refuse to auto-broadcast a genuinely tiny `createDataFrame` table?
6. When is forcing `broadcast()` a bug waiting to happen?

If 1–5 are solid, Day 5 turns the Spark UI into a debugging instrument: three labs — a slow
shuffle, an OOM, and data skew (the `megacorp` tenant) — read entirely from the UI.

## Cleanup
Nothing persists.
