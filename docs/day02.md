# Day 2 — Spark's architecture: driver, executors, jobs, stages and tasks

**Goal:** by the end of today you can name every moving part of a running Spark job and
point at it in the Spark UI. When someone says "the job is stuck in stage 3", you know what
a stage is, why there are stages at all, and where to look.

**Assumed:** Day 1 (you can run a PySpark job and read the first lines of a plan). **Not
assumed:** anything about clusters, executors or the UI.

**How to read the commands.** Same as Day 1: every block has *what it does*, *you should
see*, *what it tells you*, *when you'll use it*. Run them in your venv from the repo root.

---

## 1. The problem this architecture solves

Day 1's idea: split the data into partitions, run the same code on each, shuffle when you
must combine. Today is the *machinery* that does that. Four words carry the whole chapter:

- **Driver** — the one process that holds your program and hands out work.
- **Executor** — a worker process that does the work and holds data in memory.
- **Job / Stage / Task** — the three-level breakdown of "what needs doing".

Everything else is detail hung off these four.

---

## 2. Driver and executors — who runs what

### Step A — The driver

When you call `SparkSession.builder...getOrCreate()`, the process that runs your Python file
becomes (or connects to) the **driver**. The driver:

- holds your code and the `SparkSession`;
- builds the plan (the `explain()` output from Day 1);
- breaks it into tasks and schedules them;
- collects results back (e.g. `.collect()` pulls rows *to the driver*).

There is exactly **one** driver per application. If the driver dies, the application dies.
This is why `.collect()` on a huge result is dangerous: it pulls everything into the one
driver's memory (you will cause this on purpose in Lab 04b).

### Step B — The executors

**Executors** are separate JVM processes that actually run tasks. Each executor has:

- a fixed number of **cores** (how many tasks it can run at once);
- a chunk of **memory** (for the data it processes and caches).

On a cluster there are many executors on many machines. On your laptop in `local[2]` there
is **one** process that plays *both* driver and executor, with **2** task slots. That is
what `local[2]` means: run locally, with 2 worker threads.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("d2").master("local[2]").getOrCreate()
sc = spark.sparkContext
print("master:", sc.master)
print("default parallelism (task slots):", sc.defaultParallelism)
print("Spark UI:", sc.uiWebUrl)
```

**What it does:** starts Spark locally with 2 slots and prints how Spark sees itself.

**You should see (this machine):**
```
master: local[2]
default parallelism (task slots): 2
Spark UI: http://<your-host>:4040
```

**What it tells you:** `local[2]` → 2 tasks run at once. `local[*]` would use every core.
`defaultParallelism` is the number of tasks Spark will try to keep busy at once — on a
cluster it is roughly total executor cores.

**When you'll use it:** the first thing to confirm when a job runs single-threaded ("why is
nothing parallel?") — check the master string and core count.

### Step C — The cluster manager (one line for now)

On a real cluster, something has to *start* those executors and give them machines: that is
the **cluster manager** (YARN, Kubernetes, or Spark's own standalone master). In `local`
mode there is no cluster manager — Spark just starts threads. You do not need one until the
cloud capstone (Day 12), so we name it and move on.

```
          ┌────────────── your application ──────────────┐
          │                                               │
   your code → DRIVER ──schedules tasks──► EXECUTOR 1  (cores: ■ ■)
                  │                         EXECUTOR 2  (cores: ■ ■)
                  └──results come back──    EXECUTOR 3  (cores: ■ ■)
   (local[2] = driver + ONE executor with 2 cores, all in one process)
```

---

## 3. One action → a Job

Day 1 said: transformations are lazy, an **action** triggers execution. Each action you call
(`count`, `collect`, `show`, `write`) becomes one **Job** (sometimes a couple). "How many
jobs did my script run?" = "how many actions did I call?" — roughly.

```python
from pyspark.sql import functions as F
df = spark.read.json("data/raw/orders/*/events.jsonl")
rev = df.filter(F.col("amount") > 100).groupBy("tenant_id").agg(F.sum("amount").alias("rev"))
rev.orderBy("tenant_id").collect()      # ONE action → one job (plus a tiny setup job)
```

Open the UI's **Jobs** tab while this runs (or right after) and you see the job listed with
a description like `collect at ...`.

**Measured on this machine**, the script above produced these jobs (from the UI's REST API):
```
JOB 0  json at ...           (read the file schema)
JOB 1  collect at ...        (the real work)
```

**What it tells you:** reading JSON needs a peek at the data to infer the schema, so it costs
a small extra job. Give the reader an explicit `schema=` (as the labs do) and that job
disappears. One action ≈ one job is the mental model; the UI is the ground truth.

---

## 4. One Job → Stages (and why the boundary is always a shuffle)

A job is split into **stages**. The rule is simple and worth memorising:

> **A new stage starts at every shuffle.**

Why? A stage is a run of work that needs **no data movement between tasks** — each task reads
its input, does its thing, done. The moment the computation needs rows from one partition to
meet rows from another (a `groupBy`, a `join`, a `distinct`, a `repartition`), the data must
move across the network. Spark cannot continue the same stage across that boundary, because
the next step's input does not exist until the movement finishes. So it ends the stage,
performs the **shuffle**, and starts a new stage to consume the shuffled data.

Look again at Day 1's plan for `groupBy("tenant_id")`:

```
HashAggregate(... count(1))              ← stage N+1 (reduce side: combine)
  Exchange hashpartitioning(tenant_id)   ← THE SHUFFLE = the stage boundary
    HashAggregate(... partial_count(1))  ← stage N   (map side: per-partition partials)
      FileScan json [tenant_id]          ← stage N   (read)
```

`Exchange` is the shuffle. Everything below it is one stage (read + partial aggregate);
everything above it is the next stage (final aggregate). That is why even a trivial
`groupBy` always shows **two** stages in the UI.

**Measured on this machine** for the `rev` query above, the completed stages were:
```
STAGE (map)    tasks=3   input=66,044,493 bytes   shuffleWrite=984 bytes
STAGE (reduce) tasks=1   shuffleRead=984 bytes     shuffleWrite=0
```

**What it tells you:** the map stage read 66 MB (the JSON) and wrote under 1 KB to the
shuffle (just 5 tenant partial sums per partition). The reduce stage read that 1 KB back and
produced the answer. **The shuffle here is tiny** — that is why it is fast. When a shuffle is
*not* tiny (Day 4), this is exactly where the pain shows up.

---

## 5. One Stage → Tasks (the unit of work)

A **task** is one stage's work on **one partition**. So:

> number of tasks in a stage = number of partitions the stage operates on.

- The map stage above had **3 tasks** because the JSON reader made 3 input partitions (one
  per ~22 MB split — Spark targets ~128 MB but our files are smaller, and it still split
  into 3 here).
- The reduce stage had **1 task** — not 200. You asked for 200 (`spark.sql.shuffle.partitions`
  default), but **Adaptive Query Execution (AQE)** saw the shuffle output was under 1 KB and
  coalesced it to a single partition. You will prove this in **Lab 01**.

Tasks of the same stage run **in parallel, up to your core count**. With `local[2]`, 3 map
tasks run 2-at-a-time then 1 more. On a cluster with 50 cores they would all run at once.

```
Job
 └─ Stage 0 (map)     : task task task          ← 3 tasks, 2 slots → runs 2+1
 └─ Stage 1 (reduce)  : task                     ← 1 task (AQE coalesced 200→1)
```

---

## 6. AQE — the thing that will confuse you in the UI

**Adaptive Query Execution** (default **on** since Spark 3.2) re-plans parts of the query
*at runtime* using the real sizes it observes, instead of trusting the up-front estimate.
The two you will meet first:

- **Coalesce shuffle partitions:** merges many tiny post-shuffle partitions into fewer tasks
  (why your 200 became 1 above).
- **Skew-join handling and join-strategy switches:** Day 4/5.

The practical consequence: **"tasks ≠ the number I configured" is usually AQE, not a bug.**
In the plan, AQE shows up as `AdaptiveSparkPlan isFinalPlan=false` at the top — `false`
because the final shape is decided only once it runs. You can see the *final* plan in the
UI's SQL tab after the query completes.

**Lab 01 (`labs/01-one-task/`)** makes you meet this head-on: you set 8 shuffle partitions,
see 1 task, and work out why. Do it now.

```bash
cd labs/01-one-task
cat README.md
./break.sh
PYTHONPATH=../../app python job.py      # run from repo root in practice; see README
./verify.sh
cat solution.md
```

---

## 7. Map it in the live UI

Run any multi-stage job and open `http://localhost:4040` **while it runs** (the UI dies when
the process exits; in a short script, add `input("enter to quit")` at the end to keep it up).

```python
import time
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("d2-ui").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.json("data/raw/orders/*/events.jsonl")
df.groupBy("tenant_id").count().orderBy("tenant_id").show()
input("Spark UI is at http://localhost:4040 — look around, then press Enter to quit...")
```

**What to find in the UI, tab by tab:**

| Tab | What it shows | Day 2 concept |
|---|---|---|
| **Jobs** | one row per action | §3 (one action → a job) |
| **Stages** | the stages of a job, task counts, shuffle read/write | §4, §5 |
| **SQL / DataFrame** | the query as a diagram, with the *final* (post-AQE) plan | §6 |
| **Executors** | each executor's cores, memory, tasks, GC time | §2 |

**You should see:** under **Executors**, a single `driver` row with 2 cores (that is your
`local[2]`). Under **Stages**, two stages for the `groupBy`, the second much smaller.

**When you'll use it:** every time you debug. Day 5 is nothing but this UI used in anger.

---

## 8. `local[2]` vs a real cluster — what actually changes

Almost nothing in your *code*. What changes is the physical picture:

| | `local[2]` (your laptop) | real cluster |
|---|---|---|
| driver | a thread in your process | one process, often on a gateway/edge node |
| executors | the same process, 2 slots | many processes across many machines |
| cluster manager | none | YARN / Kubernetes / standalone |
| a task failing | retried locally | retried, possibly on a different machine |
| data | local disk | HDFS / S3 / cloud storage |

The partition→task→stage model is **identical**. That is the entire reason this course can
teach you real Spark on a laptop: you are running the same engine, just with one small
executor. Day 12 changes only `master(...)` and the submit command to put it on a cluster.

---

## 9. End-of-day checklist

Answer without scrolling up:

1. How many drivers does an application have? What happens if it dies?
2. What does `local[2]` mean, precisely, in terms of driver/executor/cores?
3. State the rule for where a new stage begins. Why *there* and not elsewhere?
4. A stage shows 3 tasks. Where did the number 3 come from?
5. You set `spark.sql.shuffle.partitions=200` and the reduce stage has 1 task. Give the
   one-word reason and where you'd confirm it.
6. Why does `.collect()` on a 500M-row result threaten the *driver* specifically?

If 1–5 are solid, Day 3 goes deep on the plan: transformations vs actions, laziness, and the
Catalyst optimizer that produced those `HashAggregate`/`Exchange` lines.

## Cleanup
Nothing persistent. If you added `input(...)` to keep the UI alive, the process holds until
you press Enter.
