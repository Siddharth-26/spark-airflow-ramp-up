# Day 1 — Why distributed processing exists, and your first real Spark job

**Goal:** by the end of today you can explain *why* Spark exists in plain words, you have
built a working PySpark environment by hand, you have run a real job on 370k events, and
you can read the first three lines of a query plan.

**Assumed:** Python and general programming experience. **Not assumed:** anything about
Spark, Parquet, distributed systems or "big data" — each is defined the first time it
appears.

**How to read the commands.** Every command block is followed by:
- **What it does** — the action, in one line.
- **You should see** — roughly the output (numbers and timings will differ a little).
- **What it tells you** — the fact to take away.
- **When you'll use it** — the real-work situation where you reach for it.

Run each block and compare. If yours differs, stop and find out why; the difference is
usually the lesson.

---

## 1. What you own as an SSE here

| You own | You do **not** own |
|---|---|
| Writing the Spark job: reads, transforms, writes | Running a shared 100-node cluster for other teams |
| Its correctness on messy data (nulls, duplicates, late events) | Cluster procurement, YARN/K8s capacity, cost allocation |
| Its performance: finding *why* it is slow and fixing the job | Tuning the platform itself |
| The Airflow pipeline around it (Days 8–10) | The Airflow *installation* |
| Explaining to a teammate what a job did and why it failed | |

When a problem is on the platform side, your job is to prove it with evidence (the Spark
UI, the plan) and hand it over. "Spark is slow" is not evidence; "one task in stage 4 read
8 GB while the median read 40 MB" is.

---

## 2. Why Spark exists — built in four steps

### Step A — One program, one machine

You have a file of orders and you want the number of orders per tenant. In plain Python:

```python
counts = {}
for line in open("events.jsonl"):
    tenant = json.loads(line)["tenant_id"]
    counts[tenant] = counts.get(tenant, 0) + 1
```

That works. The work is *read every line once, bump a counter*. The time it takes grows
with the file size. 50k lines: a blink. 50 million: minutes. 50 billion: hours, if it even
fits.

**Where we are:** one program, one CPU core, one machine's disk.

### Step B — Two limits you eventually hit

1. **Too slow.** One core reads so many bytes per second. A bigger machine helps for a
   while, then gets absurdly expensive.
2. **Doesn't fit.** The data is bigger than the machine's memory or disk.

Both limits have the same answer: use *many* machines.

### Step C — Split the data, run the same code on each piece

Cut the file into pieces. Give each piece to a different worker. Each worker counts *its
own* piece. This works because counting is independent per line:

```
file (400 GB)
  ├─ piece 1 ──► worker 1 ──► {acme: 120, globex: 98, ...}
  ├─ piece 2 ──► worker 2 ──► {acme: 131, globex: 90, ...}
  └─ piece 3 ──► worker 3 ──► {acme: 118, globex: 101, ...}
```

Each piece is called a **partition**. (Remember this word; it is the most important word in
Spark. A partition is a slice of the data that one worker processes at a time.)

**Where we are:** the data is split, and each piece has produced its own partial answer.

### Step D — Combine the partial answers

Three partial answers are not the answer. `acme` appears in all three. Someone must add
them up: acme = 120 + 131 + 118. That combining step requires the partial results to be
**moved to one place** (or hashed so all `acme` totals land on the same worker). That
movement over the network is called a **shuffle**, and it is the single most expensive thing
Spark does. Day 4 is about it.

**Where we are:** you now know the whole idea. Spark is a framework that (1) splits data
into partitions, (2) runs your code on each partition in parallel, (3) moves data between
workers when needed (a shuffle), and (4) hides the failure handling: if a worker dies,
Spark re-runs just that piece.

> **The one-sentence summary.** Spark lets you write code *as if* the data were one table,
> and runs it split across many workers.

Today everything runs on your laptop, which is a *very small cluster of 1*. The code you
write is identical to what would run on 100 machines. That is the point.

---

## 3. Batch vs streaming (two words you will hear constantly)

- **Batch:** "process yesterday's file." Finite input, runs, finishes. Days 1–6.
- **Streaming:** "process events as they arrive, forever." Unbounded input, never
  finishes. Day 7.

Same engine, same DataFrame API for both. We start with batch because failures are easier
to see when the job eventually ends.

---

## 4. The OrderFlow data

Your sample system is order events. An order has a short life:

```
created ──► paid ──► shipped        (most orders)
   └──────► cancelled               (a few)
```

Each step emits one event, one JSON object per line (**JSON Lines**, `.jsonl`). The
generator is `app/generate_orders.py`. It is stdlib Python, deterministic (same seed, same
data), and it can inject messiness with flags that later days switch on.

```bash
cd data-ramp-up
python3.11 app/generate_orders.py
```

**What it does:** writes 7 days × ~53k events into `data/raw/orders/dt=<date>/events.jsonl`.

**You should see:**
```
wrote 373676 events over 7 days to data/raw/orders
```

**What it tells you:** 373,676 events across 7 files. That is about 20,000 orders a day,
each with 1–4 events.

**When you'll use it:** to regenerate the data (it is gitignored) or, from Day 4, with
`--skew`, `--dupes`, `--bad`, `--late` to create specific problems.

Now look at the data before you process it. Always do this first.

```bash
head -n 3 data/raw/orders/dt=2026-10-01/events.jsonl
wc -l data/raw/orders/*/events.jsonl
du -sh data/raw/orders
```

**What it does:** prints the first three lines of one day, counts lines in every day, and
totals the size on disk.

**You should see:**
```
{"event_id":"20261001-016313-created","order_id":"20261001-016313","tenant_id":"globex","event_type":"created","amount":240.06,"currency":"GBP","event_ts":"2026-10-01T00:29:21Z"}
{"event_id":"20261001-013826-created",...,"tenant_id":"hooli",...}
...
   53352 data/raw/orders/dt=2026-10-01/events.jsonl
   ...
  373676 total
 63M	data/raw/orders
```

**What it tells you:** the fields (`event_id`, `order_id`, `tenant_id`, `event_type`,
`amount`, `currency`, `event_ts`), roughly 53k rows/day, and 63 MB total. Notice the lines
are **not in time order**: the first line is 00:29, the second 00:32, the third 20:55.
Events arrive in whatever order they arrive. Never assume order.

**When you'll use it:** the first thing you do with any new dataset, before writing any
Spark. Ninety percent of "Spark bugs" are "I did not look at my data."

---

## 5. Files and formats — why Spark jobs almost never read JSON in production

The data above is JSON: each line is a complete object. That format is easy to produce and
terrible to analyse. Here is why, in two steps.

### Step A — Row-oriented vs column-oriented

JSON (and CSV) store **one row after another**:

```
row1: [globex, 240.06, GBP, ...]
row2: [hooli,  325.45, USD, ...]
row3: [umbrella, 382.95, USD, ...]
```

To compute `sum(amount)`, you must read *every field of every row* and throw away six of
the seven. Wasteful.

**Parquet** is a *column-oriented* file format: it stores each column together.

```
tenant_id: [globex, hooli, umbrella, ...]
amount:    [240.06, 325.45, 382.95, ...]
currency:  [GBP, USD, USD, ...]
```

To compute `sum(amount)`, read only the `amount` block and skip the rest. Parquet also
stores a **schema** (names and types, so no re-guessing) and compresses each column well,
because values in one column look alike.

### Step B — Partitioned directories

A big dataset is not one file; it is a directory of many. You can organise them as
`column=value/` folders:

```
data/curated/orders/
  dt=2026-10-01/part-0000.parquet
  dt=2026-10-02/part-0000.parquet
  ...
```

This is **partitioning on disk** (a slightly different use of the word from the in-memory
partition in §2; they are related but not the same, and Day 4 unpicks it). The folder name
becomes a column, `dt`. The payoff is **partition pruning**: ask for `dt = '2026-10-03'`
and Spark only opens that one folder and skips the other six, without reading them.

Let's measure instead of believing. This is the real result from this machine.

Create `scratch.py` anywhere you like and run it in the venv (set up in §6):

```python
import time
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("d1").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

raw = spark.read.json("data/raw/orders/*/events.jsonl")
raw = raw.withColumn("dt", F.substring("event_ts", 1, 10))
raw.write.mode("overwrite").partitionBy("dt").parquet("data/curated/orders")

pq = spark.read.parquet("data/curated/orders")
t = time.time(); pq.agg(F.sum("amount")).collect(); print("parquet sum:", round(time.time()-t, 2), "s")
t = time.time(); spark.read.json("data/raw/orders/*/events.jsonl").agg(F.sum("amount")).collect(); print("json sum:", round(time.time()-t, 2), "s")
pq.filter("dt = '2026-10-03'").explain()
```

**What it does:** reads all JSON, adds a `dt` column taken from the first 10 characters of
`event_ts`, writes it as partitioned Parquet, then times the same `sum(amount)` on both
formats and prints the plan for a one-day query.

**You should see (this machine):**
```
parquet sum: 0.12 s
json sum:    0.47 s
...
FileScan parquet [...] PartitionFilters: [isnotnull(dt#67), (dt#67 = 2026-10-03)] ...
```
and on disk:
```
 63M  data/raw/orders        (JSON)
 8.3M data/curated/orders    (Parquet)
```

**What it tells you:**
- **7.6× smaller** on disk (63 MB → 8.3 MB), from columnar layout plus compression.
- **~4× faster** for a one-column aggregate, because only the `amount` column is read.
  The gap grows with data size and with the number of columns.
- `PartitionFilters: [... dt = 2026-10-03]` in the plan is **pruning happening**. Spark is
  telling you it will only open one folder. When you debug a slow job, finding this line
  (or its absence) is a standard first check.

**When you'll use it:** every time you decide how to land data. Rule of thumb: JSON/CSV at
the edge (what other systems give you), Parquet for everything you process yourself.

> **Notice something odd.** `ls data/curated/orders` lists `dt=2026-10-01` through
> `dt=2026-10-08`. Eight folders from seven input days? The raw folders are named by the
> day the *file* was written, but we partitioned by the day in `event_ts`. An order created
> at 23:50 and shipped at 00:40 lands its `shipped` event on the **next** day. Also
> `dt=2026-10-03` has 53,427 rows while the raw 10-03 file has 53,390. Same data, two
> different notions of "which day". Files-by-arrival vs partitions-by-event-time is a real,
> classic source of off-by-one-day bugs in pipelines. You will fix it properly on Day 6;
> for now just notice that it is there.

---

## 6. Build your environment by hand

You do this once manually so that `scripts/setup-venv.sh` is no longer magic.

### 6.1 Check Java

Spark is written in Scala and runs on the **JVM**, so Java must be installed even though
you write Python. (PySpark is a Python layer that talks to a JVM process.)

```bash
java -version
```

**What it does:** prints the Java version.

**You should see:** something like `openjdk version "17.0.15"`.

**What it tells you:** Spark 3.5 supports Java 8, 11, 17 and 21. A newer or older Java
(for example 22+) is not supported by this version and can fail in confusing ways, so
check the number against the version you are running.

**When you'll use it:** the first thing to run when "Spark won't start". Wrong Java is the
number-one setup failure.

### 6.2 Create an isolated Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
which python
```

**What it does:** creates a private copy of Python in `.venv/`, then switches your shell to
use it. (A **virtual environment** keeps this project's libraries from clashing with your
system's.)

**You should see:** a path ending in `data-ramp-up/.venv/bin/python`.

**What it tells you:** your `python` command is now the project's, not the system's. If
`which python` shows `/usr/bin/python3`, activation did not take effect.

**When you'll use it:** every project, always.

### 6.3 Install PySpark, pinned

```bash
pip install pyspark==3.5.3 pyarrow==17.0.0 pandas==2.2.3
python -c "import pyspark; print(pyspark.__version__)"
```

**What it does:** installs three libraries at exact versions, then prints the PySpark
version. `pyspark` bundles Spark itself (the JVM jars), so no separate Spark install is
needed. `pyarrow` and `pandas` are used when Spark exchanges data with Python.

**You should see:** `3.5.3`.

**What it tells you:** the stack works end to end. Exact versions (`==`) matter: Spark's
behaviour changes between versions, and "works on my machine" is usually a version skew.

**When you'll use it:** the same pattern for any pinned dependency set; this is what
`requirements.txt` and `setup-venv.sh` automate.

---

## 7. Lab 00 — your first job

```bash
cd labs/00-first-job
cat README.md
./break.sh          # makes sure the sample data exists
# write my_job.py (see the README)
./verify.sh         # your grade
cat solution.md     # after you pass
```

You are asked to read one day of events and count them per tenant. It is small on purpose.
The point is to see Spark run, and to meet its warnings.

When you run it you will see some noise. Two lines are harmless and appear on this
machine too:

```
WARN Utils: Your hostname ... resolves to a loopback address ... using 10.x.x.x instead
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
```

**What they mean:** Spark noticed it has no real network name and no native Hadoop
libraries, and fell back. Neither affects results. Learn to tell *noise* from *signal*:
an `ERROR`, a stack trace, or `OutOfMemoryError` is signal; these two are noise.

---

## 8. Read your first query plan

After Lab 00, add one line to your job: `agg.explain()` where `agg` is the grouped
DataFrame. Here is the real output from this machine:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[tenant_id], functions=[count(1)])
   +- Exchange hashpartitioning(tenant_id, 200)
      +- HashAggregate(keys=[tenant_id], functions=[partial_count(1)])
         +- FileScan json [tenant_id] ... ReadSchema: struct<tenant_id:string>
```

**What it does:** prints the plan Spark will execute. **Read it bottom to top**, the order
data flows.

**You should see / what it tells you, line by line:**

| Line | Meaning | Maps to §2 |
|---|---|---|
| `FileScan json [tenant_id]` | Read the files, and only the `tenant_id` field. | Step C: each worker reads its own piece |
| `HashAggregate ... partial_count(1)` | Each worker counts its own piece. | Step C: the partial answers |
| `Exchange hashpartitioning(tenant_id, 200)` | Send every row with the same `tenant_id` to the same place. | **Step D: the shuffle** |
| `HashAggregate ... count(1)` | Add the partial counts. | Step D: the combine |

You have just read the four steps from §2 as a real execution plan. The `Exchange` line is
the expensive one: whenever you see it, data is moving across the network. Day 3 teaches
the plan fully; Day 4 teaches why `Exchange` is where jobs hurt.

Also notice `ReadSchema: struct<tenant_id:string>`: Spark worked out that you only need one
field and did not read the other six. That is **column pruning**, free, because Catalyst
(Spark's optimizer, Day 3) noticed.

**When you'll use it:** `.explain()` is how you check what a job *will* do before you pay
to run it on 4 TB.

---

## 9. End-of-day checklist

Answer these out loud or in writing, without scrolling up:

1. In your own words, what problem does a partition solve?
2. Why is a shuffle needed to compute "orders per tenant" but *not* to compute "orders
   with amount > 100"? (Hint: does each row need to know about any other row?)
3. Why is Parquet faster than JSON for `sum(amount)`, and what does the `PartitionFilters`
   line tell you?
4. `java -version` prints Java 24 and Spark 3.5 fails to start. Why, and what do you do?
5. Name one reason a `dt=` folder might not contain only that date's events.

If you can answer 1–3 you are ready for Day 2: what the driver and executors actually are,
and how one job becomes stages and tasks.

---

## Cleanup

Nothing to tear down. Spark runs inside the python process and exits when it ends. If you
want to reclaim space: `rm -rf data/curated` (regenerable).
