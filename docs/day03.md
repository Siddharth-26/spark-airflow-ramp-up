# Day 3 — DataFrames, laziness, and the query plan

**Goal:** by the end of today you can look at any chain of DataFrame calls and say which
lines *do nothing yet* and which line *triggers the work*, and you can read a full
`explain(True)` top to bottom. This is the single most useful debugging skill in Spark: most
"bugs" are a misunderstanding of *when* and *in what order* things run.

**Assumed:** Day 2 (driver/executor, job/stage/task). **Not assumed:** transformations vs
actions, laziness, Catalyst — all defined below.

---

## 1. A DataFrame is a recipe, not a table

Coming from pandas or SQL result sets, the trap is to think a DataFrame *holds rows*. It does
not. A **DataFrame is a description of a computation** — a recipe for how to produce rows
from some source. Writing `df.filter(...)` does not filter anything; it returns a *new recipe*
that says "...and then filter". No data is read, no executor does anything.

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("d3").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

import time
t = time.time()
df = spark.read.json("data/raw/orders/*/events.jsonl")      # recipe: "read these files"
big = df.filter(F.col("amount") > 100)                       # recipe: "...then keep amount>100"
by_t = big.groupBy("tenant_id").agg(F.sum("amount"))         # recipe: "...then sum per tenant"
print("built the recipe in", round(time.time() - t, 3), "s")
```

**What it does:** builds a three-step recipe and times how long *building* took.

**You should see (this machine):**
```
built the recipe in 0.03 s
```

**What it tells you:** 66 MB of JSON, three "operations", and it took 30 milliseconds —
because **nothing ran**. You only assembled a plan. Compare to Day 1, where `.collect()`
on a similar query took ~1 s: that second is the actual work.

**When you'll use it:** when a line that "should be slow" returns instantly, or a line that
"should be instant" takes a minute — you are confusing recipe-building with execution.

---

## 2. Transformations vs actions

Every DataFrame method is one of two kinds:

- **Transformation** — returns a new DataFrame (a bigger recipe). **Lazy.** Runs nothing.
  `select`, `filter`, `withColumn`, `groupBy().agg()`, `join`, `distinct`, `orderBy`,
  `repartition`, `withColumnRenamed`, `drop`...
- **Action** — asks for an actual result, so it **runs the whole recipe**. `count`,
  `collect`, `show`, `take`, `first`, `write`, `foreach`, `toPandas`...

Rule of thumb: **if it returns a DataFrame, it's lazy; if it returns rows, a number, or
writes files, it's an action.**

```python
rows = by_t.collect()        # ACTION — now all three recipe steps execute
for r in rows: print(r)
```

**What it tells you:** the read, the filter and the aggregate all happen *now*, triggered by
`collect()`. Not one of them happened on the earlier lines.

---

## 3. Laziness has a sharp edge: each action re-runs the whole recipe

This is the one that bites everyone. A DataFrame remembers *how to produce itself*, not its
rows. So **every action re-executes the entire lineage from the source**, unless you tell
Spark to keep a result.

```python
import time
big = spark.read.json("data/raw/orders/*/events.jsonl").filter(F.col("amount") > 100)
for label in ("first", "second", "third"):
    t = time.time(); n = big.count(); print(label, "count:", n, "in", round(time.time()-t, 2), "s")
```

**You should see (this machine):** three counts, each taking roughly the **same** time
(~0.6–0.9 s) — because each `count()` re-reads and re-filters all 66 MB from disk. Spark did
not "remember" the filtered data between calls.

**The fix is `cache()` / `persist()`:**
```python
big = big.cache()            # mark it: keep the result in executor memory after first compute
big.count()                  # first action fills the cache (still reads from disk)
big.count()                  # now reads from memory — much faster
```

**Caveats to keep:** `cache()` is itself *lazy* (it fills on the first action, not when you
call it); it consumes executor memory, so don't cache things you touch once; call
`unpersist()` when done. **Lab 02 (`labs/02-scan-once/`)** is exactly this bug — three
counts, three scans — and you fix it. Do it now.

```bash
cd labs/02-scan-once && cat README.md && ./break.sh
# edit job.py, then:
./verify.sh && cat solution.md
```

---

## 4. Catalyst: the optimizer between your recipe and the tasks

You write a recipe. Before running it, Spark's optimizer — **Catalyst** — rewrites it into a
faster, equivalent one. You met its output on Day 1 (`FileScan ... ReadSchema:
struct<tenant_id>` even though the files have 7 fields). Today you read all four stages of a
plan. Run:

```python
big = (spark.read.json("data/raw/orders/*/events.jsonl")
            .filter(F.col("amount") > 100)
            .select("tenant_id", "amount"))
big.groupBy("tenant_id").agg(F.sum("amount")).explain(True)
```

`explain(True)` prints **four** plans. Here is the **real output from this machine**, trimmed,
with what each stage means:

```
== Parsed Logical Plan ==          ← your recipe, exactly as written, unchecked
'Aggregate ['tenant_id], [... sum('amount) ...]
 +- Project [tenant_id, amount]
    +- Filter (amount > 100)
       +- Relation [...7 columns...] json

== Analyzed Logical Plan ==        ← names/types resolved; errors caught here
tenant_id: string, sum(amount): double
Aggregate [tenant_id#14], [... sum(amount#8) ...]
 +- Project [tenant_id#14, amount#8]
    +- Filter (amount#8 > cast(100 as double))      ← note: your 100 (int) cast to double
       +- Relation [...] json

== Optimized Logical Plan ==       ← Catalyst's rewrite
Aggregate [tenant_id#14], [... sum(amount#8) ...]
 +- Project [tenant_id#14, amount#8]
    +- Filter (isnotnull(amount#8) AND (amount#8 > 100.0))   ← added isnotnull; folded cast
       +- Relation [...] json

== Physical Plan ==                ← what actually runs, as tasks
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[tenant_id#14], functions=[sum(amount#8)])
   +- Exchange hashpartitioning(tenant_id#14, 8)             ← the shuffle (Day 2 §4)
      +- HashAggregate(keys=[tenant_id#14], functions=[partial_sum(amount#8)])
         +- Filter (isnotnull(amount#8) AND (amount#8 > 100.0))
            +- FileScan json [amount#8,tenant_id#14]
               PushedFilters: [IsNotNull(amount), GreaterThan(amount,100.0)]
               ReadSchema: struct<amount:double,tenant_id:string>
```

**What each plan is for:**

| Plan | Who makes it | What it tells you when debugging |
|---|---|---|
| **Parsed** | the parser | that Spark read your code as you meant it |
| **Analyzed** | the analyzer | column/type resolution; `AnalysisException`s come from here |
| **Optimized** | Catalyst | *what Spark decided to actually compute* (often less than you wrote) |
| **Physical** | the planner | the real operators + the shuffle(s) = where time goes |

**Three Catalyst wins visible above, each a real optimisation you rely on:**

1. **Column pruning** — `ReadSchema: struct<amount, tenant_id>`: only 2 of 7 fields are read
   from disk. You wrote `select` late; Catalyst pushed it down to the scan.
2. **Predicate / filter pushdown** — `PushedFilters: [... GreaterThan(amount, 100.0)]`: the
   `amount > 100` filter is handed to the file reader, so fewer rows even enter Spark.
3. **Null-check insertion & constant folding** — it added `isnotnull(amount)` (an aggregate
   can skip nulls) and folded your `cast(100 as double)` into the literal `100.0`.

**When you'll use it:** reading the **Optimized** plan answers "why did my filter not run?"
or "why is it still reading all columns?"; reading the **Physical** plan answers "where is
the shuffle?" and "which join did it pick?" (Day 4).

---

## 5. Why you should (mostly) trust Catalyst

The instinct from hand-tuned code is to pre-optimise: filter early, select only what you
need, reorder joins. With Spark, **write the query for clarity and let Catalyst reorder it.**
The plan above proves it moved your `select` down to the scan and pushed the filter into the
reader even though you wrote them "late". Fighting the optimizer — caching needlessly,
manual repartitions, clever reorderings — usually makes things slower and the code harder to
read.

**The exceptions, where you *do* intervene** (and the rest of the course is largely these):
- join strategy when Catalyst lacks size stats (Day 4, Lab 03);
- partition count when it does not match your data (Day 4, Lab 04a);
- data skew, which the cost model cannot see from stats (Day 5, Lab 04c);
- caching a result reused across *actions* (today, Lab 02).

So: trust it by default; override it only with evidence from the plan or the UI.

---

## 6. A habit worth forming: predict the plan, then check

For the rest of the course, before you run a non-trivial query, say out loud: *how many
shuffles? how many stages? which columns get read?* Then run `explain()` and compare. Every
surprise is a thing you did not understand about your own query. Try it now:

```python
# PREDICT first: how many Exchange lines? how many stages in the UI?
events = spark.read.json("data/raw/orders/*/events.jsonl")
counts = events.groupBy("tenant_id").count()              # shuffle #1
joined = events.join(counts, "tenant_id")                 # shuffle #2 (join)
joined.explain()
```

**You should see (this machine):** a `SortMergeJoin` with an `Exchange` on **each** side,
*and* the `Exchange` from the `groupBy` — three shuffles total, so four stages. If you
predicted one shuffle, you just learned that a join is itself a shuffle (the whole of Day 4).

---

## 7. End-of-day checklist

1. Is `df.filter(...)` a transformation or an action? Does it read any data?
2. You call `df.count()` three times on an uncached DataFrame read from 10 GB of JSON. How
   many times is the 10 GB read? How do you make it once?
3. Name the four plans `explain(True)` prints, in order, and say which one tells you what
   Spark will *actually compute*.
4. In the Physical plan, which line is the shuffle? Which line proves column pruning?
5. Why is "filter early, select early" less necessary in Spark than in hand-written code?
6. Give one case where you *should* override Catalyst rather than trust it.

If 1–4 are solid, Day 4 is the expensive part of every real job: shuffles and joins — what a
shuffle physically moves, broadcast vs sort-merge join, and `repartition` vs `coalesce`.

## Cleanup
`spark.catalog.clearCache()` if you cached anything; otherwise nothing persists.
