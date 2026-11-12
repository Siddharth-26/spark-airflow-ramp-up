# Day 5 — Reading the Spark UI to debug a slow job

**Goal:** by the end of today the Spark UI is an instrument you read, not a wall of tabs. You
can find the slow stage, decide whether the problem is *too many tasks*, *too much memory in
one task (OOM)*, or *one task doing everything (skew)*, and prove it from the numbers before
you change code.

**Assumed:** Days 1–4. **Not assumed:** how to diagnose from the UI, what skew is, salting.

Today is three labs. The reading is short on purpose; the learning is in the diagnosis.

---

## 1. The debugging loop

Every Spark performance investigation is the same four steps. Memorise the loop; the labs
drill it.

1. **Find the slow job**, then its **slow stage** (Stages tab, sort by duration).
2. **Look at that stage's task table.** The three columns that diagnose almost everything:
   **Duration**, **Shuffle Read Size/Records**, and **GC Time**.
3. **Read the task *distribution*, not the total.** The UI shows Min / 25th / Median / 75th /
   Max for each metric. The gap between **Median** and **Max** is the whole game.
4. **Form one hypothesis, change one thing, re-measure.** Never change three knobs at once.

The three shapes you will learn to recognise instantly:

| What the task table shows | Diagnosis | Lab |
|---|---|---|
| hundreds/thousands of tasks, each tiny, total time dominated by overhead | **too many partitions** | 04a |
| one task with huge shuffle-read + long GC, then `OutOfMemoryError` | **OOM: working set too big** | 04b |
| Max task reads/takes 10–100× the Median; 7 tasks done, 1 still running | **data skew** | 04c |

---

## 2. Where each number lives

```
Spark UI  http://localhost:4040
 ├─ Jobs      : which action is slow (duration per job)
 ├─ Stages    : WHICH STAGE is slow → click it
 │    └─ (stage detail)
 │         ├─ Summary Metrics : Min/25th/Median/75th/Max per task  ← read the spread
 │         ├─ Event Timeline  : tasks as bars → one long bar = skew, visually
 │         └─ Task table      : per-task Duration, Shuffle Read, GC, Errors
 ├─ SQL       : the final (post-AQE) plan as a diagram; hover for row counts
 └─ Executors : per-executor memory, cores, GC, failed tasks  ← OOM shows here
```

**The single most useful view** is a stage's **Summary Metrics** table. If Median shuffle
read is 40 MB and Max is 8 GB, you have skew, full stop — no guessing.

---

## 3. Lab 04a — the slow job that is *all overhead*

The symptom: a trivial one-day aggregate is slow, and the aggregation stage shows **2,000
tasks** each doing almost nothing.

**Read it in the UI:** open the Stages tab → the reduce stage has 2,000 tasks, each a few ms,
with near-zero shuffle read. Total time is scheduling, not computing. This is the "too many
partitions" shape.

**Fix:** size `spark.sql.shuffle.partitions` to the data, or remove the override and let AQE
coalesce (Day 2 §6, Day 4 §6). Measured on this machine: ~10 s → ~6 s; on a real cluster the
gap is far larger because every one of 2,000 tasks pays real scheduling cost.

```bash
cd labs/04a-too-many-tasks && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 4. Lab 04b — the OOM

The symptom: the job dies with `java.lang.OutOfMemoryError: Java heap space`. A teammate says
"just bump the memory". Don't — understand it first.

**Read it in the UI:** the failed stage has one task with a huge "Shuffle Read Size" and long
GC time; the **Executors** tab shows a lost executor with an OOM error. The working set of
*one task* did not fit in the JVM heap.

**The cause pattern** (this lab, measured on this machine): the query uses
`collect_list("payload")` to build, per tenant, a list holding **every** payload string for
that tenant — tens of thousands of 3 KB strings in one object, in one task — only to *measure*
those lists afterwards. It materialises the whole population to produce two numbers.

**Fix:** use aggregates that keep a running value, not the values:
```python
df.groupBy("tenant_id").agg(F.count("*").alias("events"), F.sum(F.length("payload")).alias("chars"))
```
`count` and `sum` hold one number per group and support map-side partial aggregation, so
memory stays flat regardless of data size. At the lab's 480 MB driver limit, the
`collect_list` version reliably OOMs and the aggregate version passes.

**Why "bump the memory" is the wrong first answer:** it works until the data grows, then
fails again, and hides the design bug. Reach for more memory only once you have shown the
working set is genuinely that large and cannot be streamed.

```bash
cd labs/04b-oom && cat README.md && ./break.sh
PYTHONPATH=../../app python job.py 2>&1 | grep -E "ROW|OutOfMemory" | head    # see it die
# fix job.py, then ./verify.sh && cat solution.md
```

---

## 5. Lab 04c — data skew (the `megacorp` tenant)

The symptom: a join is slow; in the UI, 7 tasks finish in a second and **1 task is still
running**. The `--skew` data gives one tenant ~60% of all events.

**Read it in the UI:** the join stage's Summary Metrics show Max shuffle-read records far
above Median. The Event Timeline shows seven short bars and one long one. This is skew: a
property of the **data**, not a config.

**Why config can't fix it:** a shuffle sends every row with the same key to the same task.
`hash("megacorp") % P` always lands on one partition, so one task gets 60% of the rows no
matter how many executors you add. Measured on this machine: one task read **95,936 of
160,451** records (60%).

**Fix — salting:** split the hot key into N sub-keys on the big side, and replicate the small
side N times so every sub-key still finds its match.
```python
N = 8
events_s = events.withColumn("salt", (F.rand() * N).cast("int"))
dim_s = dim.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))
joined = events_s.join(dim_s, ["tenant_id", "salt"])
```
Measured on this machine: the heaviest task dropped from **60% → 28%** of the rows, and the
result is unchanged. The cost: the small side is duplicated N times (trivial here) and the
code is uglier.

**Know the alternatives:** AQE skew-join (`spark.sql.adaptive.skewJoin.*`) does this
automatically, but needs enough map-side partitions to split a heavy one — with only 3 input
files it cannot, which is why the manual fix is needed here. And if the dimension is truly
small, `broadcast()` (Day 4) removes the shuffle entirely and skew with it.

```bash
cd labs/04c-skew && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 6. A diagnosis cheat-sheet to keep

When a job is slow, in order:

1. **Stages tab → slowest stage.** Don't optimise a fast stage.
2. **Summary Metrics → Median vs Max.** Big gap = **skew** (§5). Small gap, huge totals =
   genuine volume → check partition count (§3) and the plan for an avoidable shuffle (Day 4).
3. **One task with long GC + OOM in Executors = memory** (§4): look for `collect_list`,
   `collect`, `groupByKey`, giant broadcasts, wide rows.
4. **Thousands of tiny tasks = too many partitions** (§3): fix `shuffle.partitions` / AQE.
5. **A shuffle you didn't expect** (Day 3 §6, Day 4): can you broadcast, or avoid the join?

"Spark is slow" is never a bug report. "Stage 4's Max task read 8 GB while the Median read
40 MB" is — and it names the fix.

---

## 7. End-of-day checklist

1. Which UI tab and which row/column tell you a stage is skewed?
2. What does the Median-vs-Max gap in Summary Metrics mean?
3. A stage has 2,000 tasks each taking 5 ms. Problem and fix?
4. A task OOMs after a long GC with a huge shuffle read. What kind of operation do you look
   for in the code, and why is "more memory" the wrong first move?
5. Why can't adding executors fix skew on a single hot key? Name two fixes that can.
6. Give the four-step debugging loop from §1.

If 1–5 are solid, Day 6 is the truth about real data: schema drift, nulls, corrupt records,
small files, duplicates and late events — three labs on data that fights back.

## Cleanup
Nothing persists. `data/skew` and `data/messy` (generated for the labs) are gitignored.
