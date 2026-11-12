# Day 6 — Data is never clean: edge cases

**Goal:** by the end of today you can defend a job against the five ways real data breaks it:
schema drift, nulls, corrupt records, small files, and duplicate/late events. You know the
difference between a job that *fails loudly* and one that *silently drops data* — the second
is the dangerous one.

**Assumed:** Days 1–5. **Not assumed:** permissive/corrupt-record modes, null semantics,
`dropDuplicates`, the small-files problem, late vs arrival time.

The `--bad`, `--dupes`, `--late`, `--skew` flags on the generator switch these on. The labs
use `data/messy` (all four) so the mess is realistic.

---

## 1. The one rule that matters most

> **Failing loudly is safe. Silently dropping data is not.** Design every ingest so that
> every input record ends up in exactly one of {valid, quarantined}, and `valid + bad = input`
> is an assertion the job can check — **check it before you dedupe**, because duplicates are
> *valid* rows you choose to collapse later, not *bad* rows. (`good` after dedupe is smaller
> than `valid`; don't assert `good + bad = input` or you'll trip on every duplicate.)

Most of today is applications of this rule.

---

## 2. Corrupt and malformed records

Spark's JSON/CSV readers default to **PERMISSIVE** mode: a malformed line does **not** fail
the job — Spark produces a row of nulls (and, with the right schema, fills a special
`_corrupt_record` column with the raw text). A naive `filter(col.isNotNull())` then silently
discards those rows *and* the legitimately-null ones, and logs nothing.

The three reader modes:

| mode | a bad line becomes | use when |
|---|---|---|
| `PERMISSIVE` (default) | a row of nulls (+ `_corrupt_record`) | you want to keep & inspect bad rows |
| `DROPMALFORMED` | silently dropped | you can discard junk and don't need to count it |
| `FAILFAST` | the job throws | any bad line means upstream is broken; stop now |

**The safe pattern** — keep bad rows, count them, quarantine the raw text:
```python
schema = ("amount double, currency string, event_id string, event_ts string, event_type string,"
          " order_id string, tenant_id string, _corrupt_record string")
df = spark.read.schema(schema).option("mode", "PERMISSIVE").json("data/messy/*/events.jsonl").cache()
ok   = F.col("_corrupt_record").isNull() & F.col("amount").isNotNull() & F.col("tenant_id").isNotNull()
good = df.filter(ok)
bad  = df.filter(~ok)
print("GOOD", good.count(), "BAD", bad.count())        # good + bad == input lines
```
`.cache()` is needed because Spark refuses a query that touches only `_corrupt_record` on an
uncached raw read.

**Measured on this machine** (`data/messy`, 3 days): GOOD **161,664**, BAD **1,677**, summing
to the input line count. The 1,677 are the `--bad` records: truncated JSON, null amount, or
missing tenant.

**Lab 05a (`labs/05a-bad-records/`)** is exactly this: a job reports a GOOD count and 1,677
records have vanished with no trace. You account for every line and quarantine the bad ones.
Do it now.

```bash
cd labs/05a-bad-records && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 3. Nulls have surprising semantics

SQL/Spark nulls are *three-valued logic*: a comparison with null is **null (unknown)**, not
true or false. This changes how aggregates and predicates behave.

```python
nn = spark.read.schema(schema).json("data/messy/*/events.jsonl")
print("rows with null amount:", nn.filter("amount is null").count())
print("count(amount):", nn.agg(F.count("amount")).first()[0], " count(*):", nn.count())
print("avg(amount):", nn.agg(F.avg("amount")).first()[0])
spark.sql("select NULL = NULL as eq, NULL <=> NULL as safe_eq, 2 not in (1, NULL) as b").show()
```

**Measured on this machine:**
```
rows with null amount: 1135
count(amount): 162206   count(*): 163341      ← count(col) SKIPS nulls; count(*) doesn't
avg(amount):   252.64                          ← avg IGNORES nulls (divides by non-null count)
eq=NULL   safe_eq=true   b=NULL                ← NULL = NULL is NULL, not true; <=> is null-safe
```

**What to take away:**
- `count(col)` ignores nulls; `count(*)` counts rows. They differ whenever the column has
  nulls — a classic "my numbers don't tie out" bug.
- `sum`/`avg` ignore nulls. `avg` divides by the *non-null* count, so a column that is half
  null still gives a plausible-looking average.
- `col = NULL` is never true. Use `.isNull()` / `.isNotNull()`, or the null-safe `<=>`
  (`eqNullSafe`) when you need `NULL = NULL → true` (e.g. joining on nullable keys).
- `x NOT IN (a, NULL)` is `NULL`, i.e. filtered out — a nasty `NOT IN` + null trap.

**When you'll use it:** every aggregate on real columns, and every join on a nullable key.

---

## 4. The small-files problem

A dataset is a directory of files. Thousands of tiny files are a performance disaster:
per-file open/list overhead (brutal on object stores like S3), a task per file downstream,
and metadata pressure. The usual cause is a wide write: `repartition(200)` then
`partitionBy("tenant_id")` writes up to 200 × (#tenants) files, each a few KB.

**Measured on this machine (Lab 05b):** one day (53,352 rows, ~6 MB) written as
`repartition(200).partitionBy("tenant_id")` produced **1,000** parquet files. The fix —
`repartition("tenant_id")` (one file per tenant) — produced **5**.

**Fixes:**
```python
df.repartition("tenant_id").write.partitionBy("tenant_id")...   # 1 file/tenant; shuffle, skew-prone for a huge tenant
df.coalesce(2).write.partitionBy("tenant_id")...                # <=2 files/tenant; no shuffle, sizes uneven
```
Target file size: roughly **128 MB–1 GB**. For data that already landed as small files, a
periodic **compaction** job (read a partition, `coalesce`, rewrite) fixes it.

**Lab 05b (`labs/05b-small-files/`)** — turn 1,000 files into ≤10, same rows. Do it now.

```bash
cd labs/05b-small-files && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 5. Duplicates — dedupe on the business key, not the whole row

`distinct()` and `dropDuplicates()` with no columns compare the **entire row**. Add any column
that varies per delivery (an ingest timestamp, a `uuid()`, a Kafka offset) and no two rows are
ever equal, so nothing is removed.

**Measured on this machine (Lab 05c):** the job adds `row_uuid = uuid()` for auditing, then
calls `.distinct()`. Result: **161,664** rows — the ~2% `--dupes` duplicates survive, because
every row's uuid is unique. Dedupe on the key instead:
```python
deduped = df.dropDuplicates(["event_id"])     # one row per event_id
```
gives **158,460** (the distinct `event_id` count).

**Which duplicate survives?** `dropDuplicates` keeps an *arbitrary* one. If duplicates can
differ (a corrected re-send), pick deterministically with a window:
```python
from pyspark.sql import Window
w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
deduped = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```
Also: `uuid()` is non-deterministic — if Spark recomputes a partition (retry, cache eviction)
the same row can get a *different* uuid. Generate audit ids **after** dedupe, or derive them
from a hash of the business key.

**Lab 05c (`labs/05c-dedupe/`)** — make the dedupe actually dedupe. Do it now.

```bash
cd labs/05c-dedupe && cat README.md && ./break.sh && ./verify.sh && cat solution.md
```

---

## 6. Late and out-of-order data — "which day" is ambiguous

Day 1 noticed it: a `dt=` folder named by the *file's* day held rows whose `event_ts` fell on
a different day, because an order created at 23:50 ships at 00:40. The `--late` flag makes
this explicit by back-dating ~3% of events by 2–5 days.

**Measured on this machine (`data/messy`):** of 162,783 events with a valid timestamp, **4,862
have an event day earlier than the file's day**, spread across lags of 1–5 days:
```
lag=1: 38   lag=2: 1256   lag=3: 1144   lag=4: 1228   lag=5: 1196
```

**The consequence:** if your daily job for `dt=2026-10-03` reads only the `dt=2026-10-03`
*file*, it misses events that *belong* to the 3rd but arrived in a later file — and includes
events that arrived in the 3rd's file but belong to earlier days. "Partition by arrival time"
and "partition by event time" are different datasets.

**How pipelines handle it:**
- **Partition by event time**, not arrival time (derive `dt` from `event_ts`, as Day 1's
  curation step did), so a row lands in the day it belongs to regardless of when it arrived.
- **Reprocess a trailing window** (e.g. re-run the last 3 days each night) so late arrivals
  are folded in — which only works if the job is *idempotent* (Day 8/9: overwrite the
  partition, don't append).
- For true streaming, bound the lateness with a **watermark** (Day 7).

This is why idempotent, partition-overwriting jobs (Days 8–10) are not a nicety: late data
*guarantees* you will reprocess days, and a non-idempotent job double-counts when you do.

---

## 7. End-of-day checklist

1. What does PERMISSIVE mode do with a malformed line, and why is `filter(isNotNull)` after
   it dangerous?
2. `count(amount)` returns 162,206 but `count(*)` returns 163,341. Why, and is that a bug?
3. Why is `col = NULL` never true, and what do you use instead?
4. `repartition(200).partitionBy("tenant_id")` wrote 1,000 tiny files. Why 1,000, and what's
   the one-line fix?
5. You add `uuid()` for auditing, then `distinct()` removes nothing. Why? Give the correct
   dedupe.
6. An event created at 23:55 on the 1st and shipped at 00:30 on the 2nd — which day's
   partition should each event land in, and what job property makes reprocessing safe?

If 1–5 are solid, Day 7 is Structured Streaming: processing an unbounded input, with
watermarks and checkpoints — the same OrderFlow data, arriving a file at a time.

## Cleanup
`data/messy`, `data/skew` are gitignored. `spark.catalog.clearCache()` if you cached.
