# Solution — Lab 05a

**Cause:** Spark's JSON reader defaults to `PERMISSIVE` mode. A malformed line does not fail
the job; Spark makes a row of all nulls (and, with inference, adds `_corrupt_record`). The
`isNotNull` filter then silently discards both those rows *and* the legitimately null ones.
Nothing is logged. Dropping data quietly is the most dangerous failure mode a pipeline has.

**Fix:**
```python
schema = ("amount double, currency string, event_id string, event_ts string, event_type string,"
          " order_id string, tenant_id string, _corrupt_record string")
df = spark.read.schema(schema).option("mode", "PERMISSIVE").json("data/messy/*/events.jsonl").cache()
good_cond = F.col("_corrupt_record").isNull() & F.col("amount").isNotNull() & F.col("tenant_id").isNotNull()
good, bad = df.filter(good_cond), df.filter(~good_cond)
print("GOOD", good.count())
print("BAD", bad.count())
bad.select(F.coalesce("_corrupt_record", F.to_json(F.struct(*[c for c in df.columns if c != "_corrupt_record"]))).alias("raw")) \
   .coalesce(1).write.mode("overwrite").text("labs/05a-bad-records/quarantine")
```
**Rules to keep:** (1) every record goes to exactly one of good/bad, so `good + bad = input`
is an assertion you can put in the job; (2) keep the raw text of failures; (3) alert when the
bad rate jumps, not when the job fails. `FAILFAST` mode is also valid for data you can
reject wholesale.
