# Solution — Lab 02

**Cause:** `big` is only a *recipe*. Each `.count()` is an action, so each one re-reads and
re-filters the files from scratch: three actions, three scans (~198 MB total input).

**Fix:** keep the result after the first action.
```python
big = (...).filter(F.col("amount") > 10).cache()
```
First `count()` reads the files and fills the cache; the next two read memory. Input drops
to ~66 MB.

**Caveats to say out loud in review:** `cache()` is itself lazy (it fills on the first
action); it uses executor memory, so don't cache things you use once; and call
`big.unpersist()` when done. An alternative is one pass with `F.count`/`F.sum(F.when(...))`
conditional aggregates, which needs no cache at all.
