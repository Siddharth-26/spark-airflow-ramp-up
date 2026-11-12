# Solution — Lab 05b

**Cause:** each of the 200 in-memory partitions holds a bit of every tenant, and each task
writes its own file into every `tenant_id=` folder it has rows for: up to 200 x 5 = 1,000
files, each ~6 KB. The write is the product of "number of partitions" and "number of output
folders".

**Fix:** make the partitioning of the DataFrame match the layout on disk.
```python
df.repartition("tenant_id").write.partitionBy("tenant_id")...    # 1 file per tenant (5)
# or
df.coalesce(2).write.partitionBy("tenant_id")...                 # <= 2 files per tenant
```
`repartition("tenant_id")` shuffles so each tenant's rows are together (fewest files, but
skew-prone for a huge tenant); `coalesce(n)` avoids a shuffle but can give uneven files.

**Target:** files of roughly 128 MB-1 GB. Small files hurt through per-file open/list
overhead (object stores especially), huge task counts downstream, and NameNode/metadata
pressure. For the data that already landed, a periodic compaction job fixes it.
