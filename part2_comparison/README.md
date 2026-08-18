# Part 2 — Comparative Analysis: Spark vs. Hadoop

## Processing Speed

Hadoop MapReduce writes intermediate data to disk between the Map and Reduce phases (the shuffle step), and each job stage runs as a separate set of JVM processes launched by YARN. This makes every job pay a fixed cost in disk I/O and process startup, regardless of how small the computation is.

Spark, by contrast, keeps intermediate data in memory across transformations whenever possible, and reuses a single set of executor processes for the whole application instead of launching new ones per stage. This is why Spark is typically reported to be 10–100x faster than Hadoop MapReduce on iterative or multi-stage workloads — the speedup comes almost entirely from avoiding repeated disk I/O, not from a fundamentally different execution model.

In this project, the Hadoop job (Task 1) took on the order of ~20 seconds end-to-end for a 172-record dataset, most of which was YARN container allocation and JVM startup rather than the actual aggregation logic. The Spark job (Task 2), running three rounds of federated averaging over the same amount of data, completed in a few seconds including its own startup — most individual stages executed in well under 100ms once the SparkContext was initialized. On a dataset this small, the difference is dominated by fixed overhead rather than raw processing time, but the gap would widen substantially on larger datasets or with more iterations, since Hadoop pays its disk-I/O cost on every stage while Spark increasingly benefits from cached, in-memory data.

## Data Handling & Data Structures

| | Hadoop | Spark |
|---|---|---|
| Storage model | HDFS — data is physically split into blocks and replicated across DataNodes | RDDs/DataFrames — in-memory, partitioned collections; can read from HDFS, local disk, or be created directly in code |
| Fault tolerance | Achieved through data replication (each block is stored 2-3 times) | Achieved through *lineage* — Spark remembers the sequence of transformations used to build an RDD and recomputes lost partitions instead of relying on replicated copies |
| Intermediate results | Written to disk between Map and Reduce | Kept in memory (spilling to disk only under memory pressure) |
| Programming model | Key/value pairs only (`map` → `shuffle` → `reduce`) | Richer set of transformations (`map`, `filter`, `join`, `groupBy`, SQL-like operations on DataFrames) |

This difference in fault-tolerance strategy is a big part of why Spark can afford to skip disk writes: replication guarantees durability at the cost of I/O, while lineage-based recovery trades a small risk of recomputation for much faster normal-case execution.

## Typical Use Cases

- **Hadoop MapReduce** is best suited to large, one-pass batch jobs where the dataset doesn't fit in memory and durability matters more than latency — e.g., nightly ETL jobs, log processing, or building large static indexes. Its simple, rigid two-stage model is easy to reason about and scales predictably to very large clusters.
- **Spark** is better suited to iterative or interactive workloads: machine learning training loops, graph algorithms, ad-hoc data exploration, and streaming — anything where the same data is processed multiple times, or where low latency matters. Its richer API also makes it a more natural fit for workloads that don't map cleanly onto a single map/reduce pass.

In the context of this project, this distinction maps directly onto the two tasks: Task 1 (a single-pass aggregation over a static CSV) is a natural fit for Hadoop's batch model, while Task 2 (repeated rounds of averaging over the same in-memory dataset) is exactly the kind of iterative workload Spark was designed for.
