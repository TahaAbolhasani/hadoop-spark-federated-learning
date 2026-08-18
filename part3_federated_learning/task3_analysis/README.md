# Task 3 — Performance, Scalability & Federated Learning Analysis

## 1. Performance & Scalability

### Hadoop (Task 1)

The MapReduce job processed 173 input records (172 data rows + 1 header) in roughly 20 seconds end-to-end, based on the job log:

| Stage | Timestamp | Elapsed |
|---|---|---|
| Job submitted | 06:49:35 | — |
| Map 100% | 06:49:49 | ~14s |
| Reduce 100% | 06:49:55 | ~6s |
| Job completed | 06:49:56 | — |

Counters from the run confirm the shape of the job: `Map input records=173`, `Map output records=172` (header correctly skipped), `Reduce input groups=3` (one per client), `Reduce output records=3`.

For a dataset this small, almost none of that ~20 seconds is spent on actual computation — it's dominated by fixed costs: YARN allocating a container, the JVM starting up, and the single map task reading its input split. **This overhead does not scale with the size of the dataset.** If the input were 100x larger, the job would likely still complete in well under a minute more, because the map and reduce logic themselves are O(n) and trivially parallelizable — the bottleneck would shift from fixed startup cost to the shuffle phase, since all values for a given `client_id` must be transferred to a single reducer. With only 3 distinct keys (3 clients), that shuffle is inherently unbalanced: it cannot use more than 3 reducers no matter how many nodes the cluster has. A real deployment with far more clients would parallelize much better, since the number of reduce tasks (and thus achievable parallelism) is bounded by the number of distinct keys.

Resource utilization during the run was minimal — a single mapper and single reducer, well under the memory ceiling reported in the job counters (`Physical memory (bytes) snapshot=492703744`, ~470MB) — meaning the pseudo-cluster had significant unused capacity for this workload.

### Spark (Task 2)

The Spark job completed in a few seconds, most of which was `SparkContext` initialization rather than computation. Once initialized, each stage (the `sum` aggregations, and the three `collect` calls for the federated averaging rounds) finished in roughly 60–150ms — see `output/spark_run_output.txt` for exact per-stage timings.

Scalability here depends on two different axes:
- **More clients** — since the whole dataset (3 client tuples) is trivially small, adding more clients would have almost no effect until the dataset stops fitting comfortably in a single executor's memory. Spark's partitioning would then spread the RDD across more executors, and the per-round `collect()` calls (which pull all results back to the driver) would start to matter more, since `collect()` doesn't scale well for large result sets.
- **More rounds** — the current implementation recomputes `local_averages` from the *original* RDD every round rather than carrying the updated averages into the next round's RDD (this mirrors the assignment's own example code). This means each round's cost stays constant rather than growing — a real iterative Spark workload would typically increase its round-over-round cost only if it built up a longer transformation lineage, and would benefit from calling `.cache()` on data reused across rounds, which this script does not currently do.

### Spark vs. Hadoop, observed

The gap between the two systems is small on this dataset simply because both run to completion in single-digit seconds — the workload is too small to expose Spark's core advantage (avoiding repeated disk I/O). The difference that *is* visible is structural: Hadoop's ~20s includes a full JVM/YARN cold start for a single job, while Spark, once its context is up, executes each stage in well under a second because there's no disk round-trip between operations. On a workload with many more rounds or a much larger dataset, this gap would widen sharply in Spark's favor, since Hadoop would pay its shuffle-to-disk cost on every single stage while Spark's in-memory RDDs stay resident (or would, with explicit caching) across the whole computation.

## 2. Federated Learning: Challenges & Benefits

### Challenges observed in this implementation

- **Data partitioning.** A real FL system keeps each client's raw data physically on that client's device and never centralizes it. This project's Task 1 does the opposite for convenience: `data.csv` is a single file containing all clients' data, uploaded to one HDFS cluster. This is a legitimate simplification for learning the aggregation mechanics, but it means Task 1, as implemented, models the *aggregation server's* computation, not genuine on-device federated computation — the privacy-preserving property of real FL (raw data never leaving the client) isn't actually present here.
- **Communication overhead.** In Hadoop, the shuffle phase — transferring all of a key's values to one reducer — is a reasonable stand-in for the client-to-server communication cost in real FL, where each round requires clients to transmit model updates over a network. Both are bottlenecked by the same fundamental constraint: total data moved, not raw compute.
- **Fault tolerance.** Hadoop's replication and Spark's lineage-based recovery both assume a comparatively stable cluster. Real FL clients are far less predictable — they can be mobile devices that drop offline mid-round — so production FL frameworks need round-level fault tolerance (skipping or timing out stragglers) that neither Hadoop nor Spark provide out of the box.

### Benefits of using Hadoop/Spark for the aggregation steps implemented here

- **Hadoop** was a good fit for Task 1 because the per-client aggregation is a single, one-shot batch computation over static data — exactly the workload MapReduce's model was designed for, and its disk-backed shuffle guarantees the job survives node failures without extra code.
- **Spark** was a good fit for Task 2 because federated averaging is inherently iterative (multiple rounds over the same aggregated data), and Spark's in-memory RDDs avoid re-reading data from disk on every round — the same property that makes it well suited to real federated-averaging implementations that run many more rounds than the 3 simulated here.

### Advantages / disadvantages summary

| | Advantage in this context | Disadvantage in this context |
|---|---|---|
| Hadoop (Task 1) | Simple, reliable batch aggregation; scales to very large raw datasets | High fixed overhead per job; shuffle parallelism capped by number of distinct clients |
| Spark (Task 2) | Fast iteration over the same in-memory data; natural fit for multi-round averaging | Requires enough memory to hold the working set; `collect()`-heavy code doesn't scale to very large client populations |
