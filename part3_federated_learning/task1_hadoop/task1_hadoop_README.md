# Task 1 — Hadoop MapReduce: Per-Client Data Aggregation

## Problem

In a Federated Learning setting, before any global model update can happen, each client's local data needs to be summarized — for example, computing the sum and count of a feature so a global average can later be derived. This task implements that "local aggregation" step as a Hadoop MapReduce job: given a CSV file where each row belongs to a specific client, compute the total feature value and the number of records for each client.

## Input

`data/data.csv` — 172 rows (plus a header), each row shaped like:

```
client_id,feature_value,other_data
client1,10.5,abc
client2,15.2,def
client1,12.0,ghi
```

`other_data` is not used — only `client_id` and `feature_value` matter for this job.

## Approach

The job follows the standard MapReduce pattern:

- **Map** (`ClientAggregationMapper.java`) — reads each CSV line, skips the header, and emits `(client_id, "feature_value,1")`. The trailing `1` is a per-record counter used to compute the count during reduction.
- **Shuffle** — Hadoop groups all emitted values by `client_id` and routes each group to a single reducer.
- **Reduce** (`ClientAggregationReducer.java`) — for each `client_id`, sums the feature values and counts how many records contributed, then emits `client_id -> "Sum: X, Count: Y"`.
- **Driver** (`ClientAggregationDriver.java`) — wires the Mapper/Reducer together and configures the job's input/output paths.

One implementation detail worth calling out: `feature_value` is a decimal (e.g. `10.5`), so the reducer accumulates the sum as a `double` rather than an `int` — parsing a value like `10.5` with `Integer.parseInt` would throw a `NumberFormatException`.

## Environment

The job runs on a small pseudo-distributed Hadoop cluster defined in `../../../docker/hadoop/docker-compose.yml`: a NameNode, a DataNode, a Secondary NameNode, and YARN (Resource Manager + Node Manager), using the `gelog/hadoop` image.

## How to Run

```bash
# 1. Start the Hadoop cluster
cd docker/hadoop
docker compose up -d

# 2. Copy the source code and dataset into the NameNode container
docker cp ../../part3_federated_learning/task1_hadoop/src hdfs-namenode:/tmp/src
docker cp ../../part3_federated_learning/task1_hadoop/data/data.csv hdfs-namenode:/tmp/data.csv

# 3. Compile and package the job
docker exec -it hdfs-namenode bash
cd /tmp/src
javac -cp $(hadoop classpath) ClientAggregationMapper.java ClientAggregationReducer.java ClientAggregationDriver.java
jar cf clientagg.jar *.class

# 4. Upload the dataset to HDFS
hadoop fs -mkdir -p /input
hadoop fs -put /tmp/data.csv /input/data.csv

# 5. Run the job
hadoop jar clientagg.jar ClientAggregationDriver /input/data.csv /output

# 6. Inspect the result
hadoop fs -cat /output/part-r-00000
```

Job progress can also be monitored via the web UIs:
- HDFS NameNode — `http://localhost:50070`
- YARN Resource Manager (All Applications) — `http://localhost:8088`

## Result

```
client1  Sum: 2197.6000000000004, Count: 58
client2  Sum: 2381.7999999999997, Count: 57
client3  Sum: 2568.5999999999985, Count: 57
```

(The trailing decimal noise, e.g. `2197.6000000000004`, is standard floating-point summation error — not a bug.)

Job counters confirm the job processed the data as expected: `Map input records=173` (172 data rows + 1 header), `Map output records=172` (header correctly skipped), `Reduce input groups=3` (one per client), `Reduce output records=3`.

Full deliverables:
- Source code: `src/`
- Compiled jar: `output/clientagg.jar`
- Job output: `output/result.txt`
- Execution screenshots: `screenshots/` — terminal job execution (`Running.png`, `Running2.png`), final aggregated output (`Result.png`), HDFS NameNode overview (`NameNodeUI.png`), and YARN application list (`AllAplicationHadoopUI.png`)

## Where This Feeds Into the Project

The per-client `(sum, count)` pairs produced here are exactly the input Task 2 (Spark) consumes to compute the global average and simulate federated averaging — see `../task2_spark/task2_spark_README.md`.
