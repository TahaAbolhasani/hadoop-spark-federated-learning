from pyspark import SparkContext
import random
import re
import os

# Path to the Hadoop MapReduce output file (produced in Task 1).
# Expected line format: "client1\tSum: 2197.6, Count: 58"
HADOOP_OUTPUT_PATH = "../task1_hadoop/output/result.txt"


def parse_hadoop_output(path):
    """
    Reads the Hadoop Reducer output and converts each line into
    (client_id, (sum_of_feature_value, count)).
    Returns None if the file is missing, so the caller can fall back
    to simulated data instead of crashing.
    """
    if not os.path.exists(path):
        return None

    parsed = []
    # Matches: client_id <tab or spaces> Sum: <number>, Count: <number>
    line_pattern = re.compile(r"^(\S+)\s+Sum:\s*([\d.]+),\s*Count:\s*(\d+)")

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            match = line_pattern.match(line)
            if match:
                client_id = match.group(1)
                feature_sum = float(match.group(2))
                count = int(match.group(3))
                parsed.append((client_id, (feature_sum, count)))

    return parsed if parsed else None


# Initialize SparkContext for local execution
sc = SparkContext("local", "FederatedAveraging")

# Try to load real results from the Hadoop job first.
data = parse_hadoop_output(HADOOP_OUTPUT_PATH)

if data is not None:
    print(f"Loaded {len(data)} client records from Hadoop output: {HADOOP_OUTPUT_PATH}")
else:
    # Fallback: simulated aggregated results, used only if Hadoop output
    # is not available (e.g. running this script on its own for testing).
    print("Hadoop output not found, using simulated data instead.")
    data = [("client1", (2197.6, 58)),
            ("client2", (2381.8, 57)),
            ("client3", (2568.6, 57))]

for row in data:
    print(row)

# Parallelizing the data to create an RDD (Resilient Distributed Dataset)
rdd = sc.parallelize(data)

# Calculate the initial global average across all clients
total_sum = rdd.map(lambda x: x[1][0]).sum()   # sum of feature values across all clients
total_count = rdd.map(lambda x: x[1][1]).sum()  # total number of records across all clients
global_average = total_sum / total_count

print(f"Initial Global Average: {global_average}")

# Federated averaging rounds (multiple rounds of local model updates and global aggregation)
for round_num in range(3):
    # Calculate local averages for each client
    local_averages = rdd.map(lambda x: (x[0], x[1][0] / x[1][1])).collect()
    print(f"Round {round_num + 1} Local Averages: {local_averages}")

    # Simulate a local update: each client nudges its local average
    # by a small random value, as if it had run one local training step.
    updated_averages = [(client, avg + random.uniform(-1, 1)) for client, avg in local_averages]
    print(f"Round {round_num + 1} Updated Averages: {updated_averages}")

    # Recalculate the global average after updating each client's local model
    global_average = sum([avg for client, avg in updated_averages]) / len(updated_averages)
    print(f"Round {round_num + 1} Global Average: {global_average}")

sc.stop()
