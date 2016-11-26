"""

"""

# import pprint

# AWS
DATA_AMI = {"eu-west-1": {"ami": 'ami-d3225da0', "az": 'eu-west-1c', "keypair": "gazzettaEU",
                          "price": "0.3"},
            "us-west-2": {"ami": 'ami-7f5ff81f', "snapid": "snap-4f38bf1c", "az": 'us-west-2c',
                          "keypair": "gazzetta",
                          "price": "0.4"}}

CREDENTIAL_PROFILE = 'matteo'
REGION = "us-west-2"
KEYPAIR_PATH = "C:\\Users\\Matteo\\Downloads\\" + DATA_AMI[REGION]["keypair"] + ".pem"
SECURITY_GROUP = "spark-cluster"
PRICE = DATA_AMI[REGION]["price"]
INSTANCE_TYPE = "r3.4xlarge"
NUMINSTANCE = 9
EBS_OPTIMIZED = True if "r3" not in INSTANCE_TYPE else False
REBOOT = 0
KILL_JAVA = 1
NUM_RUN = 1

CLUSTER_ID = "1"
print("Cluster ID : " + str(CLUSTER_ID))
TAG = [{
    "Key": "ClusterId",
    "Value": CLUSTER_ID
}]

# HDFS
HDFS_MASTER = "ec2-35-160-124-233.us-west-2.compute.amazonaws.com"

# Spark config
SPARK_2 = "/opt/spark/"
SPARK_DOCKER = "/usr/local/spark/"
SPARK_HOME = SPARK_DOCKER

LOG_LEVEL = "INFO"
UPDATE_SPARK = 0
UPDATE_SPARK_MASTER = 0
UPDATE_SPARK_DOCKER = 0
ENABLE_EXTERNAL_SHUFFLE = "true"
LOCALITY_WAIT = 0
LOCALITY_WAIT_NODE = 0
LOCALITY_WAIT_PROCESS = 1
LOCALITY_WAIT_RACK = 0
CPU_TASK = 1
RAM_DRIVER = "50g"
RAM_EXEC = '"60g"' if "r3" not in INSTANCE_TYPE else '"100g"'
OFF_HEAP = False
if OFF_HEAP:
    RAM_EXEC = '"30g"' if "r3" not in INSTANCE_TYPE else '"70g"'
OFF_HEAP_BYTES = 30720000000

# Core Config
COREVM = 8
COREHTVM = 16
DISABLEHT = 1
if DISABLEHT:
    COREHTVM = COREVM

# CONTROL
ALPHA = 0.95
DEADLINE = 239474
# SVM
# 0%  217500
# 20% 261000
# 40% 304500
# KMeans
# 0%  166250
# 20% 199500
# 40% 232750
# PageRank
# 0%  209062
# 20% 250874
# 40% 284375
MAXEXECUTOR = 8
OVERSCALE = 2
K = 50
TI = 12000
TSAMPLE = 1000
COREQUANTUM = 0.05
COREMIN = 0.0
CPU_PERIOD = 100000

# BENCHMARK
RUN = 1
SYNC_TIME = 1
PREV_SCALE_FACTOR = 1000
BENCH_NUM_TRIALS = 1

BENCHMARK_PERF = [
    # "scala-agg-by-key",
    # "scala-agg-by-key-int",
    # "scala-agg-by-key-naive",
    # "scala-sort-by-key",
    # "scala-sort-by-key-int",
    # "scala-count",
    # "scala-count-w-fltr",
]

BENCHMARK_BENCH = [
    "PageRank",
    # "DecisionTree",
    # "KMeans",
    # "SVM"
]

if len(BENCHMARK_PERF) + len(BENCHMARK_BENCH) > 1 or len(BENCHMARK_PERF) + len(
        BENCHMARK_BENCH) == 0:
    print("ERROR BENCHMARK SELECTION")
    exit(1)

# config: (line, value)
BENCH_CONF = {
    "scala-agg-by-key": {
        "ScaleFactor": 10
    },
    "scala-agg-by-key-int": {
        "ScaleFactor": 5
    },
    "scala-agg-by-key-naive": {
        "ScaleFactor": 10
    },
    "scala-sort-by-key": {
        "ScaleFactor": 100
    },
    "scala-sort-by-key-int": {
        "ScaleFactor": 50
    },
    "scala-count": {
        "ScaleFactor": 10
    },
    "scala-count-w-fltr": {
        "ScaleFactor": 10
    },
    "PageRank": {
        "NUM_OF_PARTITIONS": (3, 1000),
        "numV": (2, 7000000),
        "MAX_ITERATION": (8, 1)
    },
    "KMeans": {
        # DataGen
        "NUM_OF_POINTS": (2, 100000000),
        "NUM_OF_CLUSTERS": (3, 10),
        "DIMENSIONS": (4, 20),
        "SCALING": (5, 0.6),
        "NUM_OF_PARTITIONS": (6, 1000),
        # Run
        "MAX_ITERATION": (8, 1)
    },
    "DecisionTree": {
        "NUM_OF_PARTITIONS": (4, 1200),
        "NUM_OF_EXAMPLES": (2, 50000000),
        "NUM_OF_FEATURES": (3, 6),
        "NUM_OF_CLASS_C": (7, 10),
        "MAX_ITERATION": (21, 1)
    },
    "SVM": {
        "NUM_OF_PARTITIONS": (4, 1000),
        "NUM_OF_EXAMPLES": (2, 200000000),
        "NUM_OF_FEATURES": (3, 10),
        "MAX_ITERATION": (7, 1)
    }
}
if len(BENCHMARK_PERF) > 0:
    SCALE_FACTOR = BENCH_CONF[BENCHMARK_PERF[0]]["ScaleFactor"]
    INPUT_RECORD = 200 * 1000 * 1000 * SCALE_FACTOR
    NUM_TASK = SCALE_FACTOR
else:
    SCALE_FACTOR = BENCH_CONF[BENCHMARK_BENCH[0]]["NUM_OF_PARTITIONS"][1]
    NUM_TASK = SCALE_FACTOR
    try:
        INPUT_RECORD = BENCH_CONF[BENCHMARK_BENCH[0]]["NUM_OF_EXAMPLES"][1]
    except KeyError:
        try:
            INPUT_RECORD = BENCH_CONF[BENCHMARK_BENCH[0]]["NUM_OF_POINTS"][1]
        except KeyError:
            INPUT_RECORD = BENCH_CONF[BENCHMARK_BENCH[0]]["numV"][1]
BENCH_CONF[BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0]][
    "NumTrials"] = BENCH_NUM_TRIALS

# Terminate istance after benchmark
TERMINATE = 0

# HDFS
HDFS = 1 if HDFS_MASTER == "" else 0
HADOOP_CONF = "/usr/local/lib/hadoop-2.7.2/etc/hadoop/"
DELETE_HDFS = 1 if SCALE_FACTOR != PREV_SCALE_FACTOR else 0

# CONFIG JSON
CONFIG_DICT = {
    "Benchmark": {
        "Name": BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0],
        "Config": BENCH_CONF[BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0]]
    },
    "Deadline": DEADLINE,
    "Control": {
        "Alpha": ALPHA,
        "OverScale": OVERSCALE,
        "MaxExecutor": MAXEXECUTOR,
        "CoreVM": COREVM,
        "K": K,
        "Ti": TI,
        "TSample": TSAMPLE,
        "CoreQuantum": COREQUANTUM
    },
    "Aws": {
        "InstanceType": INSTANCE_TYPE,
        "HyperThreading": not DISABLEHT,
        "Price": PRICE,
        "AMI": DATA_AMI[REGION]["ami"],
        "Region": REGION,
        "AZ": DATA_AMI[REGION]["az"],
        "SecurityGroup": SECURITY_GROUP,
        "KeyPair": DATA_AMI[REGION]["keypair"],
        "EbsOptimized": EBS_OPTIMIZED,
        "SnapshotId": DATA_AMI[REGION]["snapid"]
    },
    "Spark": {
        "ExecutorCore": COREVM,
        "ExecutorMemory": RAM_EXEC,
        "ExternalShuffle": ENABLE_EXTERNAL_SHUFFLE,
        "LocalityWait": LOCALITY_WAIT,
        "LocalityWaitProcess": LOCALITY_WAIT_PROCESS,
        "LocalityWaitNode": LOCALITY_WAIT_NODE,
        "LocalityWaitRack": LOCALITY_WAIT_RACK,
        "CPUTask": CPU_TASK,
        "SparkHome": SPARK_HOME
    },
    "HDFS": bool(HDFS)
}

# Line needed for enabling/disabling benchmark in spark-perf config.py
BENCH_LINES = {"scala-agg-by-key": ["226", "227"],
               "scala-agg-by-key-int": ["230", "231"],
               "scala-agg-by-key-naive": ["233", "234"],
               "scala-sort-by-key": ["237", "238"],
               "scala-sort-by-key-int": ["240", "241"],
               "scala-count": ["243", "244"],
               "scala-count-w-fltr": ["246", "247"]}

# pprint.pprint(CONFIG_DICT)
