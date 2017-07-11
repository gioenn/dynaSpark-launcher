from enum import Enum

class Heuristic(Enum):
    CONTROL = 0
    FIXED = 1
    CONTROL_UNLIMITED = 2

"""
Configuration module of cSpark test benchmark
"""
# NEW
PROVIDER = "AZURE"
# PROVIDER = "AWS_SPOT"
"""Provider to be used"""
print("Provider : " + PROVIDER)

AZ_LOCATION = 'westeurope'
"""AZURE Datacenter Location"""
# AZ_SIZE = 'Basic_A2'
# AZ_SIZE = 'Standard_G3'
AZ_SIZE = 'Standard_D14_v2_Promo'
"""AZURE VM Size"""
AZ_IMAGE = 'Canonical:UbuntuServer:14.04.5-LTS:14.04.201703230'
"""AZURE VM Image"""
AZ_VHD_IMAGE = {"StorageAccount": "csparkdisks2",
                "BlobContainer": "vhds",
                "Name": "vm-os.vhd"}  # csparkvm13-os.vhd
"""AZURE VHD Image"""
# ssh-keygen -t rsa -b 2048
AZ_KEY_NAME = "id_rsa"
"""Name of the RSA 2048 key"""
AZ_PUB_KEY_PATH = 'C:\\Users\\Simone Ripamonti\\Desktop\\' + AZ_KEY_NAME + '.pub'
"""AZURE Public Key Path (RSA 2048)"""
AZ_PRV_KEY_PATH = 'C:\\Users\\Simone Ripamonti\\Desktop\\' + AZ_KEY_NAME
"""AZURE Private Key Path (RSA 2048)"""
AZ_RESOURCE_GROUP = 'cspark'
"""AZURE Resource Group"""
AZ_STORAGE_ACCOUNT = 'csparkdisks2'
"""AZURE Storage Group"""
AZ_SA_SKU = "standard_lrs"
"""AZURE Storage SKU"""
AZ_SA_KIND = "storage"
"""AZURE Storage Kind"""
AZ_NETWORK = 'cspark-vnet2'
"""AZURE Virtual Network"""
AZ_SUBNET = "default"
"""AZURE Subnet"""
AZ_SECURITY_GROUP = 'cspark-securitygroup2'
"""AZURE Security Group"""
#AWS
DATA_AMI = {"eu-west-1": {"ami": 'ami-bf61fbc8',  # modificata per funzionare con t1.small
                          "az": 'eu-west-1c',
                          "keypair": "simone",
                          "price": "0.0035"},
            "us-west-2": {"ami": 'ami-7f5ff81f',  # 'ami-05930d65',
                          "snapid": "snap-4f38bf1c",  # "snap-5c511c0b",
                          "az": 'us-west-2c',
                          "keypair": "simone2",
                          "price": "0.015"}}
"""AMI id for region and availability zone"""

CREDENTIAL_PROFILE = 'cspark'
"""Credential profile name of AWS"""
REGION = "us-west-2"
"""Region of AWS to use"""
KEY_PAIR_PATH = "C:\\Users\\Simone Ripamonti\\Desktop\\" + DATA_AMI[REGION]["keypair"] + ".pem"
"""KeyPair path for the instance"""
SECURITY_GROUP = "spark-cluster"
"""Secutiry group of the instance"""
PRICE = DATA_AMI[REGION]["price"]
INSTANCE_TYPE = "m3.medium"
"""Instance type"""
NUM_INSTANCE = 0
"""Number of instance to use"""
EBS_OPTIMIZED = True if "r3" not in INSTANCE_TYPE else False
REBOOT = 0
"""Reboot the instances of the cluster"""
KILL_JAVA = 1
"""Kill every java application on the cluster"""
NUM_RUN = 1
"""Number of run to repeat the benchmark"""

CLUSTER_ID = "CSPARKWORK"
# CLUSTER_ID = "CSPARKHDFS"
# CLUSTER_ID = "DEV"
"""Id of the cluster with the launched instances"""
print("Cluster ID : " + str(CLUSTER_ID))
TAG = [{
    "Key": "ClusterId",
    "Value": CLUSTER_ID
}]

# HDFS
# HDFS_MASTER = "ec2-52-88-156-209.us-west-2.compute.amazonaws.com"
HDFS_MASTER = "10.0.0.5"  # use private ip for azure!
# HDFS_MASTER = ""
"""Url of the HDFS NameNode if not set the cluster created is an HDFS Cluster"""
# Spark config
SPARK_2_HOME = "/opt/spark/"
C_SPARK_HOME = "/usr/local/spark/"
SPARK_HOME = C_SPARK_HOME
"""Location of Spark in the ami"""

LOG_LEVEL = "INFO"
UPDATE_SPARK = 0
"""Git pull and build Spark of all the cluster"""
UPDATE_SPARK_MASTER = 0
"""Git pull and build Spark only of the master node"""
UPDATE_SPARK_DOCKER = 0
"""Pull the docker image in each node of the cluster"""
ENABLE_EXTERNAL_SHUFFLE = "true"
LOCALITY_WAIT = 0
LOCALITY_WAIT_NODE = 0
LOCALITY_WAIT_PROCESS = 1
LOCALITY_WAIT_RACK = 0
CPU_TASK = 1
RAM_DRIVER = "100g"
RAM_EXEC = '"100g"' if "r3" not in INSTANCE_TYPE else '"100g"'
OFF_HEAP = False
if OFF_HEAP:
    RAM_EXEC = '"30g"' if "r3" not in INSTANCE_TYPE else '"70g"'
OFF_HEAP_BYTES = 30720000000

# Core Config
CORE_VM = 16
CORE_HT_VM = 16
# CORE_HT_VM = 2
# CORE_VM = 2
DISABLE_HT = 0
if DISABLE_HT:
    CORE_HT_VM = CORE_VM

# CONTROL
ALPHA = 0.95
BETA = 0.33
# DEADLINE = 160000 # kmeans
# DEADLINE = 120000 # agg by key
DEADLINE = 37600

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
MAX_EXECUTOR = 4  # 9
OVER_SCALE = 2
K = 50
TI = 12000
T_SAMPLE = 1000
CORE_QUANTUM = 0.05
CORE_MIN = 0.0
CPU_PERIOD = 100000

# BENCHMARK
RUN = 1
SYNC_TIME = 1
PREV_SCALE_FACTOR = 0
"""*Important Settings* if it is equals to SCALE_FACTOR no need to generate new data on HDFS"""
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
"""Spark-perf benchmark to execute"""

BENCHMARK_BENCH = [
    # "PageRank",
    "DecisionTree",
    # "KMeans",
    # "SVM"
]
"""Spark-bench benchmark to execute"""

if len(BENCHMARK_PERF) + len(BENCHMARK_BENCH) > 1 or len(BENCHMARK_PERF) + len(
        BENCHMARK_BENCH) == 0:
    print("ERROR BENCHMARK SELECTION")
    exit(1)

# config: (line, value)
BENCH_CONF = {
    "scala-agg-by-key": {
        "ScaleFactor": 5
    },
    "scala-agg-by-key-int": {
        "ScaleFactor": 5
    },
    "scala-agg-by-key-naive": {
        "ScaleFactor": 5
    },
    "scala-sort-by-key": {
        "ScaleFactor": 25
    },
    "scala-sort-by-key-int": {
        "ScaleFactor": 25
    },
    "scala-count": {
        "ScaleFactor": 25
    },
    "scala-count-w-fltr": {
        "ScaleFactor": 25
    },
    "PageRank": {
        "NUM_OF_PARTITIONS": (3, 1000),
        # "NUM_OF_PARTITIONS": (3, 10),
        "numV": (2, 3000000),  # 7000000
        # "numV": (2, 50000),
        "mu": (4, 3.0),
        "MAX_ITERATION": (8, 1),
        "NumTrials": 1
    },
    "KMeans": {
        # DataGen
        "NUM_OF_POINTS": (2, 100000000),
        "NUM_OF_CLUSTERS": (3, 10),
        "DIMENSIONS": (4, 10),
        "SCALING": (5, 0.6),
        "NUM_OF_PARTITIONS": (6, 1000),
        # Run
        "MAX_ITERATION": (8, 1)
    },
    "DecisionTree": {
        "NUM_OF_PARTITIONS": (4, 1000),
        "NUM_OF_EXAMPLES": (2, 50000000),
        "NUM_OF_FEATURES": (3, 6),
        "NUM_OF_CLASS_C": (7, 10),
        "MAX_ITERATION": (21, 1)
    },
    "SVM": {
        "NUM_OF_PARTITIONS": (4, 1000),
        "NUM_OF_EXAMPLES": (2, 100000000),
        "NUM_OF_FEATURES": (3, 5),
        "MAX_ITERATION": (7, 1)
    }
}
"""Setting of the supported benchmark"""
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
HDFS = 1 if HDFS_MASTER == "" else 0  # TODO Fix this variable for plot
HADOOP_CONF = "/usr/local/lib/hadoop-2.7.2/etc/hadoop/"
HADOOP_HOME = "/usr/local/lib/hadoop-2.7.2"
DELETE_HDFS = 1 if SCALE_FACTOR != PREV_SCALE_FACTOR else 0

# HEURISTICS
HEURISTIC = Heuristic.CONTROL_UNLIMITED

# KMEANS
STAGE_ALLOCATION = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
CORE_ALLOCATION = [14.6552, 1.3668, 4.2932, 3.2259, 5.9839, 3.1770, 5.2449, 2.5064, 6.5889, 2.6935, 5.9204, 2.8042,
                   9.6728, 1.7509, 3.8915, 0.7313, 12.2620, 3.1288]
DEADLINE_ALLOCATION = [18584, 1733, 5444, 4090, 7588, 4028, 6651, 3178, 8355, 3415, 7507, 3556, 12266, 2220, 4934, 927,
                       15549, 3967]

# SVM
# STAGE_ALLOCATION = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 18, 23]
# CORE_ALLOCATION = [0.3523, 7.9260, 0.6877, 1.3210, 0.5275, 4.1795, 7.8807, 6.6329, 2.2927, 2.9942, 3.2869, 3.2016,
#                    0.8377]
# DEADLINE_ALLOCATION = [953, 21451, 1861, 3575, 1427, 11312, 21329, 17952, 6205, 8103, 8896, 8665, 2267]

# AGG BY KEY
# STAGE_ALLOCATION = [0, 1]
# CORE_ALLOCATION = [6.3715 , 2.2592]
# DEADLINE_ALLOCATION = [84158, 29841]

# STAGE_ALLOCATION = None
# CORE_ALLOCATION = None
# DEADLINE_ALLOCATION = None

assert(isinstance(HEURISTIC, Heuristic))
if(HEURISTIC == Heuristic.FIXED):
    assert (len(STAGE_ALLOCATION) == len(DEADLINE_ALLOCATION))
    assert (len(STAGE_ALLOCATION) == len(CORE_ALLOCATION))

GIT_BRANCH = "core_tasks_antiwindup"
# CONFIG JSON
CONFIG_DICT = {
    "Provider": PROVIDER,
    "Benchmark": {
        "Name": BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0],
        "Config": BENCH_CONF[BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0]]
    },
    "Deadline": DEADLINE,
    "Control": {
        "Alpha": ALPHA,
        "Beta": BETA,
        "OverScale": OVER_SCALE,
        "MaxExecutor": MAX_EXECUTOR,
        "CoreVM": CORE_VM,
        "K": K,
        "Ti": TI,
        "TSample": T_SAMPLE,
        "CoreQuantum": CORE_QUANTUM,
        "Heuristic": HEURISTIC.name,
        "CoreAllocation": CORE_ALLOCATION,
        "DeadlineAllocation": DEADLINE_ALLOCATION,
        "StageAllocation": STAGE_ALLOCATION
    },
    "Aws": {
        "InstanceType": INSTANCE_TYPE,
        "HyperThreading": not DISABLE_HT,
        "Price": PRICE,
        "AMI": DATA_AMI[REGION]["ami"],
        "Region": REGION,
        "AZ": DATA_AMI[REGION]["az"],
        "SecurityGroup": SECURITY_GROUP,
        "KeyPair": DATA_AMI[REGION]["keypair"],
        "EbsOptimized": EBS_OPTIMIZED,
        "SnapshotId": DATA_AMI[REGION]["snapid"]
    },
    "Azure": {
        "NodeSize": AZ_SIZE,
        "NodeImage": AZ_VHD_IMAGE,
        "Location": AZ_LOCATION,
        "PubKeyPath": AZ_PUB_KEY_PATH,
        "ClusterId": CLUSTER_ID,
        "ResourceGroup": AZ_RESOURCE_GROUP,
        "StorageAccount": {"Name": AZ_STORAGE_ACCOUNT,
                           "Sku": AZ_SA_SKU,
                           "Kind": AZ_SA_KIND},
        "Network": AZ_NETWORK,
        "Subnet": AZ_SUBNET,
        "SecurityGroup": AZ_SECURITY_GROUP
    },
    "Spark": {
        "ExecutorCore": CORE_VM,
        "ExecutorMemory": RAM_EXEC,
        "ExternalShuffle": ENABLE_EXTERNAL_SHUFFLE,
        "LocalityWait": LOCALITY_WAIT,
        "LocalityWaitProcess": LOCALITY_WAIT_PROCESS,
        "LocalityWaitNode": LOCALITY_WAIT_NODE,
        "LocalityWaitRack": LOCALITY_WAIT_RACK,
        "CPUTask": CPU_TASK,
        "SparkHome": SPARK_HOME
    },
    "HDFS": bool(HDFS),
}

# Line needed for enabling/disabling benchmark in spark-perf config.py
BENCH_LINES = {"scala-agg-by-key": ["225", "226"],
               "scala-agg-by-key-int": ["229", "230"],
               "scala-agg-by-key-naive": ["232", "233"],
               "scala-sort-by-key": ["236", "237"],
               "scala-sort-by-key-int": ["239", "240"],
               "scala-count": ["242", "243"],
               "scala-count-w-fltr": ["245", "246"]}

# NEW
PRIVATE_KEY_PATH = KEY_PAIR_PATH if PROVIDER == "AWS_SPOT" \
    else AZ_PRV_KEY_PATH if PROVIDER == "AZURE" \
    else None
PRIVATE_KEY_NAME = DATA_AMI[REGION]["keypair"] + ".pem" if PROVIDER == "AWS_SPOT" \
    else AZ_KEY_NAME if PROVIDER == "AZURE" \
    else None
TEMPORARY_STORAGE = "/dev/xvdb" if PROVIDER == "AWS_SPOT" \
    else "/dev/sdb1" if PROVIDER == "AZURE" \
    else None

# Print what will run
print("Schedule : "
      + ("LAUNCH, " if (NUM_INSTANCE > 0) else "")
      + ("REBOOT, " if (REBOOT) else "")
      + ("RUN( " if (RUN) else "")
      + ("HDFS )" if (HDFS_MASTER == "" and RUN) else "SPARK )" if (RUN) else "")
      + ("TERMINATE" if (TERMINATE) else ""))

UPDATE_SPARK_BENCH = False
UPDATE_SPARK_PERF = False


SPARK_PERF_FOLDER = "spark-perf-mod"