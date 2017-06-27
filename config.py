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

ALLOCATION_DICT = {
    "KMeans": {
        "StageAllocation": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
        "CoreAllocation": [10.9914, 1.0251, 3.2199, 2.4194, 4.4879, 2.3828, 3.9337, 1.8798, 4.9417, 2.0201, 4.4403,
                           2.1032, 7.2546, 1.3132, 2.9186, 0.5485, 9.1965, 2.3466],
        "DeadlineAllocation": [24779, 2311, 7259, 5454, 10117, 5371, 8868, 4237, 11140, 4554, 10010, 4741, 16354, 2960,
                               6579, 1236, 20732, 5290],
        "Deadline": 160000
    },
    "PageRank": {
        "StageAllocation": [0, 1, 2, 3, 4, 5, 6, 7, 13],
        "CoreAllocation": [1.4627, 0.6024, 0.8787, 1.3136, 0.6964, 1.3104, 0.9905, 1.1176, 0.4977],
        "DeadlineAllocation": [70494, 29031, 42349, 63310, 33564, 63157, 47738, 53866, 23988],
        "Deadline": 450000
    },
    "SVM": {
        "StageAllocation": [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 18, 23],
        "CoreAllocation": [1.0, 3.8043, 0.3301, 0.6341, 0.2532, 2.006, 3.7827, 3.1837, 1.1007, 1.4372, 1.5777, 1.5367,
                           0.4021],
        "DeadlineAllocation": [1986, 44693, 3877, 7448, 2973, 23569, 44436, 37401, 12925, 16882, 18533, 18052, 4723],
        "Deadline": 250000
    },
    "scala-agg-by-key": {
        "StageAllocation": [0, 1],
        "CoreAllocation": [6.3715, 2.2592],
        "DeadlineAllocation": [84158, 29841],
        "Deadline": 120000
    },
    "scala-agg-by-key-int": {
        "StageAllocation": [0, 1],
        "CoreAllocation": [4.9625, 2.0589],
        "DeadlineAllocation": [107428, 44571],
        "Deadline": 160000
    },
    "scala-agg-by-key-naive": {
        "StageAllocation": [0, 1],
        "CoreAllocation": [7.7674, 3.0333],
        "DeadlineAllocation": [136639, 53360],
        "Deadline": 200000
    },
    "scala-count": {
        "StageAllocation": [0],
        "CoreAllocation": [2.4494],
        "DeadlineAllocation": [38000],
        "Deadline": 40000
    },
    "scala-count-w-fltr": {
        "StageAllocation": [0],
        "CoreAllocation": [2.6900],
        "DeadlineAllocation": [38000],
        "Deadline": 40000
    },
    "scala-sort-by-key": {
        "StageAllocation": [0, 1, 2],
        "CoreAllocation": [2.7255, 4.2021, 6.0727],
        "DeadlineAllocation": [33858, 52201, 75440],
        "Deadline": 170000
    },
    "scala-sort-by-key-int": {
        "StageAllocation": [0, 1, 2],
        "CoreAllocation": [4.328, 5.644, 6.5725],
        "DeadlineAllocation": [24851, 32408, 37739],
        "Deadline": 100000
    }
}

STAGE_ALLOCATION = None
CORE_ALLOCATION = None
DEADLINE_ALLOCATION = None

BENCH_NAME = BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0]
if HEURISTIC == Heuristic.FIXED:
    STAGE_ALLOCATION = ALLOCATION_DICT[BENCH_NAME]["StageAllocation"]
    CORE_ALLOCATION = ALLOCATION_DICT[BENCH_NAME]["CoreAllocation"]
    DEADLINE_ALLOCATION = ALLOCATION_DICT[BENCH_NAME]["DeadlineAllocation"]
    DEADLINE = ALLOCATION_DICT[BENCH_NAME]["Deadline"]

assert (isinstance(HEURISTIC, Heuristic))

if HEURISTIC == Heuristic.FIXED:
    assert (len(STAGE_ALLOCATION) == len(DEADLINE_ALLOCATION))
    assert (len(STAGE_ALLOCATION) == len(CORE_ALLOCATION))



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
      + ("REBOOT, " if REBOOT else "")
      + ("RUN( " if RUN else "")
      + (
      "HDFS )" if (HDFS_MASTER == "" and RUN) else "SPARK " + HEURISTIC.name + " " + BENCH_NAME + " )" if (RUN) else "")
      + ("TERMINATE" if TERMINATE else ""))

UPDATE_SPARK_BENCH = False
UPDATE_SPARK_PERF = False
# GIT_BRANCH = "xSpark-1.0"
GIT_BRANCH = "dev-multiapp-fix-2"

PROFILING_FILES = {
    "scala-agg-by-key": {
        "Source": "TestRunner__aggregate-by-key_20170511110351.json",
        "Destination": "TestRunner__aggregate-by-key.json",
    },
    "scala-agg-by-key-int": {
        "Source": "TestRunner__aggregate-by-key-int_20170511112110.json",
        "Destination": "TestRunner__aggregate-by-key-int.json",
    },
    "scala-agg-by-key-naive": {
        "Source": "TestRunner__aggregate-by-key-naive_20170511114259.json",
        "Destination": "TestRunner__aggregate-by-key-naive.json",
    },
    "scala-sort-by-key": {
        "Source": "TestRunner__sort-by-key_20170511131321.json",
        "Destination": "TestRunner__sort-by-key.json",
    },
    "scala-sort-by-key-int": {
        "Source": "TestRunner__sort-by-key-int_20170511133334.json",
        "Destination": "TestRunner__sort-by-key-int.json",
    },
    "scala-count": {
        "Source": "TestRunner__count_20170511135036.json",
        "Destination": "TestRunner__count.json",
    },
    "scala-count-w-fltr": {
        "Source": "TestRunner__count-with-filter_20170511140627.json",
        "Destination": "TestRunner__count-with-filter.json",
    },
    "PageRank": {
        "Source": "Spark_PageRank_Application_20170531101020.json",
        "Destination": "Spark_PageRank_Application.json",
    },
    "DecisionTree": {
        "Source": "DecisionTree_classification_Example_20170523134646.json",
        "Destination": "DecisionTree_classification_Example.json",
    },
    "KMeans": {
        "Source": "Spark_KMeans_Example_20170509081738.json",
        "Destination": "Spark_KMeans_Example.json",
    },
    "SVM": {
        "Source": "SVM_Classifier_Example_20170509105715.json",
        "Destination": "SVM_Classifier_Example.json",
    }
}

PROFILE_SOURCE_FOLDER = "C:\\workspace\\spark-log-profiling\\output_json\\"
PROFILE_DEST_FOLDER = "/usr/local/spark/conf/"

PROFILING_MODE = False # True to delete profiles
UPDATE_PROFILES = False # True to upload a local profile file

COMPOSITE_BENCH = {
    # "KMeans": {
    #     "Delay": 0,
    #     "Deadline": 160000
    # },
    # "SVM": {
    #     "Delay": 30,
    #     "Deadline": 250000
    # },
    # "scala-agg-by-key": {
    #     "Delay": 60,
    #     "Deadline": 120000
    # },
    # "scala-agg-by-key-int": {
    #     "Delay": 90,
    #     "Deadline": 160000
    # }
    "scala-agg-by-key": {
        "Delay": 0,
        "Deadline": 120000
    },
    "scala-agg-by-key-int": {
        "Delay": 30,
        "Deadline": 160000
    },
    "scala-agg-by-key-naive": {
        "Delay": 60,
        "Deadline": 200000
    }
}

if len(COMPOSITE_BENCH) > 0:
    RAM_EXEC = '"{}g"'.format(int(100/ len(COMPOSITE_BENCH)))
    print(str(RAM_EXEC))

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
    "CompositeBench": COMPOSITE_BENCH
}