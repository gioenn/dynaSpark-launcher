# AWS
dataAMI = {"eu-west-1": {"ami": 'ami-d3225da0', "az": 'eu-west-1c', "keypair": "gazzettaEU", "price": "0.4"},
           "us-west-2": {"ami": 'ami-4a994b2a', "az": 'us-west-2c', "keypair": "gazzetta", "price": "0.7"}}

REGION = "us-west-2"
KEYPAIR_PATH = "C:\\Users\\Matteo\\Downloads\\" + dataAMI[REGION]["keypair"] + ".pem"
SECURITY_GROUP = "spark-cluster"

PRICE = dataAMI[REGION]["price"]
INSTANCE_TYPE = "r3.4xlarge"
NUMINSTANCE = 0
EBS_OPTIMIZED = True if not "r3" in INSTANCE_TYPE else False


# Core
COREVM = 8
COREHTVM = 16
DISABLEHT = 0
if DISABLEHT:
    COREHTVM = COREVM


# CONTROL
ALPHA = 0.8
DEADLINE = 200000
MAXEXECUTOR = 4
OVERSCALE = 2
K = 75
TI = 10000
TSAMPLE = 5000
COREQUANTUM = 1


# BENCHMARK
RUN = 1
HDFS = 1
PREV_SCALE_FACTOR = 3
SCALE_FACTOR = 10
DELETE_HDFS = 1 if SCALE_FACTOR != PREV_SCALE_FACTOR else 0
RAM_EXEC = '"60g"' if not "r3" in INSTANCE_TYPE else '"100g"'

linesBench = {"scala-agg-by-key": ["226", "227"],
              "scala-agg-by-key-int": ["230", "231"],
              "scala-agg-by-key-naive": ["233", "234"],
              "scala-sort-by-key": ["237", "238"],
              "scala-sort-by-key-int": ["240", "241"],
              "scala-count": ["243", "244"],
              "scala-count-w-fltr": ["246", "247"]}
BENCHMARK_PERF = [# "scala-agg-by-key",
             # "scala-agg-by-key-int",
             "scala-agg-by-key-naive",
             #"scala-sort-by-key",
             # "scala-sort-by-key-int",
             # "scala-count",
             # "scala-count-w-fltr",
             ]
BENCHMARK_BENCH = [ # "PageRank",
             ]

# Terminate istance after benchmark
TERMINATE = 0

# PLOT ALL
PLOT_ALL = 0
