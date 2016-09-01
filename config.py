# AWS
dataAMI = {"eu-west-1": {"ami": 'ami-a03d4fd3', "az": 'eu-west-1c', "keypair": "gazzettaEU", "price": "0.3"},
           "us-west-2": {"ami": 'ami-0bb5646b', "az": 'us-west-2c', "keypair": "gazzetta", "price": "1"}}

REGION = "us-west-2"
KEYPAIR_PATH = "C:\\Users\\Matteo\\Downloads\\" + dataAMI[REGION]["keypair"] + ".pem"
SECURITY_GROUP = "spark-cluster"

PRICE = dataAMI[REGION]["price"]
INSTANCE_TYPE = "r3.4xlarge"
NUMINSTANCE = 5
EBS_OPTIMIZED = True if not "r3" in INSTANCE_TYPE else False

# Core
COREVM = 8
COREHTVM = 16
DISABLEHT = 0
if DISABLEHT:
    COREHTVM = COREVM

# CONTROL
TSAMPLE = 3000
DEADLINE = 50000
MAXEXECUTOR = 4
ALPHA = 0.8
OVERSCALE = 2

# BENCHMARK
SCALE_FACTOR = 1
RAM_EXEC = '"60g"' if not "r3" in INSTANCE_TYPE else '"100g"'
lineBench = {"scala-agg-by-key": {"line": ""},
             "scala-agg-by-key-int": {"line": ""},
             "scala-agg-by-key-naive": {"line": ""},
             "scala-sort-by-key": {"line": ""},
             "scala-sort-by-key-int": {"line": ""},
             "scala-count": {"line": ""},
             "scala-count-w-fltr": {"line": ""}}
BENCHMARK = [lineBench.keys()]

# Terminate istance after benchmark
TERMINATE = 0


