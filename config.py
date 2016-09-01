# AWS
dataAMI = {"eu-west-1": {"ami": 'ami-a03d4fd3', "az": 'eu-west-1c', "keypair": "gazzettaEU"},
           "us-west-2": {"ami": 'ami-52598f32', "az": 'us-west-2c', "keypair": "gazzetta"}}

REGION = "eu-west-1"
KEYPAIR_PATH = "C:\\Users\\Matteo\\Downloads\\" + dataAMI[REGION]["keypair"] + ".pem"
SECURITY_GROUP = "spark-cluster"

PRICE = "0.3"
INSTANCE_TYPE = "m4.4xlarge"
NUMINSTANCE = 5

# Core
COREVM = 8
COREHTVM = 16
DISABLEHT = 0
if DISABLEHT:
    COREHTVM = COREVM

# CONTROL
TSAMPLE = 3000
DEADLINE = 70000
MAXEXECUTOR = 4
ALPHA = 0.8
OVERSCALE = 2

# BENCHMARK
SCALE_FACTOR = 1
BENCHMARK = []
lineBench = {"nom": []}

# Terminate istance after benchmark
TERMINATE = 0


