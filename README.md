# Spark Control Benchmark

Spark Control Benchmark is a benchmarking suite specific for testing the perfomance of the planning controller in [cSpark](https://github.com/ElfoLiNk/spark).

The tool is composed by five principal component: **launch.py**, **run.py**, **log.py**, **plot.py**, **metrics.py** in addition to the configuration file **config.py**. The **launch.py** module manages the startup of the instances on *Amazon EC2* via spot request and waits the instance creation linked to the spot request and its full launch. Subsequently the **run.py** module receives as input the instances on which to configure the cluster (*HDFS* or *Spark*) and configure the benchmarks to be executed and waits for the end of the benchmark. The module **log.py** download and save the log of the benchmark performed by such. The **plot.py** and **metrics.py** modules respectively generate graphs and calculate metrics.

The documentation of the API can be found [here](https://elfolink.github.io/benchmark-sparkcontrol/).

## Download & Requirements

```bash
git clone https://github.com/ElfoLiNk/benchmark-sparkcontrol.git
cd benchmark-sparkcontrol
pip instal -r requirements.txt
```

## AWS Credentials
Open the credential file of Amazon AWS

```bash
nano ~/.aws/credentials
```

And add the credential for cspark

```bash
[cspark]
aws_access_key_id=< KEY-ID >
aws_secret_access_key=< ACCESS-KEY >
```

## Configuration
See [config.py](https://elfolink.github.io/benchmark-sparkcontrol/config.html) 

## Example: Test PageRank
After added the AWS credential to create a cluster with open the file config.py and change this setting:
```python
DATA_AMI = # The name of the KeyPair for the instance
KEY_PAIR_PATH = # The local path of the KeyPair 
NUM_INSTANCE = 7 # 1 NameNode + 6 DataNode
CLUSTER_ID = "HDFS" # We first create an HDFS cluster
```
After editing the config.py, launch the file main.py. After setup and launch of the HDFS cluster copy the HDFS Master dns address.

Now to launch PageRank on a new cSpark cluster setup the config.py file as follows:
```python
DATA_AMI = # The name of the KeyPair for the instance
KEY_PAIR_PATH = # The local path of the KeyPair 
NUM_INSTANCE = 7 # 1 MasterNode + 6 WorkerNode
CLUSTER_ID = "CSPARK" # We first create an HDFS cluster
HDFS_MASTER = # The HDFS master dns address
PREV_SCALE_FACTOR = 0 # This is needed for generate new data
BENCHMARK_BENCH = ["PageRank"]
```


### TODO
- [ ] Add commandline parameters in main.py to override config.py
- [ ] Add support for Azure
