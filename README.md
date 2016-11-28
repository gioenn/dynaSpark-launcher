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


## Parameters Main.py

```python
# TODO
```
