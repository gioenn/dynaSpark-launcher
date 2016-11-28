# Spark Control Benchmark

Spark Control Benchmark is a benchmarking suite specific for testing the perfomance of the planning controller in [cSpark](https://github.com/ElfoLiNk/spark).

The tool is composed by five principal component: **launch.py**, **run.py**, **log.py**, **plot.py**, **metrics.py** in addition to the configuration file **config.py**. The **launch.py** module manages the startup of the instances on *Amazon EC2* via spot request and waits the instance creation linked to the spot request and its full launch. Subsequently the **run.py** module receives as input the instances on which to configure the cluster (*HDFS* or *Spark*) and configure the benchmarks to be executed and waits for the end of the benchmark. The module **log.py** download and save the log of the benchmark performed by such. The **plot.py** and **metrics.py** modules respectively generate graphs and calculate metrics.

The documentation of the API can be found [here](https://elfolink.github.io/benchmark-sparkcontrol/).

## Requirements

```bash
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


## Example Main.py

```python
import copy
import time
import boto3
import launch
import run
from config import NUM_INSTANCE, REGION, TAG, REBOOT, CLUSTER_ID, TERMINATE, RUN, NUM_RUN, \
    CREDENTIAL_PROFILE, CONFIG_DICT


def main():
    """ Main function;
    * Launch spot request of NUMINSTANCE and TAG it
    * Run Benchmark:
      * Download Log
      * Plot data from log
    """
    session = boto3.Session(profile_name=CREDENTIAL_PROFILE)
    client = session.client('ec2', region_name=REGION)

    if NUM_INSTANCE > 0:
        # Launch Instances
        spot_request_ids = launch.launch(client, NUM_INSTANCE, CONFIG_DICT)

        print("CHECK SECURITY GROUP ALLOWED IP SETTINGS!!!")

        # Wait for our spots to fulfill
        launch.wait_for_fulfillment(client, spot_request_ids, copy.deepcopy(spot_request_ids))

        spot_instance_response = client.describe_spot_instance_requests(
            SpotInstanceRequestIds=spot_request_ids)
        instance_ids = [result["InstanceId"] for result in
                        spot_instance_response["SpotInstanceRequests"]]

        # Tag instance
        client.create_tags(Resources=instance_ids, Tags=TAG)

        # Wait Running
        launch.wait_for_running(client, instance_ids, copy.deepcopy(instance_ids))

        time.sleep(5)
        
        # Ping Instance
        launch.wait_ping(client, instance_ids, copy.deepcopy(instance_ids))

    if REBOOT:
        print("Rebooting instances...")
        instances = client.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                     {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}])
        instance_ids = [x.id for x in instances]
        client.reboot_instances(InstanceIds=instance_ids)
        launch.wait_ping(client, instance_ids, copy.deepcopy(instance_ids))

    if RUN:
        for i in range(NUM_RUN):
            run.run_benchmark()

    if TERMINATE:
        instances = client.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                     {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}])
        instance_ids = [x.id for x in instances]
        # TODO get spot_request_ids
        launch.terminate(client, spot_request_ids, instance_ids)


if __name__ == "__main__":
    main()
```
