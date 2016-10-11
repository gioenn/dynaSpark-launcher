import log
import boto3
from config import *
import plot

ec2 = boto3.resource('ec2', region_name=REGION)
instances = ec2.instances.filter(
    Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
             {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}
             ])

logfolder = "./spark-bench/num"
master_dns = "ec2-52-11-49-86.us-west-2.compute.amazonaws.com"
#master_dns = "ec2-54-70-77-95.us-west-2.compute.amazonaws.com"
output_folder="./spark-bench/num/"
output_folder = log.download(logfolder, instances, master_dns, output_folder)


# PLOT LOGS
plot.plot(output_folder)
