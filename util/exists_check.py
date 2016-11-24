import boto3
from boto.manage.cmdshell import sshclient_from_instance

from csparkbench.config import *

ec2 = boto3.resource('ec2', region_name=REGION)
instances = ec2.instances.filter(
    Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
             {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}
             ])

print("Instance Found: " + str(len(list(instances))))
if len(list(instances)) == 0:
    print("No instances running")
    exit(1)

master_instance = list(instances)[0]

ssh_client = sshclient_from_instance(master_instance, KEYPAIR_PATH, user_name='ubuntu')
path = DATA_AMI[REGION]["keypair"] + ".pem"
status = ssh_client.run('[ ! -e %s ]; echo $?' % path)
print(status)
