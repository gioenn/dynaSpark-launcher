import copy
import time

import boto3

import run
from config import *

ec2 = boto3.resource('ec2', region_name=REGION)


def between(value, a, b):
    # Find and validate before-part.
    pos_a = value.find(a)
    if pos_a == -1: return ""
    # Find and validate after part.
    pos_b = value.find(b)
    if pos_b == -1: return ""
    # Return middle part.
    adjusted_pos_a = pos_a + len(a)
    if adjusted_pos_a >= pos_b: return ""
    return value[adjusted_pos_a:pos_b]


def wait_for_running(conn, instance_ids, pending_instance_ids):
    results = conn.describe_instances(InstanceIds=pending_instance_ids)
    for result in results["Reservations"]:
        for instace in result["Instances"]:
            if instace["State"]["Name"] == 'running':
                pending_instance_ids.pop(pending_instance_ids.index(instace["InstanceId"]))
                print("instance `{}` running!".format(instace["InstanceId"]))
            else:
                print("waiting on `{}`".format(instace["InstanceId"]))

    if len(pending_instance_ids) == 0:
        print("all instances running!")
    else:
        time.sleep(10)
        wait_for_running(conn, instance_ids, pending_instance_ids)


def wait_for_fulfillment(conn, request_ids, pending_request_ids):
    results = conn.describe_spot_instance_requests(SpotInstanceRequestIds=pending_request_ids)
    for result in results["SpotInstanceRequests"]:
        if result["Status"]["Code"] == 'fulfilled':
            pending_request_ids.pop(pending_request_ids.index(result["SpotInstanceRequestId"]))
            print("spot request `{}` fulfilled!".format(result["SpotInstanceRequestId"]))
        else:
            print("waiting on `{}`".format(result["SpotInstanceRequestId"]))

    if len(pending_request_ids) == 0:
        print("all spots fulfilled!")
    else:
        time.sleep(10)
        wait_for_fulfillment(conn, request_ids, pending_request_ids)


client = boto3.client('ec2', region_name=REGION)

requests = client.request_spot_instances(SpotPrice=PRICE,
                                         InstanceCount=NUMINSTANCE,
                                         Type='one-time',
                                         AvailabilityZoneGroup=dataAMI[REGION]["az"],
                                         LaunchSpecification={
                                             "ImageId": dataAMI[REGION]["ami"],
                                             "KeyName": dataAMI[REGION]["keypair"],
                                             "SecurityGroups": [
                                                 SECURITY_GROUP,
                                             ],
                                             "InstanceType": INSTANCE_TYPE,
                                             "EbsOptimized": True
                                         })

print([req["SpotInstanceRequestId"] for req in requests["SpotInstanceRequests"]])

request_ids = [req["SpotInstanceRequestId"] for req in requests["SpotInstanceRequests"]]

# Wait for our spots to fulfill
wait_for_fulfillment(client, request_ids, copy.deepcopy(request_ids))

results = client.describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
instance_ids = [result["InstanceId"] for result in results["SpotInstanceRequests"]]

# Wait Running
wait_for_running(client, instance_ids, copy.deepcopy(instance_ids))

time.sleep(15)

run.runbenchmark()

if TERMINATE:
    # DISTRUGGERE SPOT REQUEST
    client.cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)

    # TERMINARE INSTANCE
    ec2.instances.filter(InstanceIds=instance_ids).stop()
    ec2.instances.filter(InstanceIds=instance_ids).terminate()
