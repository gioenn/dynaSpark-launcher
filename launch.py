import copy
import socket
import sys
import time
import datetime
from errno import ECONNREFUSED
from errno import ETIMEDOUT

import boto3

import run
from config import *


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def ping(host, port):
    try:
        socket.socket().connect((host, port))
        print(str(port) + " Open")
        return port
    except socket.error as err:
        if err.errno == ECONNREFUSED or err.errno == ETIMEDOUT:
            return False
        raise


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


def wait_ping(conn, instance_ids, pending_instance_ids):
    results = conn.describe_instances(InstanceIds=pending_instance_ids)
    for result in results["Reservations"]:
        for instace in result["Instances"]:
            if ping(instace["PublicDnsName"], 22) == 22:
                pending_instance_ids.pop(pending_instance_ids.index(instace["InstanceId"]))
                print("instance `{}` ping ok!".format(instace["InstanceId"]))
            else:
                print("pinging on `{}`".format(instace["InstanceId"]))

    if len(pending_instance_ids) == 0:
        print("all instances running!")
    else:
        time.sleep(2)
        wait_ping(conn, instance_ids, pending_instance_ids)


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

if NUMINSTANCE > 0:
    choice = query_yes_no("Are you sure to launch " + str(NUMINSTANCE) + " new instance?", "no")
    if choice:
        # Check Price SPOT INSTANCE
        response = client.describe_spot_price_history(InstanceTypes=[INSTANCE_TYPE], ProductDescriptions=['Linux/UNIX'],
                                                  AvailabilityZone=dataAMI[REGION]["az"])
        print(response['SpotPriceHistory'][0])
        lastspotprice = [float(x['SpotPrice']) for x in response['SpotPriceHistory'][:10]]
        print(lastspotprice)
        print("Number of responses:", len(response['SpotPriceHistory']))
        spotprice = float(sum(lastspotprice)) / max(len(lastspotprice), 1)
        spotprice += (spotprice * 0.2)
        spotprice = float("{0:.2f}".format(spotprice))
        print("LAST 10 SPOT PRICE + 20%: " + str(spotprice))
        print("YOUR PRICE: " + str(PRICE))
        if float(PRICE) < spotprice:
            print("ERROR PRICE")
            exit(1)

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
                                                     'Placement': {
                                                         'AvailabilityZone': dataAMI[REGION]["az"],
                                                     },
                                                     "InstanceType": INSTANCE_TYPE,
                                                     "EbsOptimized": EBS_OPTIMIZED,
                                                     "BlockDeviceMappings": [
                                                         {
                                                             "DeviceName": "/dev/sda1",
                                                             "Ebs": {
                                                                 "DeleteOnTermination": True,
                                                                 "VolumeType": "gp2",
                                                                 "VolumeSize": 200,
                                                                 "SnapshotId": dataAMI[REGION]["snapid"]
                                                             }
                                                         },
                                                         {
                                                             "DeviceName": "/dev/sdb",
                                                             "VirtualName": "ephemeral0"
                                                         }
                                                     ],
                                                 })

        print([req["SpotInstanceRequestId"] for req in requests["SpotInstanceRequests"]])

        request_ids = [req["SpotInstanceRequestId"] for req in requests["SpotInstanceRequests"]]

        print("CHECK SECURITY GROUP ALLOWED IP SETTINGS!!!")

        # Wait for our spots to fulfill
        wait_for_fulfillment(client, request_ids, copy.deepcopy(request_ids))

        results = client.describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
        instance_ids = [result["InstanceId"] for result in results["SpotInstanceRequests"]]

        client.create_tags(Resources=instance_ids, Tags=TAG)

        # Wait Running
        wait_for_running(client, instance_ids, copy.deepcopy(instance_ids))

        time.sleep(15)

        wait_ping(client, instance_ids, copy.deepcopy(instance_ids))

if REBOOT:
    print("Rebooting instances...")
    ec2 = boto3.resource('ec2', region_name=REGION)
    instances = ec2.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                 {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}
                 ])
    instance_ids = [x.id for x in instances]
    client.reboot_instances(InstanceIds=instance_ids)
    wait_ping(client, instance_ids, copy.deepcopy(instance_ids))

if RUN:
    run.runbenchmark()

if TERMINATE:
    # TODO: Fix TERMINATE handling
    ec2 = boto3.resource('ec2', region_name=REGION)
    if request_ids == None:
        # TODO
        print("add missing requests id to terminate")
    if instance_ids == None:
        instances = ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

    # DISTRUGGERE SPOT REQUEST
    client.cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)

    # TERMINARE INSTANCE
    ec2.instances.filter(InstanceIds=instance_ids).stop()
    ec2.instances.filter(InstanceIds=instance_ids).terminate()
