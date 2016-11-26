"""
Handle the instance:
* Launch new instance with spot request
* Terminate instance
* Check instance connectivity
"""

import socket
import sys
import time
from errno import ECONNREFUSED
from errno import ETIMEDOUT


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    :param question: is a string that is presented to the user.
    :param default:  is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    :return: The "answer" return value is True for "yes" or False for "no".
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
    """
    Ping the port of the host

    :param host: the host to ping
    :param port: the port of the host to ping
    :return: the port if the port is open or False if there is a connection error
    """

    try:
        socket.socket().connect((host, port))
        print(str(port) + " Open")
        return port
    except socket.error as err:
        if err.errno == ECONNREFUSED or err.errno == ETIMEDOUT:
            return False
        raise


def wait_ping(client, instance_ids, pending_instance_ids):
    """Wait that all the instance have open the ssh port (22)

    :param client: the ec2 client
    :param instance_ids: id of all the instance to ping
    :param pending_instance_ids: id of the remaining instance to ping
    :return: Exit when all the instance are reachable on port 22
    """

    results = client.describe_instances(InstanceIds=pending_instance_ids)
    for result in results["Reservations"]:
        for instance in result["Instances"]:
            if ping(instance["PublicDnsName"], 22) == 22:
                pending_instance_ids.pop(pending_instance_ids.index(instance["InstanceId"]))
                print("instance `{}` ping ok!".format(instance["InstanceId"]))
            else:
                print("pinging on `{}`".format(instance["InstanceId"]))

    if len(pending_instance_ids) == 0:
        print("all instances running!")
    else:
        time.sleep(2)
        wait_ping(client, instance_ids, pending_instance_ids)


def wait_for_running(client, instance_ids, pending_instance_ids):
    """Wait that all the instance are in running state

    :param client: the ec2 client
    :param instance_ids: id of all the instance to check running
    :param pending_instance_ids: id of the remaining instance to check running
    :return: Exit when all the instance are running
    """

    results = client.describe_instances(InstanceIds=pending_instance_ids)
    for result in results["Reservations"]:
        for instance in result["Instances"]:
            if instance["State"]["Name"] == 'running':
                pending_instance_ids.pop(pending_instance_ids.index(instance["InstanceId"]))
                print("instance `{}` running!".format(instance["InstanceId"]))
            else:
                print("waiting on `{}`".format(instance["InstanceId"]))

    if len(pending_instance_ids) == 0:
        print("all instances running!")
    else:
        time.sleep(10)
        wait_for_running(client, instance_ids, pending_instance_ids)


def wait_for_fulfillment(client, request_ids, pending_request_ids):
    """Wait that all the spot instance request are fulfilled

    :param client: the ec2 client
    :param request_ids: id of all the spot instance request to fulfill
    :param pending_request_ids: id of the remaining spot instance request to fulfill
    :return: Exit when all the spot request are fulfilled
    """

    results = client.describe_spot_instance_requests(SpotInstanceRequestIds=pending_request_ids)
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
        wait_for_fulfillment(client, request_ids, pending_request_ids)


def terminate(client, spot_request_ids, instance_ids):
    """Delete all the spot_request_ids and terminate al the instance_ids

    :param client:  the ec2 client
    :param spot_request_ids: id of all the spot instance request to delete
    :param instance_ids: id of all the instance to terminate
    :return:
    """

    # DELETE SPOT REQUEST
    client.cancel_spot_instance_requests(SpotInstanceRequestIds=spot_request_ids)

    # TERMINATE INSTANCE
    client.instances.filter(InstanceIds=instance_ids).stop()
    client.instances.filter(InstanceIds=instance_ids).terminate()


def check_spot_price(client, config):
    """Check the current spot price on the selected amazon region of the instance type choosen
        and compare with the one provided by the user

    :param client: the ec2 client
    :param config: the configuration dictionary of the user
    :return: Exit if the spot price of the user is too low (< current price + 20%)
    """

    spot_price_history_response = client.describe_spot_price_history(
        InstanceTypes=[config["Aws"]["InstanceType"]],
        ProductDescriptions=['Linux/UNIX'],
        AvailabilityZone=config["Aws"]["AZ"])
    print(spot_price_history_response['SpotPriceHistory'][0])
    last_spot_price = [float(x['SpotPrice']) for x in
                       spot_price_history_response['SpotPriceHistory'][:10]]
    print(last_spot_price)
    print("Number of responses:", len(spot_price_history_response['SpotPriceHistory']))
    spot_price = float(sum(last_spot_price)) / max(len(last_spot_price), 1)
    spot_price += (spot_price * 0.2)
    spot_price = float("{0:.2f}".format(spot_price))
    print("LAST 10 SPOT PRICE + 20%: " + str(spot_price))
    print("YOUR PRICE: " + str(config["Aws"]["Price"]))
    if float(config["Aws"]["Price"]) < spot_price:
        print("ERROR PRICE")
        exit(1)


def launch(client, num_instance, config):
    """
    Launch num_instance on Amazon EC2 with spot request

    :param client: the ec2 client
    :param num_instance: number of instance to launch
    :param config: the configuration dictionary of the user
    :return: the list of spot request's ids
    """
    if query_yes_no("Are you sure to launch " + str(num_instance) + " new instance?", "no"):
        check_spot_price(client, config)
        spot_request_response = client.request_spot_instances(
            SpotPrice=config["Aws"]["Price"],
            InstanceCount=num_instance,
            Type='one-time',
            AvailabilityZoneGroup=
            config["Aws"]["AZ"],
            LaunchSpecification={
                "ImageId": config["Aws"]["AMI"],
                "KeyName": config["Aws"]["KeyPair"],
                "SecurityGroups": [
                    config["Aws"]["SecurityGroup"],
                ],
                'Placement': {
                    'AvailabilityZone': config["Aws"]["AZ"],
                },
                "InstanceType": config["Aws"]["InstanceType"],
                "EbsOptimized": config["Aws"]["EbsOptimized"],
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/sda1",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "VolumeType": "gp2",
                            "VolumeSize": 200,
                            "SnapshotId": config["Aws"]["SnapshotId"]
                        }
                    },
                    {
                        "DeviceName": "/dev/sdb",
                        "VirtualName": "ephemeral0"
                    }
                ],
            })

        print([req["SpotInstanceRequestId"] for req in
               spot_request_response["SpotInstanceRequests"]])

        return [req["SpotInstanceRequestId"] for req in
                spot_request_response["SpotInstanceRequests"]]
