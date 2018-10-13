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

from libcloud.compute.base import NodeImage, NodeAuthSSHKey
from libcloud.compute.types import NodeState

from drivers.azurearm.driver import AzureVhdImage
from drivers.ccglibcloud.ec2spot import SpotRequestState

import copy

#from config import PROVIDER, CONFIG_DICT, CLUSTER_ID
from configure import config_instance as c

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


def wait_ping_libcloud(driver, instance_ids, pending_instance_ids):
    """Wait that all the instances have open the ssh port (22)

    :param driver: libcloud driver
    :param instance_ids: id of the nodes associated to the driver
    :param pending_instance_ids: id of remaining nodes to ping
    :return: Exit when all nodes are reachable on port 22
    """
    if (c.PROVIDER == "AWS_SPOT"):
        nodes = driver.list_nodes(ex_node_ids=pending_instance_ids)
    elif (c.PROVIDER == "AZURE"):
        nodes = driver.list_nodes(ex_resource_group=c.CONFIG_DICT["Azure"]["ResourceGroup"])
        nodes = [n for n in nodes if n.id in pending_instance_ids]
    for node in nodes:
        if ping(node.public_ips[0], 22) == 22:
            pending_instance_ids.pop(pending_instance_ids.index(node.id))
            print("node {} ping ok!".format(node.id))
        else:
            print("pinging {}".format(node.id))
    if len(pending_instance_ids) == 0:
        print("all instances running!")
    else:
        time.sleep(2)
        wait_ping_libcloud(driver, instance_ids, pending_instance_ids)


def wait_for_running_libcloud(driver, instance_ids, pending_instance_ids):
    """ Wait for all the node insances to be in running state

    :param driver: the libcloud driver
    :param instance_ids: the node ids
    :param pending_instance_ids: the remaining node ids to check
    :return: when all nodes are running
    """
    if (c.PROVIDER == "AWS_SPOT"):
        nodes = driver.list_nodes(ex_node_ids=pending_instance_ids)
    elif (c.PROVIDER == "AZURE"):
        nodes = driver.list_nodes(ex_resource_group=c.CONFIG_DICT["Azure"]["ResourceGroup"])
        nodes = [n for n in nodes if n.id in pending_instance_ids]
    for node in nodes:
        if node.state == NodeState.RUNNING:
            pending_instance_ids.pop(pending_instance_ids.index(node.id))
            print("node {} running!".format(node.id))
        else:
            if node.state == NodeState.ERROR:
                print("error detected while launching node {}!\naborting...".format(node.id))
                return False
            print("waiting on {}".format(node.id))
    if len(pending_instance_ids) == 0:
        print("all nodes running")
        return True
    else:
        time.sleep(10)
        return wait_for_running_libcloud(driver, instance_ids, pending_instance_ids)


def wait_for_fulfillment_libcloud(driver, request_ids, pending_request_ids):
    """Wait that all the spot requests are fullfilled

    :param driver: libcloud ec2 spot driver
    :param request_ids: ec2 spot request id to fullfill
    :param pending_request_ids: remaining ec2 spot request id to fullfill
    :return: Exit when all ec2 spot request are fullfilled (ACTIVE state)
    """
    results = driver.ex_list_spot_requests(spot_request_ids=pending_request_ids)
    for result in results:
        if result.state == SpotRequestState.ACTIVE:
            pending_request_ids.pop(pending_request_ids.index(result.id))
            print("spot request `{}` fulfilled!".format(result.id))
        else:
            print("waiting on `{}`".format(result.id))
    if len(pending_request_ids) == 0:
        print("all spots fulfilled!")
    else:
        time.sleep(10)
        wait_for_fulfillment_libcloud(driver, request_ids, pending_request_ids)


def check_spot_price(driver, config):
    """Check the current spot price on the selected amazon region of the instance type choosen
        and compare with the one provided by the user

    :param client: the libcloud ec2 spot driver
    :param config: the configuration dictionary of the user
    :return: Exit if the spot price of the user is too low (< current price + 20%)
    """

    # Obtain an array of EC2SpotPriceElement
    spot_price_history = driver.ex_describe_spot_price_history({
        "instance-type": [config["Aws"]["InstanceType"]],
        "product-description": ['Linux/UNIX'],
        "availability-zone": config["Aws"]["AZ"],
    })

    print(spot_price_history)

    last_spot_price = [float(x.spot_price) for x in spot_price_history[:10]]
    print(last_spot_price)

    print("Number of responses:", len(spot_price_history))

    # Calculate a possible spot price
    spot_price = float(sum(last_spot_price)) / max(len(last_spot_price), 1)
    spot_price += spot_price * 0.2

    spot_price = float("{0:.2f}".format(spot_price))

    print("LAST 10 SPOT PRICE + 20%: " + str(spot_price))
    print("YOUR PRICE:" + str(config["Aws"]["Price"]))

    if float(config["Aws"]["Price"]) < spot_price:
        print("ERROR PRICE")
        exit(1)


def launch_libcloud(driver, num_instance, config, cluster_id=c.CLUSTER_ID, assume_yes=False):
    """Launch num_instance instances on the desired provider given by the driver, using a provider depended config

    :param driver: the desired provider driver
    :param num_instance: the number of instances to be launched
    :param config: the configuration dictionary of the user
    :return: list of the created nodes, if provider AWS_SPOT also list of spot request is returned
    """
    proceed = True if assume_yes else query_yes_no("Are you sure to launch " + str(num_instance) + " new instances on " + cluster_id + "?", "no")
    if proceed:
        if (c.PROVIDER == "AWS_SPOT"):
            check_spot_price(driver, config)

            # pick size and images

            sizes = driver.list_sizes()
            size = [s for s in sizes if s.id == config["Aws"]["InstanceType"]][0]
            image = NodeImage(id=config["Aws"]["AMI"], name=None, driver=driver)

            locations = driver.list_locations()
            location = [l for l in locations if l.name == config["Aws"]["AZ"]][0]

            # Array of EC2SpotRequest
            spot_request = driver.ex_request_spot_instances(image=image,
                                                            size=size,
                                                            spot_price=config["Aws"]["Price"],
                                                            instance_count=num_instance,
                                                            type='one-time',
                                                            location=location,
                                                            keyname=config["Aws"]["KeyPair"],
                                                            security_groups=[config["Aws"]["SecurityGroup"]],
                                                           #ebs_optimized=config["Aws"]["EbsOptimized"],
                                                            blockdevicemappings=[
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
                                                            ]
                                                            )

            print(spot_request)

            # Request created

            spot_request_ids = [s.id for s in spot_request]

            print(spot_request_ids)

            wait_for_fulfillment_libcloud(driver, spot_request_ids, copy.deepcopy(spot_request_ids))

            # Spot Instances ACTIVE
            spot_request_updates = driver.ex_list_spot_requests(spot_request_ids)
            instance_ids = [s.instance_id for s in spot_request_updates]

            nodes = driver.list_nodes(ex_node_ids=instance_ids)

            return nodes, spot_request_updates

        if c.PROVIDER == "AZURE":
            # obtain size
            print("Collecting node size")
            sizes = driver.list_sizes()
            size = [s for s in sizes if s.id == config["Azure"]["NodeSize"]][0]

            # obtain image
            print("Collecting node image")
            # image = driver.get_image('Canonical:UbuntuServer:14.04.5-LTS:14.04.201703230')
            image = AzureVhdImage(storage_account=config["Azure"]["NodeImage"]["StorageAccount"],
                                  blob_container=config["Azure"]["NodeImage"]["BlobContainer"],
                                  name=config["Azure"]["NodeImage"]["Name"],
                                  driver=driver)

            # create resource group
            print("Creating resource group")
            driver.ex_create_resource_group(resource_group=config["Azure"]["ResourceGroup"])

            # create storage account
            print("Creating storage account")
            driver.ex_create_storage_account(resource_group=config["Azure"]["ResourceGroup"],
                                             storage_account=config["Azure"]["StorageAccount"]["Name"],
                                             sku=config["Azure"]["StorageAccount"]["Sku"],
                                             kind=config["Azure"]["StorageAccount"]["Kind"])

            # create security group
            print("Creating security group")
            security_rules = [
                {
                    "name": "default-allow-ssh",
                    "properties": {
                        "protocol": "TCP",
                        "sourcePortRange": "*",
                        "destinationPortRange": "22",
                        "sourceAddressPrefix": "*",
                        "destinationAddressPrefix": "*",
                        "access": "allow",
                        "priority": 1000,
                        "direction": "Inbound"
                    }
                },
                {
                    "name": "hadoop-9000",
                    "properties": {
                        "protocol": "*",
                        "sourcePortRange": "*",
                        "destinationPortRange": "9000",
                        "sourceAddressPrefix": "*",
                        "destinationAddressPrefix": "*",
                        "access": "allow",
                        "priority": 1010,
                        "direction": "Inbound"
                    }
                },
                {
                    "name": "hadoop-50090",
                    "properties": {
                        "protocol": "*",
                        "sourcePortRange": "*",
                        "destinationPortRange": "50090",
                        "sourceAddressPrefix": "*",
                        "destinationAddressPrefix": "*",
                        "access": "allow",
                        "priority": 1020,
                        "direction": "Inbound"
                    }
                },
                {
                    "name": "hadoop-50070",
                    "properties": {
                        "protocol": "*",
                        "sourcePortRange": "*",
                        "destinationPortRange": "50070",
                        "sourceAddressPrefix": "*",
                        "destinationAddressPrefix": "*",
                        "access": "allow",
                        "priority": 1030,
                        "direction": "Inbound"
                    }
                },
                {
                    "name": "hadoop-50010",
                    "properties": {
                        "protocol": "*",
                        "sourcePortRange": "*",
                        "destinationPortRange": "50010",
                        "sourceAddressPrefix": "*",
                        "destinationAddressPrefix": "*",
                        "access": "allow",
                        "priority": 1040,
                        "direction": "Inbound"
                    }
                },
                {
                    "name": "spark-7077",
                    "properties": {
                        "protocol": "*",
                        "sourcePortRange": "*",
                        "destinationPortRange": "7077",
                        "sourceAddressPrefix": "*",
                        "destinationAddressPrefix": "*",
                        "access": "allow",
                        "priority": 1050,
                        "direction": "Inbound"
                    }
                },
                {
                    "name": "spark-webui-4040",
                    "properties": {
                        "protocol": "*",
                        "sourcePortRange": "*",
                        "destinationPortRange": "4040",
                        "sourceAddressPrefix": "*",
                        "destinationAddressPrefix": "*",
                        "access": "allow",
                        "priority": 1060,
                        "direction": "Inbound"
                    }
                }
            ]
            driver.ex_create_security_group(resource_group=config["Azure"]["ResourceGroup"],
                                            security_group=config["Azure"]["SecurityGroup"],
                                            security_rules=security_rules)

            # create network and subnet
            print("Create network")
            network_parameters = {
                "addressSpace": {
                    "addressPrefixes": ["10.0.0.0/16"]
                },
                "subnets": [
                    {
                        "name": config["Azure"]["Subnet"],
                        "properties": {
                            "addressPrefix": "10.0.0.0/24"
                        }
                    }
                ]
            }
            network = driver.ex_create_network(resource_group=config["Azure"]["ResourceGroup"],
                                               network=config["Azure"]["Network"],
                                               extra=network_parameters)

            # retrieve subnet
            print("Find default subnet")
            subnets = driver.ex_list_subnets(network)
            subnet = [s for s in subnets if s.name == "default"][0]

            # public ips
            print("Create public ips")
            public_ips = [driver.ex_create_public_ip(name="{}ip{}".format(cluster_id, i),
                                                    #name="testip",
                                                     resource_group=config["Azure"]["ResourceGroup"]) for i in
                          range(num_instance)]

            # network interface
            print("Create network interfaces")
            network_interfaces = [
                driver.ex_create_network_interface(name="{}nic{}".format(cluster_id, i),
                                                   #name="testnic",
                                                   subnet=subnet,
                                                   resource_group=config["Azure"]["ResourceGroup"],
                                                   public_ip=public_ips[i]
                                                   )
                for i in range(num_instance)]

            # auth
            print("Load public SSH key")
            with open(config["Azure"]["PubKeyPath"], 'r') as pubkey:
                pubdata = pubkey.read()
            auth = NodeAuthSSHKey(pubdata)



            # create nodes
            print("Beginning node creation")
            nodes = [driver.create_node(name="{}node{}".format(cluster_id, i),
                                        #name="vm",
                                        size=size,
                                        image=image,
                                        auth=auth,
                                        ex_resource_group=config["Azure"]["ResourceGroup"],
                                        ex_storage_account=config["Azure"]["StorageAccount"]["Name"],
                                        ex_blob_container='vhds',
                                        ex_user_name='ubuntu',
                                        ex_network=None,
                                        ex_subnet=None,
                                        ex_nic=network_interfaces[i],
                                        ex_customdata='')
                     for i in range(num_instance)]
            print("Created {} nodes".format(len(nodes)))

            return nodes
