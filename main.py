import copy
import time

import launch
import run
from config import NUM_INSTANCE, REGION, TAG, REBOOT, CLUSTER_ID, TERMINATE, RUN, NUM_RUN, CONFIG_DICT, \
    PROVIDER
from credentials import AWS_ACCESS_ID, AWS_SECRET_KEY,\
    AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID

from libcloud.compute.providers import get_driver
from drivers.ccglibcloud.ec2spot import set_spot_drivers
from drivers.azurearm.driver import set_azurearm_driver

import libcloud.common.base

libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True


def main():
    """ Main function;
    * Launch spot request of NUMINSTANCE
    * Run Benchmark
    * Download Log
    * Plot data from log
    """

    if PROVIDER == "AWS_SPOT":
        set_spot_drivers()
        cls = get_driver("ec2_spot_" + REGION.replace('-', '_'))
        driver = cls(AWS_ACCESS_ID, AWS_SECRET_KEY)
    elif PROVIDER == "AZURE":
        set_azurearm_driver()
        cls = get_driver("CustomAzureArm")
        driver = cls(tenant_id=AZ_TENANT_ID,
                     subscription_id=AZ_SUBSCRIPTION_ID,
                     key=AZ_APPLICATION_ID, secret=AZ_SECRET, region=CONFIG_DICT["Azure"]["Location"])

    else:
        print("Unsupported provider", PROVIDER)
        return

    if NUM_INSTANCE > 0:

        # Create nodes
        if PROVIDER == "AWS_SPOT":
            nodes, spot_requests = launch.launch_libcloud(driver, NUM_INSTANCE, CONFIG_DICT)

        if PROVIDER == "AZURE":
            nodes = launch.launch_libcloud(driver, NUM_INSTANCE, CONFIG_DICT)

        # nodes is a list of "libcloud.compute.base.Node"

        print("CHECK SECURITY GROUP ALLOWED IP SETTINGS!!!")

        # Tag nodes
        if PROVIDER == "AWS_SPOT":
            for node in nodes:
                driver.ex_create_tags(node, TAG[0])
        elif PROVIDER == "AZURE":
            for node in nodes:
                driver.ex_create_tags(node, {"ClusterId": CONFIG_DICT["Azure"]["ClusterId"]})

        instance_ids = [n.id for n in nodes]

        # Wait for all the nodes to become RUNNNING
        print("Waiting for nodes to run")
        launch.wait_for_running_libcloud(driver, instance_ids, copy.deepcopy(instance_ids))

        time.sleep(15)

        # Wait for all the nodes to be pingable
        print("Waiting for nodes to be pingable")
        launch.wait_ping_libcloud(driver, instance_ids, copy.deepcopy(instance_ids))

    if REBOOT:
        print("Rebooting instances...")

        # Retrieve running nodes
        if PROVIDER == "AWS_SPOT":
            nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
            nodes = [n for n in nodes if driver.ex_describe_tags(node)['Value'] == CLUSTER_ID]
        elif PROVIDER == "AZURE":
            nodes = driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])
            nodes = [n for n in nodes if n.extra["tags"]["ClusterId"] == CLUSTER_ID]

        # Reboot nodes
        for node in nodes:
            driver.reboot_node(node)

        # Wait for all the nodes to be pingable
        instance_ids = [n.id for n in nodes]
        launch.wait_ping_libcloud(driver, instance_ids, copy.deepcopy(instance_ids))

    if RUN:
        for i in range(NUM_RUN):
            if PROVIDER == "AWS_SPOT":
                nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
                nodes = [n for n in nodes if driver.ex_describe_tags(n)['Value'] == CLUSTER_ID]
            elif PROVIDER == "AZURE":
                nodes = driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])
                nodes = [n for n in nodes if n.extra["tags"]["ClusterId"] == CLUSTER_ID]

            # nodes is a list of "libcloud.compute.base.Node"
            print("Found {} nodes".format(len(nodes)))

            run.run_benchmark(nodes)


    if TERMINATE:
        print("Begin termination of instances and cleaning")

        # Cancel Spot Request
        if PROVIDER == "AWS_SPOT" and NUM_INSTANCE > 0:
            for s in spot_requests:
                driver.ex_cancel_spot_instance_request(s)
            print("Spot requests cancelled")

        ###################################################

        # Retrieve running nodes
        if PROVIDER == "AWS_SPOT":
            nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
            nodes = [n for n in nodes if driver.ex_describe_tags(n)['Value'] == CLUSTER_ID]
        elif PROVIDER == "AZURE":
            nodes = driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])
            nodes = [n for n in nodes if n.extra["tags"]["ClusterId"] == CLUSTER_ID]
        print("Found {} nodes".format(len(nodes)))

        # nodes is a list of "libcloud.compute.base.Node"

        # Destroy all nodes
        print("Destroying nodes")
        for node in nodes:
            driver.destroy_node(node)

        print("All nodes destroyed")


if __name__ == "__main__":
    main()
