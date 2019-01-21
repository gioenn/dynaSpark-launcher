import copy

import launch
#from config import CONFIG_DICT
from configure import config_instance as c
#from credentials import AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID

from libcloud.compute.providers import get_driver
from drivers.ccglibcloud.ec2spot import set_spot_drivers
from drivers.azurearm.driver import set_azurearm_driver

import libcloud.common.base

libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True
set_azurearm_driver()
cls = get_driver("CustomAzureArm")
driver = cls(tenant_id=c.AZ_TENANT_ID,
             subscription_id=c.AZ_SUBSCRIPTION_ID,
             key=c.AZ_APPLICATION_ID, secret=c.AZ_SECRET, region=c.CONFIG_DICT["Azure"]["Location"])

start = True;
all = True;
# tag = "CSPARKWORK"
tag = "CSPARKHDFS"

nodes = driver.list_nodes(ex_resource_group=c.CONFIG_DICT["Azure"]["ResourceGroup"])
if not all:
    nodes = [n for n in nodes if n.extra["tags"]["ClusterId"] == tag]

print("start = {}, all = {}, nodes = {}".format(str(start), str(all), str(len(nodes))))

if start:
    for node in nodes:
        driver.ex_start_node(node)
    instance_ids = [n.id for n in nodes]
    launch.wait_for_running_libcloud(driver, instance_ids, copy.deepcopy(instance_ids))
else:
    for node in nodes:
        driver.ex_stop_node(node)
