import json
import pickle
import run
from configure import config_instance as c
import log
from spark_log_profiling import processing, average_runs
from spark_log_profiling.average_runs import OUTPUT_DIR
import metrics
import plot
import os
import shutil
#from run import REGION, CONFIG_DICT, MAX_EXECUTOR
#from config import PRIVATE_KEY_PATH, PRIVATE_KEY_NAME, PROVIDER, SPARK_HOME, \
#                      AWS_ACCESS_ID, AWS_SECRET_KEY,\
#                      AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID

#from credentials import AWS_ACCESS_ID, AWS_SECRET_KEY,\
#    AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID

from libcloud.compute.providers import get_driver
from drivers.ccglibcloud.ec2spot import set_spot_drivers
from drivers.azurearm.driver import set_azurearm_driver
from util.utils import get_cfg, write_cfg, open_cfg
#import config as c
import pprint
pp = pprint.PrettyPrinter(indent=4)
#from configure import config_instance
import libcloud.common.base

libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True


try:
    print('in plot')
    plot.plot("home/ubuntu/dagsymb/num/app-20181206114842-0003")
except Exception as e:
    print("Plot failed: ", e)
try:    
    print('in metrics')
    metrics.compute_metrics("home/ubuntu/dagsymb/num/app-20181206114842-0003")
except Exception as e:
    print("Metrics failed: ", e)