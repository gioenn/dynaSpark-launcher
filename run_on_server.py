import pickle
import run
import log
import metrics
import plot
from config import UPDATE_SPARK_DOCKER, DELETE_HDFS, SPARK_HOME, KILL_JAVA, SYNC_TIME, \
    KEY_PAIR_PATH, \
    UPDATE_SPARK, \
    DISABLE_HT, ENABLE_EXTERNAL_SHUFFLE, OFF_HEAP, OFF_HEAP_BYTES, K, T_SAMPLE, TI, CORE_QUANTUM, \
    CORE_MIN, CPU_PERIOD, HDFS, \
    CORE_VM, UPDATE_SPARK_MASTER, DEADLINE, MAX_EXECUTOR, ALPHA, BETA, OVER_SCALE, LOCALITY_WAIT, \
    LOCALITY_WAIT_NODE, CPU_TASK, \
    LOCALITY_WAIT_PROCESS, LOCALITY_WAIT_RACK, INPUT_RECORD, NUM_TASK, BENCH_NUM_TRIALS, \
    SCALE_FACTOR, RAM_EXEC, \
    RAM_DRIVER, BENCHMARK_PERF, BENCH_LINES, HDFS_MASTER, DATA_AMI, REGION, HADOOP_CONF, \
    CONFIG_DICT, HADOOP_HOME,\
    SPARK_2_HOME, BENCHMARK_BENCH, BENCH_CONF, LOG_LEVEL, CORE_ALLOCATION,DEADLINE_ALLOCATION,\
    UPDATE_SPARK_BENCH, UPDATE_SPARK_PERF, NUM_INSTANCE, STAGE_ALLOCATION, HEURISTIC

from config import PRIVATE_KEY_PATH, PRIVATE_KEY_NAME, PROVIDER

from credentials import AWS_ACCESS_ID, AWS_SECRET_KEY,\
    AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID

from libcloud.compute.providers import get_driver
from drivers.ccglibcloud.ec2spot import set_spot_drivers
from drivers.azurearm.driver import set_azurearm_driver

import libcloud.common.base

libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True

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

if PROVIDER == "AWS_SPOT":
    nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
elif PROVIDER == "AZURE":
    nodes = driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])

with open('nodes_ids.pickle', 'rb') as f:
    nodes_ids = pickle.load(f)
    
with open('logfolder.pickle', 'rb') as f:
    logfolder = pickle.load(f)
    
with open('output_folder.pickle', 'rb') as f:
    output_folder = pickle.load(f)

with open('master_ip.pickle', 'rb') as f:
    master_ip = pickle.load(f)
    
nodes = [n for n in nodes if n.id in nodes_ids]
end_index = min(len(nodes), MAX_EXECUTOR + 1)
# DOWNLOAD LOGS
output_folder = log.download(logfolder, [i for i in nodes[:end_index]], master_ip,
                             output_folder, CONFIG_DICT)

run.write_config(output_folder)

# PLOT LOGS
plot.plot(output_folder + "/")

# COMPUTE METRICS
metrics.compute_metrics(output_folder + "/")

print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")
