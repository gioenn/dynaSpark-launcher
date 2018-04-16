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

if c.PROVIDER == "AWS_SPOT":
        set_spot_drivers()
        cls = get_driver("ec2_spot_" + c.REGION.replace('-', '_'))
        driver = cls(c.AWS_ACCESS_ID, c.AWS_SECRET_KEY)
elif c.PROVIDER == "AZURE":
    set_azurearm_driver()
    cls = get_driver("CustomAzureArm")
    driver = cls(tenant_id=c.AZ_TENANT_ID,
                 subscription_id=c.AZ_SUBSCRIPTION_ID,
                 key=c.AZ_APPLICATION_ID, secret=c.AZ_SECRET, region=c.CONFIG_DICT["Azure"]["Location"])

else:
    print("Unsupported provider", c.PROVIDER)

if c.PROVIDER == "AWS_SPOT":
    nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
elif c.PROVIDER == "AZURE":
    nodes = driver.list_nodes(ex_resource_group=c.CONFIG_DICT["Azure"]["ResourceGroup"])

with open('nodes_ids.pickle', 'rb') as f:
    nodes_ids = pickle.load(f)
    
with open('logfolder.pickle', 'rb') as f:
    logfolder = pickle.load(f)
    
with open('output_folder.pickle', 'rb') as f:
    output_folder = pickle.load(f)

with open('master_ip.pickle', 'rb') as f:
    master_ip = pickle.load(f)

'''
with open('config_instance.pickle', 'rb') as f:
    config_instance = pickle.load(f)
    print("pickle_loaded config_instance.cfg_dict: ")
    pp.pprint(config_instance.cfg_dict)
'''
#config_instance.cfg_dict = json.load(open("cfg_dict.json"))
cfg_dict = json.load(open("cfg_dict.json"))
#print("loaded from client: cfg_dict.json: ")
#pp.pprint(cfg_dict)
c.update_ext_parms(cfg_dict) 
nodes = [n for n in nodes if n.id in nodes_ids]
end_index = min(len(nodes), c.MAX_EXECUTOR + 1)
'''
end_index = len(nodes)
c.NUM_RUN = cfg_dict["NumRun"]
c.HDFS_MASTER = cfg_dict["HdfsMaster"]
c.DEADLINE = cfg_dict["Deadline"]
c.MAX_EXECUTOR = cfg_dict[""]
c.RUN = cfg_dict["Run"]
c.PREV_SCALE_FACTOR = cfg_dict["PrevScaleFactor"]
c.BENCHMARK_PERF = cfg_dict["BenchmarkPerf"]
c.BENCHMARK_BENCH = cfg_dict["BenchmarkBench"]
c.BENCH_CONF = cfg_dict["BenchConf"]
c.HDFS = cfg_dict["Hdfs"]
c.DELETE_HDFS = cfg_dict["DeleteHdfs"]
c.CONFIG_DICT = cfg_dict["ConfigDict"]
c.SCALE_FACTOR = cfg_dict["ScaleFactor"]
c.INPUT_RECORD = cfg_dict["InputRecord"]
 = cfg_dict[""]
 = cfg_dict[""]
 = cfg_dict[""]

with open_cfg() as cfg:
    c.update_exp_parms(cfg)
    if 'main' in cfg and 'max_executors' in cfg['main']:
        c.MAX_EXECUTOR = int(cfg['main']['max_executors'])
        c.CONFIG_DICT["Control"]["MaxExecutor"] = c.MAX_EXECUTOR
        end_index = min(len(nodes), int(cfg['main']['max_executors']) + 1)
#end_index = min(len(nodes), c.MAX_EXECUTOR + 1)
print("MAX_EXECUTOR in process_on_server b4 local assignment: " + str(c.MAX_EXECUTOR))
c.cfg_dict["MaxExecutor"] = c.MAX_EXECUTOR = str(end_index - 1)
print("MAX_EXECUTOR in process_on_server after local assignment: " + str(c.MAX_EXECUTOR))
c.CONFIG_DICT["Control"]["MaxExecutor"] = c.MAX_EXECUTOR
c.cfg_dict["ConfigDict"] = c.CONFIG_DICT        
c.update_config_parms(c)
'''
#print("process_on_server config_instance.cfg_dict: ")
#pp.pprint(c.cfg_dict)
#print("passed to log.download: c.CONFIG_DICT: ")
#pp.pprint(c.CONFIG_DICT)
# DOWNLOAD LOGS
output_folder = log.download(logfolder, [i for i in nodes[:end_index]], master_ip, #vboxvm
                             output_folder, c.CONFIG_DICT)                           #vboxvm

with open_cfg() as cfg:
    profile = True if 'profile' in cfg else False
    profile_option = cfg.getboolean('main', 'profile') if 'main' in cfg and 'profile' in cfg['main'] else False
    if profile or profile_option:                                                                       # Profiling
        processing.main()                                                                                       # Profiling
        for filename in os.listdir('./spark_log_profiling/output_json/'):                                       # Profiling
            if output_folder.split("/")[-1].split("-")[-1] in filename:                                         # Profiling                                                             # Profilimg
                shutil.copy('./spark_log_profiling/output_json/' + filename, output_folder + "/" + filename)    # Profiling

run.write_config(output_folder)
print("Saving output folder {}".format(os.path.abspath(output_folder)))
with open_cfg(mode='w') as cfg:
    cfg['out_folders']['output_folder_'+str(len(cfg['out_folders']))] = os.path.abspath(output_folder)
    # Saving cfg on project home directory and output folder
    write_cfg(cfg)
    write_cfg(cfg, output_folder)

#simulate plot
for f in os.listdir('./dataplot/'):                                   # simulate plot
    shutil.copy('./dataplot/' + f, output_folder + "/" + f)    # simulate plot

    
# PLOT LOGS
plot.plot(output_folder + "/")

# COMPUTE METRICS
metrics.compute_metrics(output_folder + "/")

print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")

with open_cfg() as cfg:
    if profile and 'main' in cfg and 'iter_num' in cfg['main'] \
               and 'num_run' in cfg['main'] and 'benchmarkname' in cfg['experiment'] \
               and 'experiment_num' in cfg['main'] and 'num_experiments' in cfg['main'] \
               and int(cfg['main']['iter_num']) == int(cfg['main']['num_run']): #\
               #and int(cfg['main']['experiment_num']) == int(cfg['main']['num_experiments']):           
        benchmark = cfg['experiment']['benchmarkname']
        profile_name=cfg[benchmark]['profile_name'] 
        average_runs.main(profile_name=profile_name)
        profile_fname = profile_name + '.json'
        filedir = OUTPUT_DIR
        filepath = filedir + '/' + profile_fname
        print("Loading benchmark profile: " + profile_fname + "\n")
        try:
            if not profile_fname in os.listdir(c.C_SPARK_HOME + "conf/"):
                shutil.copy(filepath, c.C_SPARK_HOME + "conf/" + profile_fname)
                os.chmod(c.C_SPARK_HOME + "conf/" + profile_fname, 0o400)
                print("Benchmark profile successfully loaded\n") 
                """Copy profile to spark conf directory"""
            else:
                print("Not uploaded: a benchmark profile with the same name already exists\n")
        except (OSError, IOError) as exc:
            print('ERROR: {}\n\nCould not load profile)'.format(exc))