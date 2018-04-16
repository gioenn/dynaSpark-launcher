from enum import Enum
import json
import os
from pathlib import Path
from ast import literal_eval
#from util.utils import string_to_bool
'''
from config import Heuristic, AZ_KEY_NAME, AZ_PUB_KEY_PATH, AZ_PRV_KEY_PATH, \
    AWS_ACCESS_ID, AWS_SECRET_KEY, AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID,\
    ROOT_DIR, CLUSTERS_CFG_FILENAME, CLUSTERS_CFG_PATH, \
    PROVIDER, AZ_LOCATION, AZ_SIZE, AZ_IMAGE, AZ_VHD_IMAGE, \
    AZ_RESOURCE_GROUP, AZ_STORAGE_ACCOUNT, AZ_SA_SKU, AZ_SA_KIND, AZ_NETWORK, \
    AZ_SUBNET, AZ_SECURITY_GROUP, DATA_AMI, CREDENTIAL_PROFILE, REGION, \
    KEY_PAIR_PATH, SECURITY_GROUP, PRICE, INSTANCE_TYPE, NUM_INSTANCE, \
    EBS_OPTIMIZED, REBOOT, KILL_JAVA, NUM_RUN, PROCESS_ON_SERVER, INSTALL_PYTHON3, \
    CLUSTER_ID, TAG, HDFS_MASTER, SPARK_SEQ_HOME, SPARK_2_HOME, C_SPARK_HOME, \
    SPARK_HOME, LOG_LEVEL, UPDATE_SPARK, UPDATE_SPARK_MASTER, UPDATE_SPARK_DOCKER, \
    ENABLE_EXTERNAL_SHUFFLE, LOCALITY_WAIT, LOCALITY_WAIT_NODE, LOCALITY_WAIT_PROCESS, \
    LOCALITY_WAIT_RACK, CPU_TASK, RAM_DRIVER, RAM_EXEC, OFF_HEAP, OFF_HEAP_BYTES, \
    CORE_VM, CORE_HT_VM, DISABLE_HT, ALPHA, BETA, DEADLINE, MAX_EXECUTOR, OVER_SCALE,\
    K, TI, T_SAMPLE, CORE_QUANTUM, CORE_MIN, CPU_PERIOD, RUN, SYNC_TIME, PREV_SCALE_FACTOR, \
    BENCH_NUM_TRIALS, BENCHMARK_PERF, BENCHMARK_BENCH, BENCH_CONF, TERMINATE, HDFS, \
    HADOOP_CONF, HADOOP_HOME, DELETE_HDFS, HEURISTIC, STAGE_ALLOCATION, CORE_ALLOCATION,  \
    DEADLINE_ALLOCATION, CONFIG_DICT, BENCH_LINES, PRIVATE_KEY_PATH, PRIVATE_KEY_NAME, \
    TEMPORARY_STORAGE, UPDATE_SPARK_BENCH, UPDATE_SPARK_PERF, SPARK_PERF_FOLDER, \
    CLUSTER_MAP, VAR_PAR_MAP, INPUT_RECORD, NUM_TASK, SCALE_FACTOR
'''
#import config as c

class Config(object):
    """
    Configuration class for cSpark test benchmark
    """    
    class Heuristic(Enum):
        CONTROL = 0
        FIXED = 1
        CONTROL_UNLIMITED = 2
    
    REGION = ""                             #"""Region of AWS to use"""
    DATA_AMI = {}                           #"""AMI id for region and availability zone"""
    PROVIDER = ""                           #"""Provider to be used"""
    AZ_KEY_NAME = ""                        #""" name of Azure private key """
    AZ_PUB_KEY_PATH = ""                    #""" path of Azure public key """
    AZ_PRV_KEY_PATH = ""                    #""" path of Azure private key """
    AWS_ACCESS_ID = ""                      #""" Azure access id """
    AWS_SECRET_KEY = ""                     #""" Azure secret key """
    AZ_APPLICATION_ID = ""                  #""" Azure application id """
    AZ_SECRET = ""                          #""" Azure secret """
    AZ_SUBSCRIPTION_ID = ""                 #""" Azure subscription id """
    AZ_TENANT_ID = ""                       #""" Azure tenant id """
    KEY_PAIR_PATH = ""                      #""" AWS keypair path """
    AZ_LOCATION = ""                        #"""AZURE Datacenter Location"""
    AZ_SIZE = ""                            #"""AZURE VM Size"""                         
    AZ_IMAGE = ""                           #"""AZURE VM Image"""
    AZ_VHD_IMAGE = {}                       #"""AZURE VHD Image"""
    AZ_RESOURCE_GROUP = ""                  #"""AZURE Resource Group"""
    AZ_STORAGE_ACCOUNT = ""                 #"""AZURE Storage Group"""
    AZ_SA_SKU = ""                          #"""AZURE Storage SKU"""
    AZ_SA_KIND = ""                         #"""AZURE Storage Kind"""               
    AZ_NETWORK = ""                         #"""AZURE Virtual Network"""
    AZ_SUBNET = ""                          #"""AZURE Subnet"""
    AZ_SECURITY_GROUP = ""                  #"""AZURE Security Group"""
    CREDENTIAL_PROFILE = ""                 #"""Credential profile name of AWS"""
    SECURITY_GROUP = ""                     #"""Security group of the instance"""
    PRICE = ""                              #"""Aws spot instance 
    INSTANCE_TYPE = ""                      #"""Instance type"""
    NUM_INSTANCE = 0                        #"""Number of instance to use"""
    EBS_OPTIMIZED = False
    REBOOT = False                          #"""Reboot the instances of the cluster"""
    KILL_JAVA = True                        #"""Kill every java application on the cluster"""
    NUM_RUN = 1                             #"""Number of run to repeat the benchmark"""                          
    PROCESS_ON_SERVER = False               #"""Download benchmark logs and generate profiles and plots on server """
    INSTALL_PYTHON3 = True                  #"""Install Python3 on cspark master"""
    CLUSTER_ID = ""                         #"""Id of the cluster with the launched instances"""
    TAG = [{}]                              #"""Cluster Tag name"""
    HDFS_MASTER = ""                        #"""use private ip for azure!"""              
    SPARK_SEQ_HOME = ""                     # "sequential" Spark home directory                     
    SPARK_2_HOME = ""                       # regular Spark home directory
    C_SPARK_HOME = ""                       # "controlled" spark home directory
    SPARK_HOME = ""                         # Location of Spark in the ami"""
    LOG_LEVEL = ""                          # Spark log verbosity level
    UPDATE_SPARK = False                    #"""Git pull and build Spark of all the cluster"""
    UPDATE_SPARK_MASTER = True              #"""Git pull and build Spark only of the master node"""
    UPDATE_SPARK_DOCKER = False             #"""Pull the docker image in each node of the cluster"""
    ENABLE_EXTERNAL_SHUFFLE = "true"
    LOCALITY_WAIT = 0
    LOCALITY_WAIT_NODE = 0
    LOCALITY_WAIT_PROCESS = 1
    LOCALITY_WAIT_RACK = 0
    CPU_TASK = 1
    RAM_DRIVER = "100g"
    RAM_EXEC = ""
    OFF_HEAP = False
    OFF_HEAP_BYTES = 30720000000
    CORE_VM = 10                            # max 16
    CORE_HT_VM = 10                         # max 16
    DISABLE_HT = False
    ALPHA = 0.95
    BETA = 0.33
    DEADLINE = 37600
    MAX_EXECUTOR = 8
    OVER_SCALE = 2
    K = 50
    TI = 12000
    T_SAMPLE = 1000
    CORE_QUANTUM = 0.05
    CORE_MIN = 0.0
    CPU_PERIOD = 100000    
    
    # BENCHMARK
    RUN = True
    SYNC_TIME = 1
    PREV_SCALE_FACTOR = 0                   #"""*Important Settings* if it is equals to SCALE_FACTOR no need to generate new data on HDFS"""
    BENCH_NUM_TRIALS = 1 
    BENCHMARK_PERF = [                      #"""Spark-perf benchmark to execute"""
            # "scala-agg-by-key",
            # "scala-agg-by-key-int",
            # "scala-agg-by-key-naive",
            # "scala-sort-by-key"#,
            # "scala-sort-by-key-int",
            # "scala-count",
            # "scala-count-w-fltr",
        ]
    BENCHMARK_BENCH = [                     #"""Spark-bench benchmark to execute"""
            "PageRank"#,
            # "DecisionTree",
            # "KMeans",
            # "SVM"
        ]
    
    BENCH_CONF = {                          #"""Setting of the supported benchmark"""
        "scala-agg-by-key": {
            "ScaleFactor": 5
        },
        "scala-agg-by-key-int": {
            "ScaleFactor": 5
        },
        "scala-agg-by-key-naive": {
            "ScaleFactor": 5
        },
        "scala-sort-by-key": {
            "ScaleFactor": 13,
    #        "skew": 0,
            "num-partitions": 100,
            "unique-keys": 100,
            "reduce-tasks": 100
        },
        "scala-sort-by-key-int": {
            "ScaleFactor": 25
        },
        "scala-count": {
            "ScaleFactor": 25
        },
        "scala-count-w-fltr": {
            "ScaleFactor": 25
        },
        "PageRank": {
            "NUM_OF_PARTITIONS": 1000,
            "numV": 3000000,
            "mu": 3.0,
            "sigma": 0.0,
            "MAX_ITERATION": 1,
            "NumTrials": 1
        },
        "KMeans": {
            # DataGen
            "NUM_OF_POINTS": 100000000,
            "NUM_OF_CLUSTERS": 10,
            "DIMENSIONS": 10,
            "SCALING": 0.6,
            "NUM_OF_PARTITIONS": 1000,
            # Run
            "MAX_ITERATION": 1
        },
        "DecisionTree": {
            "NUM_OF_PARTITIONS": 1000,
            "NUM_OF_EXAMPLES": 50000000,
            "NUM_OF_FEATURES": 6,
            "NUM_OF_CLASS_C": 10,
            "MAX_ITERATION": 1
        },
        "SVM": {
            "NUM_OF_PARTITIONS": 1000,
            "NUM_OF_EXAMPLES": 100000000,
            "NUM_OF_FEATURES": 5,
            "MAX_ITERATION": 1
        }
    }

    TERMINATE = False                       # Terminate instance after benchmark
    HDFS = 0                                # TODO Fix this variable for plot
    HADOOP_CONF = "/usr/local/lib/hadoop-2.7.2/etc/hadoop/"
    HADOOP_HOME = "/usr/local/lib/hadoop-2.7.2"
    DELETE_HDFS = 0
    
    # HEURISTICS
    HEURISTIC = Heuristic.CONTROL_UNLIMITED
    
    # KMEANS
    # STAGE_ALLOCATION = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    # CORE_ALLOCATION = [14.6552, 1.3668, 4.2932, 3.2259, 5.9839, 3.1770, 5.2449, 2.5064, 6.5889, 2.6935, 5.9204, 2.8042,
    #                   9.6728, 1.7509, 3.8915, 0.7313, 12.2620, 3.1288]
    # DEADLINE_ALLOCATION = [18584, 1733, 5444, 4090, 7588, 4028, 6651, 3178, 8355, 3415, 7507, 3556, 12266, 2220, 4934, 927,
    #                       15549, 3967]
    
    # SVM
    # STAGE_ALLOCATION = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 18, 23]
    # CORE_ALLOCATION = [0.3523, 7.9260, 0.6877, 1.3210, 0.5275, 4.1795, 7.8807, 6.6329, 2.2927, 2.9942, 3.2869, 3.2016,
    #                    0.8377]
    # DEADLINE_ALLOCATION = [953, 21451, 1861, 3575, 1427, 11312, 21329, 17952, 6205, 8103, 8896, 8665, 2267]
    
    # AGG BY KEY
    # STAGE_ALLOCATION = [0, 1]
    # CORE_ALLOCATION = [6.3715 , 2.2592]
    # DEADLINE_ALLOCATION = [84158, 29841]
    
    STAGE_ALLOCATION = None
    CORE_ALLOCATION = None
    DEADLINE_ALLOCATION = None

    CONFIG_DICT = {
        "Provider": PROVIDER,
        "Benchmark": {
            "Name": BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0],
            "Config": BENCH_CONF[BENCHMARK_PERF[0] if len(BENCHMARK_PERF) > 0 else BENCHMARK_BENCH[0]]
        },
        "Deadline": DEADLINE,
        "Control": {
            "Alpha": ALPHA,
            "Beta": BETA,
            "OverScale": OVER_SCALE,
            "MaxExecutor": MAX_EXECUTOR,
            "CoreVM": CORE_VM,
            "K": K,
            "Ti": TI,
            "TSample": T_SAMPLE,
            "CoreQuantum": CORE_QUANTUM,
            "Heuristic": HEURISTIC.name,
            "CoreAllocation": CORE_ALLOCATION,
            "DeadlineAllocation": DEADLINE_ALLOCATION,
            "StageAllocation": STAGE_ALLOCATION
        },
        "Aws": {
            "InstanceType": INSTANCE_TYPE,
            "HyperThreading": not DISABLE_HT,
            "Price": PRICE,
            "AMI": "",  # DATA_AMI[REGION]["ami"],
            "Region": "",  # REGION,
            "AZ": "",  # DATA_AMI[REGION]["az"],
            "SecurityGroup": SECURITY_GROUP,
            "KeyPair":  "",  #DATA_AMI[REGION]["keypair"],
            "EbsOptimized": EBS_OPTIMIZED,
            "SnapshotId":  "",  #DATA_AMI[REGION]["snapid"]
        },
        "Azure": {
            "NodeSize": AZ_SIZE,
            "NodeImage": AZ_VHD_IMAGE,
            "Location": AZ_LOCATION,
            "PubKeyPath": AZ_PUB_KEY_PATH,
            "ClusterId": CLUSTER_ID,
            "ResourceGroup": AZ_RESOURCE_GROUP,
            "StorageAccount": {"Name": AZ_STORAGE_ACCOUNT,
                               "Sku": AZ_SA_SKU,
                               "Kind": AZ_SA_KIND},
            "Network": AZ_NETWORK,
            "Subnet": AZ_SUBNET,
            "SecurityGroup": AZ_SECURITY_GROUP
        },
        "Spark": {
            "ExecutorCore": CORE_VM,
            "ExecutorMemory": RAM_EXEC,
            "ExternalShuffle": ENABLE_EXTERNAL_SHUFFLE,
            "LocalityWait": LOCALITY_WAIT,
            "LocalityWaitProcess": LOCALITY_WAIT_PROCESS,
            "LocalityWaitNode": LOCALITY_WAIT_NODE,
            "LocalityWaitRack": LOCALITY_WAIT_RACK,
            "CPUTask": CPU_TASK,
            "SparkHome": SPARK_HOME
        },
        "HDFS": bool(HDFS),
    }
    # Line needed for enabling/disabling benchmark in spark-perf config.py
    BENCH_LINES = {"scala-agg-by-key": ["225", "226"],
                   "scala-agg-by-key-int": ["229", "230"],
                   "scala-agg-by-key-naive": ["232", "233"],
                   "scala-sort-by-key": ["236", "237"],
                   "scala-sort-by-key-int": ["239", "240"],
                   "scala-count": ["242", "243"],
                   "scala-count-w-fltr": ["245", "246"]}
    PRIVATE_KEY_PATH = None
    PRIVATE_KEY_NAME = None
    TEMPORARY_STORAGE = None
    UPDATE_SPARK_BENCH = True
    UPDATE_SPARK_PERF = False
    SPARK_PERF_FOLDER = "spark-perf"
    CLUSTER_MAP = {
        'hdfs': 'CSPARKHDFS',
        'spark': 'CSPARKWORK',
        'generic': 'ZOT'
    }
    VAR_PAR_MAP = {
        'pagerank': {
            'var_name': 'num_v',
            'default': 10000000,
            'profile_name': 'Spark_PageRank_Application',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        'kmeans': {
            'var_name': 'num_of_points',
            'default': 10000000,
            'profile_name': 'Spark_KMeans_Example',
            'stage_allocation': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
            'core_allocation': [14.6552, 1.3668, 4.2932, 3.2259, 5.9839, 3.1770, 5.2449, 2.5064, 6.5889, 2.6935, 5.9204, 2.8042,
                                9.6728, 1.7509, 3.8915, 0.7313, 12.2620, 3.1288],
            'deadline_allocation': [18584, 1733, 5444, 4090, 7588, 4028, 6651, 3178, 8355, 3415, 7507, 3556, 12266, 2220, 4934, 927,
                                    15549, 3967]
        },
        "decisiontree": {
            'profile_name': 'DecisionTree_classification_Example',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        'sort_by_key': {
            'var_name': 'scale_factor',
            'default': 15,
            'profile_name': 'TestRunner__sort-by-key',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        'SVM': {
            'var_name': 'scale_factor',
            'default': 10000000,
            'profile_name': 'SVM_Classifier_Example',
            'stage_allocation': [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 18, 23],
            'core_allocation': [0.3523, 7.9260, 0.6877, 1.3210, 0.5275, 4.1795, 7.8807, 6.6329, 2.2927, 2.9942, 3.2869, 3.2016,
                                0.8377],
            'deadline_allocation': [953, 21451, 1861, 3575, 1427, 11312, 21329, 17952, 6205, 8103, 8896, 8665, 2267]
        },
        'svm': {
            'var_name': 'scale_factor',
            'default': 10000000,
            'profile_name': 'SVM_Classifier_Example',
            'stage_allocation': [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 18, 23],
            'core_allocation': [0.3523, 7.9260, 0.6877, 1.3210, 0.5275, 4.1795, 7.8807, 6.6329, 2.2927, 2.9942, 3.2869, 3.2016,
                                0.8377],
            'deadline_allocation': [953, 21451, 1861, 3575, 1427, 11312, 21329, 17952, 6205, 8103, 8896, 8665, 2267]
        },
        "scala-agg-by-key": {
            'profile_name': 'TestRunner__aggregate-by-key',
            'stage_allocation': [0, 1],
            'core_allocation': [6.3715 , 2.2592],
            'deadline_allocation': [84158, 29841]
        },
        "scala-agg-by-key-int": {
            'profile_name': 'TestRunner__aggregate-by-key-int',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        "scala-agg-by-key-naive": {
            'profile_name': 'TestRunner__aggregate-by-key-naive',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        "scala-sort-by-key": {
            'var_name': 'scale_factor',
            'default': 15,
            'profile_name': 'TestRunner__sort-by-key',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        "scala-sort-by-key-int": {
            'profile_name': 'TestRunner__aggregate-by-key-int',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        "scala-count": {
            'profile_name': 'TestRunner__count',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        "scala-count-w-fltr": {
            'profile_name': 'TestRunner__count-with-filter',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        "PageRank": {
            'var_name': 'num_v',
            'default': 10000000,
            'profile_name': 'Spark_PageRank_Application',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        },
        "KMeans": {
            'var_name': 'num_of_points',
            'default': 10000000,
            'profile_name': 'Spark_KMeans_Example',
            'stage_allocation': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
            'core_allocation': [14.6552, 1.3668, 4.2932, 3.2259, 5.9839, 3.1770, 5.2449, 2.5064, 6.5889, 2.6935, 5.9204, 2.8042,
                                9.6728, 1.7509, 3.8915, 0.7313, 12.2620, 3.1288],
            'deadline_allocation': [18584, 1733, 5444, 4090, 7588, 4028, 6651, 3178, 8355, 3415, 7507, 3556, 12266, 2220, 4934, 927,
                                    15549, 3967]
        },
        "DecisionTree": {
            'profile_name': '',
            'stage_allocation': None,
            'core_allocation': None,
            'deadline_allocation': None
        }  
    }
    SCALE_FACTOR = "" 
    NUM_TASK = ""
    INPUT_RECORD = "" 
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CLUSTERS_CFG_FILENAME = 'cfg_clusters.ini'
    CLUSTERS_CFG_PATH = os.path.join(ROOT_DIR, CLUSTERS_CFG_FILENAME)
    
    cfg_dict = {    "AzKeyName": AZ_KEY_NAME,
                    "AzPubKeyPath": AZ_PUB_KEY_PATH,
                    "AzPrvKeyPath": AZ_PRV_KEY_PATH,
                    "AwsAccessId": AWS_ACCESS_ID, 
                    "AwsSecretKey": AWS_SECRET_KEY,
                    "AzApplicationId": AZ_APPLICATION_ID,
                    "AzSecret": AZ_SECRET,
                    "AzSubscriptionId": AZ_SUBSCRIPTION_ID,
                    "AzTenantId": AZ_TENANT_ID,
                    "KeyPairPath": KEY_PAIR_PATH,
                    "AzLocation": AZ_LOCATION,
                    "AzSize": AZ_SIZE,
                    "AzImage": AZ_IMAGE,
                    "AzVhdImage": AZ_VHD_IMAGE,
                    "AzResourceGroup": AZ_RESOURCE_GROUP,
                    "AzStorageAccount": AZ_STORAGE_ACCOUNT,
                    "AzSaSku": AZ_SA_SKU,
                    "AzSaKind": AZ_SA_KIND,
                    "AzNetwork": AZ_NETWORK,
                    "AzSubnet": AZ_SUBNET,
                    "AzSecurityGroup": AZ_SECURITY_GROUP,
                    "DataAmi": DATA_AMI,
                    "CredentialProfile": CREDENTIAL_PROFILE,
                    "Provider": PROVIDER,
                    "Region": REGION,
                    "SecurityGroup": SECURITY_GROUP,
                    "Price": PRICE,
                    "InstanceType": INSTANCE_TYPE,
                    "NumInstance": NUM_INSTANCE,
                    "EbsOptimized": EBS_OPTIMIZED,
                    "Reboot": REBOOT,
                    "KillJava": KILL_JAVA,
                    "NumRun": NUM_RUN,
                    "ProcessOnServer": PROCESS_ON_SERVER,
                    "InstallPython3": INSTALL_PYTHON3,
                    "ClusterId": CLUSTER_ID,
                    "Tag": TAG,
                    "HdfsMaster": HDFS_MASTER,
                    "SparkSeqHome": SPARK_SEQ_HOME,
                    "Spark2Home": SPARK_2_HOME,
                    "CSparkHome": C_SPARK_HOME,
                    "SparkHome": SPARK_HOME,
                    "LogLevel": LOG_LEVEL,
                    "UpdateSpark": UPDATE_SPARK,
                    "UpdateSparkMaster": UPDATE_SPARK_MASTER,
                    "UpdateSparkDocker": UPDATE_SPARK_DOCKER,
                    "EnableExternalShuffle": ENABLE_EXTERNAL_SHUFFLE,
                    "LocalityWait": LOCALITY_WAIT,
                    "LocalityWaitNode": LOCALITY_WAIT_NODE,
                    "LocalityWaitProcess": LOCALITY_WAIT_PROCESS,
                    "LocalityWaitRack": LOCALITY_WAIT_RACK,
                    "CpuTask": CPU_TASK,
                    "RamDriver": RAM_DRIVER,
                    "RamExec": RAM_EXEC,
                    "OffHeap": OFF_HEAP,
                    "OffHeapbytes": OFF_HEAP_BYTES,
                    "CoreVM": CORE_VM,
                    "CoreHTVM": CORE_HT_VM,
                    "DisableHT": DISABLE_HT,
                    "Alpha": ALPHA,
                    "Beta": BETA,
                    "Deadline": DEADLINE,
                    "MaxExecutor": MAX_EXECUTOR,
                    "OverScale": OVER_SCALE,
                    "K": K,
                    "Ti": TI,
                    "TSample": T_SAMPLE,
                    "CoreQuantum": CORE_QUANTUM,
                    "CoreMin": CORE_MIN,
                    "CpuPeriod": CPU_PERIOD,    
                    "Run": RUN,
                    "SyncTime": SYNC_TIME,
                    "PrevScaleFactor": PREV_SCALE_FACTOR,
                    "BenchNumTrials": BENCH_NUM_TRIALS, 
                    "BenchmarkPerf": BENCHMARK_PERF,
                    "BenchmarkBench": BENCHMARK_BENCH,
                    "BenchConf": BENCH_CONF,
                    "Terminate": TERMINATE,
                    "Hdfs": HDFS,
                    "HadoopConf": HADOOP_CONF,
                    "HadoopHome": HADOOP_HOME,
                    "DeleteHdfs": DELETE_HDFS,
                    "Heuristic": HEURISTIC.name,
                    "StageAllocation": STAGE_ALLOCATION,
                    "CoreAllocation": CORE_ALLOCATION,
                    "DeadlineAllocation": DEADLINE_ALLOCATION,
                    "ConfigDict": CONFIG_DICT,
                    "BenchLines": BENCH_LINES,
                    "PrivateKeyPath": PRIVATE_KEY_PATH,
                    "PrivateKeyName": PRIVATE_KEY_NAME,
                    "TemporaryStorage": TEMPORARY_STORAGE,
                    "UpdateSparkBench": UPDATE_SPARK_BENCH,
                    "UpdateSparkPerf": UPDATE_SPARK_PERF,
                    "SparkPerfFolder": SPARK_PERF_FOLDER,
                    "ClusterMap": CLUSTER_MAP,
                    "VarParMap": VAR_PAR_MAP,
                    "ScaleFactor": SCALE_FACTOR, 
                    "NumTask": NUM_TASK,
                    "InputRecord": INPUT_RECORD, 
                    "RootDir": ROOT_DIR, 
                    "ClustersCfgFilename": CLUSTERS_CFG_FILENAME, 
                    "ClustersCfgPath": CLUSTERS_CFG_PATH
                    }
    
    def __init__(self):
        self.config_credentials("credentials.json")
        self.config_setup("setup.json")
        self.config_control("control.json")
        self.update_config_parms(self)
        print("Config instance initialized.")

    def config_experiment(self, filepath, cfg):
        exp_file = Path(filepath)
        if exp_file.exists():
            experiment = json.load(open(filepath))
            if len(experiment) > 0:
                cfg["experiment"] = {}
                keys = experiment.keys()
                benchmark = ""
                is_benchmark_bench = False
                is_benchmark_perf = False
                if "ReuseDataset" in keys:
                    self.cfg_dict["DeleteHdfs"] = self.DELETE_HDFS = not experiment["ReuseDataset"]
                    self.cfg_dict["PrevScaleFactor"] = self.PREV_SCALE_FACTOR = self.NUM_TASK if experiment["ReuseDataset"] else 0
                    cfg["experiment"]["reusedataset"] = str(experiment["ReuseDataset"])
                    if 'delete_hdfs' in cfg['main']:
                        cfg['main']['delete_hdfs'] = str(not cfg.getboolean('experiment', 'reusedataset'))
                if "Deadline" in keys:
                    self.cfg_dict["Deadline"] = self.DEADLINE = experiment["Deadline"]
                    cfg["experiment"]["deadline"] = str(experiment["Deadline"])
                if "BenchmarkConf" in keys and "BenchmarkName" in keys:
                    cfg["BenchmarkConf"] = {}
                    benchmark = experiment["BenchmarkName"]
                    cfg["experiment"]["benchmarkname"] = experiment["BenchmarkName"]
                    is_benchmark_bench = benchmark in ["PageRank", "KMeans", "DecisionTree", "SVM"]
                    is_benchmark_perf = benchmark in ["scala-agg-by-key", "scala-agg-by-key-int", 
                                                              "scala-agg-by-key-naive", "scala-sort-by-key", 
                                                              "scala-sort-by-key-int", "scala-count", 
                                                              "scala-count-w-fltr"]
                    for k_bench_conf in experiment["BenchmarkConf"].keys():
                        mapped_parm = self.exp_par_map(k_bench_conf)
                        if is_benchmark_bench and k_bench_conf != "NumTrials":
                            self.cfg_dict["BenchConf"][benchmark][mapped_parm] = self.BENCH_CONF[benchmark][mapped_parm] = experiment["BenchmarkConf"][k_bench_conf]
                        elif is_benchmark_perf or is_benchmark_bench and k_bench_conf == "NumTrials":
                            self.cfg_dict["BenchConf"][benchmark][mapped_parm] = self.BENCH_CONF[benchmark][mapped_parm] = experiment["BenchmarkConf"][k_bench_conf]
                    keys = self.BENCH_CONF[benchmark].keys()
                    for key in keys:
                        inv_mapped_parm = self.exp_inverse_par_map(key)
                        cfg['BenchmarkConf'][inv_mapped_parm] = str(self.BENCH_CONF[benchmark][key]) \
                            if key != "NumTrials" else str(self.BENCH_CONF[benchmark][key])
                    self.cfg_dict["BenchmarkBench"] = self.BENCHMARK_BENCH = [benchmark] if is_benchmark_bench else []
                    self.cfg_dict["BenchmarkPerf"] = self.BENCHMARK_PERF = [benchmark] if is_benchmark_perf else []
                #if "SyncTime" in keys:
                #    SYNC_TIME = experiment["SyncTime"]
                #if "PrevScaleFactor" in keys:
                #    PREV_SCALE_FACTOR = experiment["PrevScaleFactor"]
                #if "BenchNumTrials" in keys:
                #    BENCH_NUM_TRIALS = experiment["BenchNumTrials"]            
                #if "BenchmarkPerf" in keys:
                #    BENCHMARK_PERF = experiment["BenchmarkPerf"]
                #if "BenchmarkBench" in keys:
                #    BENCHMARK_BENCH = experiment["BenchmarkBench"]
                
                #if "BenchmarkConf" in keys:
                #    BENCH_CONF = experiment["BenchmarkConf"]  else BENCH_CONF
                #    k_BenchmarkConf = experiment["BenchmarkConf"].keys()
                #    print("config_experiment: " + str(BENCHMARK_BENCH + BENCHMARK_PERF))
                #    for bench in BENCHMARK_BENCH + BENCHMARK_PERF:
                #        if bench in k_BenchmarkConf:
                #            BENCH_CONF[bench] = experiment["BenchmarkConf"][bench]
                                                              
                self.update_config_parms(self)
                
            else: 
                print("Experiment file: "+ filepath + " is empty: using defaults")
        else: 
            print("Experiment file: "+ filepath + " not found: exiting program")
            exit(-1)
    
    def config_benchmark(self, benchmark, cfg):
        #benchmark = cfg['main']['benchmark'] if 'main' in cfg and 'benchmark' in cfg['main'] else ''
        is_benchmark_bench = self.ini_par_map(benchmark) in ["PageRank", "KMeans", "DecisionTree", "SVM"]
        is_benchmark_perf = self.ini_par_map(benchmark) in ["scala-agg-by-key", "scala-agg-by-key-int", 
                                                                                  "scala-agg-by-key-naive", "scala-sort-by-key", 
                                                                                  "scala-sort-by-key-int", "scala-count", 
                                                                                  "scala-count-w-fltr"]
        if benchmark == 'pagerank':
            numV = cfg['pagerank']['num_v']
            self.cfg_dict["BenchConf"]["PageRank"]["numV"] = self.BENCH_CONF["PageRank"]["numV"] = literal_eval(numV)
            num_partitions = cfg['pagerank']['num_partitions']
            self.cfg_dict["BenchConf"]["PageRank"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["PageRank"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting numV as {}'.format(self.BENCH_CONF["PageRank"]["numV"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["PageRank"]["NUM_OF_PARTITIONS"]))

        elif benchmark == 'kmeans':
            num_of_points = cfg['kmeans']['num_of_points']
            self.cfg_dict["BenchConf"]["KMeans"]["NUM_OF_POINTS"] = self.BENCH_CONF["KMeans"]["NUM_OF_POINTS"] = literal_eval(num_of_points)
            num_partitions = cfg['kmeans']['num_partitions']
            self.cfg_dict["BenchConf"]["KMeans"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["KMeans"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting NUM_OF_POINTS as {}'.format(self.BENCH_CONF["KMeans"]["NUM_OF_POINTS"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["KMeans"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'decisiontree':
            num_of_examples = cfg['decisiontree']['num_of_examples']
            self.cfg_dict["BenchConf"]["DecisionTree"]["NUM_OF_EXAMPLES"] = self.BENCH_CONF["DecisionTree"]["NUM_OF_EXAMPLES"] = literal_eval(num_of_examples)
            num_partitions = cfg['decisiontree']['num_partitions']
            self.cfg_dict["BenchConf"]["DecisionTree"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["DecisionTree"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting NUM_OF_EXAMPLES as {}'.format(self.BENCH_CONF["DecisionTree"]["NUM_OF_EXAMPLES"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["DecisionTree"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'svm':
            scale_factor = cfg['svm']['scale_factor']
            self.cfg_dict["BenchConf"]["SVM"]["ScaleFactor"] = self.BENCH_CONF["SVM"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['svm']['num_partitions']
            self.cfg_dict["BenchConf"]["SVM"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["SVM"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting NUM_OF_EXAMPLES as {}'.format(self.BENCH_CONF["SVM"]["NUM_OF_EXAMPLES"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["SVM"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'sort_by_key':
            scale_factor = cfg['sort_by_key']['scale_factor']
            self.cfg_dict["BenchConf"]["scala-sort-by-key"]["ScaleFactor"] = self.BENCH_CONF["scala-sort-by-key"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['sort_by_key']['num_partitions']
            self.cfg_dict["BenchConf"]["scala-sort-by-key"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["scala-sort-by-key"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting ScaleFactor as {}'.format(self.BENCH_CONF["scala-sort-by-key"]["ScaleFactor"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["scala-sort-by-key"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'sort_by_key_int':
            scale_factor = cfg['sort_by_key_int']['scale_factor']
            self.cfg_dict["BenchConf"]["scala-sort-by-key-int"]["ScaleFactor"] = self.BENCH_CONF["scala-sort-by-key-int"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['sort_by_key_int']['num_partitions']
            self.cfg_dict["BenchConf"]["scala-sort-by-key-int"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["scala-sort-by-key-int"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting ScaleFactor as {}'.format(self.BENCH_CONF["scala-sort-by-key-int"]["ScaleFactor"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["scala-sort-by-key-int"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'agg_by_key':
            scale_factor = cfg['agg_by_key']['scale_factor']
            self.cfg_dict["BenchConf"]["scala-agg-by-key"]["ScaleFactor"] = self.BENCH_CONF["scala-agg-by-key"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['agg_by_key']['num_partitions']
            self.cfg_dict["BenchConf"]["scala-agg-by-key"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["scala-agg-by-key"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting ScaleFactor as {}'.format(self.BENCH_CONF["scala-agg-by-key"]["ScaleFactor"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["scala-agg-by-key"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'agg_by_key_int':
            scale_factor = cfg['agg_by_key_int']['scale_factor']
            self.cfg_dict["BenchConf"]["scala-agg-by-key-int"]["ScaleFactor"] = self.BENCH_CONF["scala-agg-by-key-int"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['agg_by_key_int']['num_partitions']
            self.cfg_dict["BenchConf"]["scala-agg-by-key-int"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["scala-agg-by-key-int"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting ScaleFactor as {}'.format(self.BENCH_CONF["scala-agg-by-key-int"]["ScaleFactor"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["scala-agg-by-key-int"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'agg_by_key_naive':
            scale_factor = cfg['agg_by_key_naive']['scale_factor']
            self.cfg_dict["BenchConf"]["scala-agg-by-key-naive"]["ScaleFactor"] = self.BENCH_CONF["scala-agg-by-key-naive"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['agg_by_key_naive']['num_partitions']
            self.cfg_dict["BenchConf"]["scala-agg-by-key-naive"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["scala-agg-by-key-naive"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            prnaive('setting ScaleFactor as {}'.format(self.BENCH_CONF["scala-agg-by-key-naive"]["ScaleFactor"]))
            prnaive('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["scala-agg-by-key-naive"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'count':
            scale_factor = cfg['count']['scale_factor']
            self.cfg_dict["BenchConf"]["scala-count"]["ScaleFactor"] = self.BENCH_CONF["scala-count"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['count']['num_partitions']
            self.cfg_dict["BenchConf"]["scala-count"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["scala-count"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting ScaleFactor as {}'.format(self.BENCH_CONF["scala-count"]["ScaleFactor"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["scala-count"]["NUM_OF_PARTITIONS"]))   
        
        elif benchmark == 'count_w_fltr':
            scale_factor = cfg['count_w_fltr']['scale_factor']
            selscale_factorf.cfg_dict["BenchConf"]["scala-count-w-fltr"]["ScaleFactor"] = self.BENCH_CONF["scala-count-w-fltr"]["ScaleFactor"] = literal_eval(scale_factor)
            num_partitions = cfg['count_w_fltr']['num_partitions']
            self.cfg_dict["BenchConf"]["scala-count-w-fltr"]["NUM_OF_PARTITIONS"] = self.BENCH_CONF["scala-count-w-fltr"]["NUM_OF_PARTITIONS"] = literal_eval(num_partitions)
            print('setting ScaleFactor as {}'.format(self.BENCH_CONF["scala-count-w-fltr"]["ScaleFactor"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(self.BENCH_CONF["scala-count-w-fltr"]["NUM_OF_PARTITIONS"]))   
        
        else: 
            print("ERROR: unknown benchmark '{}'".format(benchmark))
            exit(1)
        
        self.cfg_dict["BenchmarkBench"] = self.BENCHMARK_BENCH = [self.ini_par_map(benchmark)] if is_benchmark_bench else []
        self.cfg_dict["BenchmarkPerf"] = self.BENCHMARK_PERF = [self.ini_par_map(benchmark)] if is_benchmark_perf else []
                                                                      
        self.update_config_parms(self)
            
    def update_exp_parms(self, cfg):
        benchmark = ""
        if "experiment" in cfg:
            if "reusedataset" in cfg["experiment"]:
                self.cfg_dict["DeleteHdfs"] = self.DELETE_HDFS = not cfg.getboolean('experiment', 'reusedataset')
                self.cfg_dict["PrevScaleFactor"] = self.PREV_SCALE_FACTOR = self.NUM_TASK if cfg.getboolean('experiment', 'reusedataset') else 0
            if "deadline" in cfg["experiment"]:
                self.cfg_dict["Deadline"] = self.DEADLINE = cfg["experiment"]["deadline"]
        if "BenchmarkConf" in cfg:
            if "benchmarkname" in cfg:
                benchmark = cfg["benchmarkname"]
                is_benchmark_bench = benchmark in ["PageRank", "KMeans", "DecisionTree", "SVM"]
                is_benchmark_perf = benchmark in ["scala-agg-by-key", "scala-agg-by-key-int", 
                                                          "scala-agg-by-key-naive", "scala-sort-by-key", 
                                                          "scala-sort-by-key-int", "scala-count", 
                                                          "scala-count-w-fltr"]
                for p_bench_conf in cfg["BenchmarkConf"]:
                    mapped_parm = self.ini_par_map(p_bench_conf)
                    if is_benchmark_bench and p_bench_conf != "NumTrials":
                        self.cfg_dict["BenchConf"][benchmark][mapped_parm] = self.BENCH_CONF[benchmark][mapped_parm] = experiment["BenchmarkConf"][p_bench_conf]
                    elif is_benchmark_perf or is_benchmark_bench and p_bench_conf == "NumTrials":
                        self.cfg_dict["BenchConf"][benchmark][mapped_parm] = self.BENCH_CONF[benchmark][mapped_parm] = experiment["BenchmarkConf"][p_bench_conf]
                self.cfg_dict["BenchmarkBench"] = self.BENCHMARK_BENCH = [benchmark] if is_benchmark_bench else []
                self.cfg_dict["BenchmarkPerf"] = self.BENCHMARK_PERF = [benchmark] if is_benchmark_perf else []
        
        self.update_config_parms(self)
    
    def update_ext_parms(self, cfg_dict):
        self.NUM_RUN = self.cfg_dict["NumRun"] = cfg_dict["NumRun"]
        self.HDFS_MASTER = self.cfg_dict["HdfsMaster"] = cfg_dict["HdfsMaster"]
        self.DEADLINE = self.cfg_dict["Deadline"] = cfg_dict["Deadline"]
        self.MAX_EXECUTOR = self.cfg_dict["MaxExecutor"] = cfg_dict["MaxExecutor"]
        self.RUN = self.cfg_dict["Run"] = cfg_dict["Run"]
        self.PREV_SCALE_FACTOR = self.cfg_dict["PrevScaleFactor"] = cfg_dict["PrevScaleFactor"]
        self.BENCHMARK_PERF = self.cfg_dict["BenchmarkPerf"] = cfg_dict["BenchmarkPerf"]
        self.BENCHMARK_BENCH = self.cfg_dict["BenchmarkBench"] = cfg_dict["BenchmarkBench"]
        self.BENCH_CONF = self.cfg_dict["BenchConf"] = cfg_dict["BenchConf"]
        self.HDFS = self.cfg_dict["Hdfs"] = cfg_dict["Hdfs"]
        self.DELETE_HDFS = self.cfg_dict["DeleteHdfs"] = cfg_dict["DeleteHdfs"]
        #self.CONFIG_DICT = self.cfg_dict["ConfigDict"] = cfg_dict["ConfigDict"]
        self.SCALE_FACTOR = self.cfg_dict["ScaleFactor"] = cfg_dict["ScaleFactor"]
        self.INPUT_RECORD = self.cfg_dict["InputRecord"] = cfg_dict["InputRecord"]
        self.SPARK_HOME = self.cfg_dict["SparkHome"] = cfg_dict["SparkHome"]

        self.update_config_parms(self)    
    
    def config_credentials(self, filepath):
        credentials_file = Path(filepath)
        if credentials_file.exists():
            credentials = json.load(open("credentials.json"))
            if len(credentials) > 0:
                keys = credentials.keys()
                if "AzTenantId" in keys: 
                    self.cfg_dict["AzTenantId"] = self.AZ_TENANT_ID = credentials["AzTenantId"]
                if "AzSubscriptionId" in keys:
                    self.cfg_dict["AzSubscriptionId"] = self.AZ_SUBSCRIPTION_ID = credentials["AzSubscriptionId"]
                if "AzApplicationId" in keys:
                    self.cfg_dict["AzApplicationId"] = self.AZ_APPLICATION_ID = credentials["AzApplicationId"]
                if "AzSecret" in keys:
                    self.cfg_dict["AzSecret"] = self.AZ_SECRET = credentials["AzSecret"]
                if "AzPubKeyPath" in keys:
                    self.cfg_dict["AzPubKeyPath"] = self.AZ_PUB_KEY_PATH = credentials["AzPubKeyPath"]
                if "AzPrvKeyPath" in keys:
                    self.cfg_dict["AzPrvKeyPath"] = self.AZ_PRV_KEY_PATH = credentials["AzPrvKeyPath"] 
                if "AwsAccessId" in keys:
                    self.cfg_dict["AwsAccessId"] = self.AWS_ACCESS_ID = credentials["AwsAccessId"]
                if "AwsSecretId" in keys:
                    self.cfg_dict["AwsSecretId"] = self.AWS_SECRET_KEY = credentials["AwsSecretId"]
                if "KeyPairPath" in keys:
                    self.cfg_dict["KeyPairPath"] = self.KEY_PAIR_PATH = credentials["KeyPairPath"]
                print("Configuration from " + filepath + " done")                    
            else: 
                print("credentials file: " + filepath + "  is empty: using defaults")
        else: 
            print("credentials file:  " + filepath + "  found: using defaults")
                    
    def config_setup(self, filepath):
        setup_file = Path(filepath)
        if setup_file.exists():
            setup = json.load(open("setup.json"))
            if len(setup) > 0:
                keys = setup.keys()
                if "Provider" in keys:
                    self.cfg_dict["Provider"] = self.PROVIDER = setup["Provider"]
                if "ProcessOnServer" in keys:
                    self.cfg_dict["ProcessOnServer"] = self.PROCESS_ON_SERVER = setup["ProcessOnServer"]
                if "InstallPython3" in keys:
                    self.cfg_dict["InstallPython3"] = self.INSTALL_PYTHON3 = setup["InstallPython3"]
                #if "NumInstance" in keys: NUM_INSTANCE = setup["NumInstance"]
                #if "Reboot" in keys: REBOOT = setup["Reboot"]
                #if "KillJava" in keys: KILL_JAVA = setup["KillJava"]
                #if "NumRun" in keys: NUM_RUN = setup["NumRun"]
                #if "ClusterId" in keys: CLUSTER_ID = setup["ClusterId"]
                #if "Tag" in keys: TAG = setup["Tag"]
                #if "HdfsMaster" in keys: HDFS_MASTER = setup["HdfsMaster"]
                #if "Terminate" in keys: TERMINATE = setup["Terminate"]
                #if "HadoopConf" in keys: HADOOP_CONF = setup["HadoopConf"]
                #if "HadoopHome" in keys: HADOOP_HOME = setup["HadoopHome"]
                #if "UpdateSparkBench" in keys: UPDATE_SPARK_BENCH = setup["UpdateSparkBench"]
                #if "UpdateSparkPerf" in keys: UPDATE_SPARK_PERF = setup["UpdateSparkPerf"]
                #if "SparkPerfFolder" in keys: SPARK_PERF_FOLDER = setup["SparkPerfFolder"]
                #if "ClusterMap" in keys: CLUSTER_MAP = setup["ClusterMap"]
                #if "VarParMap" in keys: VAR_PAR_MAP = setup["VarParMap"]
                #if "BenchLines" in keys: BENCH_LINES = setup["BenchLines"]
                #if "HDFS" in keys: HDFS = setup["HDFS"]
                if "Credentials" in keys:
                    k_Credentials = setup["Credentials"].keys()
                    if "AzTenantId" in k_Credentials: 
                        self.cfg_dict["AzTenantId"] = self.AZ_TENANT_ID = setup["Credentials"]["AzTenantId"]
                    if "AzSubscriptionId" in k_Credentials:
                        self.cfg_dict["AzSubscriptionId"] = self.AZ_SUBSCRIPTION_ID = setup["Credentials"]["AzSubscriptionId"]
                    if "AzApplicationId" in k_Credentials:
                        self.cfg_dict["AzApplicationId"] = self.AZ_APPLICATION_ID = setup["Credentials"]["AzApplicationId"]
                    if "AzSecret" in k_Credentials:
                        self.cfg_dict["AzSecret"] = self.AZ_SECRET = setup["Credentials"]["AzSecret"]
                    if "AzPubKeyPath" in k_Credentials:
                        self.cfg_dict["AzPubKeyPath"] = self.AZ_PUB_KEY_PATH = setup["Credentials"]["AzPubKeyPath"]
                    if "AzPrvKeyPath" in k_Credentials:
                        self.cfg_dict["AzPrvKeyPath"] = self.AZ_PRV_KEY_PATH = setup["Credentials"]["AzPrvKeyPath"] 
                    if "AwsAccessId" in k_Credentials:
                        self.cfg_dict["AwsAccessId"] = self.AWS_ACCESS_ID = setup["Credentials"]["AwsAccessId"]
                    if "AwsSecretId" in k_Credentials:
                        self.cfg_dict["AwsSecretId"] = self.AWS_SECRET_KEY = setup["Credentials"]["AwsSecretId"]
                    if "KeyPairPath" in k_Credentials:
                        self.cfg_dict["KeyPairPath"] = self.KEY_PAIR_PATH = setup["Credentials"]["KeyPairPath"]
                
                if "VM" in keys:
                    k_VM = setup["VM"].keys()
                    if "Core" in k_VM:
                        self.cfg_dict["CoreVM"] = self.CORE_VM = setup["VM"]["Core"]
                    if "Memory" in k_VM:
                        self.cfg_dict["RamExec"] = self.RAM_EXEC = '"' + setup["VM"]["Memory"] + '"'
                
                if "Azure" in keys:
                    k_Azure = setup["Azure"].keys()
                    if "ResourceGroup" in k_Azure:
                        self.cfg_dict["AzResourceGroup"] = self.AZ_RESOURCE_GROUP = setup["Azure"]["ResourceGroup"]
                    if "SecurityGroup" in k_Azure:
                        self.cfg_dict["AzSecurityGroup"] = self.AZ_SECURITY_GROUP = setup["Azure"]["SecurityGroup"]
                    if "StorageAccount" in k_Azure:
                        k_Azure_StorageAccount = setup["Azure"]["StorageAccount"].keys()
                        if "Name" in k_Azure_StorageAccount:
                            self.cfg_dict["AzStorageAccount"] = self.AZ_STORAGE_ACCOUNT = setup["Azure"]["StorageAccount"]["Name"]
                        if "Sku" in k_Azure_StorageAccount:
                            self.cfg_dict["AzSaSku"] = self.AZ_SA_SKU = setup["Azure"]["StorageAccount"]["Sku"]
                        if "Kind" in k_Azure_StorageAccount:
                            self.cfg_dict["AzSaKind"] = self.AZ_SA_KIND = setup["Azure"]["StorageAccount"]["Kind"]
                    if "Subnet" in k_Azure:
                        self.cfg_dict["AzSubnet"] = self.AZ_SUBNET = setup["Azure"]["Subnet"]
                    if "NodeSize" in k_Azure:
                        self.cfg_dict["AzSize"] = self.AZ_SIZE = setup["Azure"]["NodeSize"]
                    if "Network" in k_Azure:
                        self.cfg_dict["AzNetwork"] = self.AZ_NETWORK = setup["Azure"]["Network"]
                    if "Location" in k_Azure:
                        self.cfg_dict["AzLocation"] = self.AZ_LOCATION = setup["Azure"]["Location"]
                    if "ImageName" in k_Azure:
                        self.cfg_dict["AzImageName"] = self.AZ_IMAGE = setup["Azure"]["ImageName"] 
                    if "NodeImage" in k_Azure:
                        self.cfg_dict["AzVhdImage"] = self.AZ_VHD_IMAGE = setup["Azure"]["NodeImage"]
                    
                if "Aws" in keys:
                    k_Aws = setup["Aws"].keys()
                    if "Region" in k_Aws:
                        self.cfg_dict["Region"] = self.REGION = setup["Aws"]["Region"]
                    if "SecurityGroup" in k_Aws:
                        self.cfg_dict["SecurityGroup"] = self.SECURITY_GROUP = setup["Aws"]["SecurityGroup"]
                    if "Price" in k_Aws:
                        self.cfg_dict["Price"] = self.PRICE = setup["Aws"]["Price"]
                    if "InstanceType" in k_Aws:
                        self.cfg_dict["InstanceType"] = self.INSTANCE_TYPE = setup["Aws"]["InstanceType"]
                    if "EbsOptimized" in k_Aws:
                        self.cfg_dict["EbsOptimized"] = self.EBS_OPTIMIZED = setup["Aws"]["EbsOptimized"]
                    if "AwsRegions" in k_Aws:
                        self.cfg_dict["DataAmi"] = self.DATA_AMI = setup["Aws"]["AwsRegions"]
                    
                if "Spark" in keys:
                    k_Spark = setup["Spark"].keys()
                    if "Home" in k_Spark:
                        self.cfg_dict["Spark2Home"] = self.SPARK_2_HOME = setup["Spark"]["Home"]
                    if "ExternalShuffle" in keys:
                        self.cfg_dict["EnableExternalShuffle"] = self.ENABLE_EXTERNAL_SHUFFLE = setup["Spark"]["ExternalShuffle"]
                    if "LocalityWait" in keys:
                        self.cfg_dict["LocalityWait"] = self.LOCALITY_WAIT = setup["Spark"]["LocalityWait"]
                    if "LocalityWaitNode" in keys:
                        self.cfg_dict["LocalityWaitNode"] = self.LOCALITY_WAIT_NODE = setup["Spark"]["LocalityWaitNode"]
                    if "LocalityWaitProcess" in keys:
                        self.cfg_dict["LocalityWaitProcess"] = self.LOCALITY_WAIT_PROCESS = setup["Spark"]["LocalityWaitProcess"]
                    if "LocalityWaitRack" in keys:
                        self.cfg_dict["LocalityWaitRack"] = self.LOCALITY_WAIT_RACK = setup["Spark"]["LocalityWaitRack"]
                    if "CpuTask" in keys:
                        self.cfg_dict["CpuTask"] = self.CPU_TASK = setup["Spark"]["CpuTask"]
                
                if "xSpark" in keys:
                    k_xSpark = setup["xSpark"].keys()
                    if "Home" in k_xSpark:
                        self.cfg_dict["CSparkHome"] = self.C_SPARK_HOME = setup["xSpark"]["Home"]
                    #C_SPARK_HOME = setup["Spark"]["CSparkHome"] if "CSparkHome" in keys else C_SPARK_HOME
                    #SPARK_SEQ_HOME = setup["Spark"]["SparkSeqHome"] if "SparkSeqHome" in keys else SPARK_SEQ_HOME
                    #SPARK_2_HOME = setup["Spark"]["Spark2Home"] if "Spark2Home" in keys else SPARK_2_HOME
                    #LOG_LEVEL = setup["Spark"]["LogLevel"] if "LogLevel" in keys else LOG_LEVEL
                    #UPDATE_SPARK = setup["Spark"]["UpdateSpark"] if "UpdateSpark" in keys else UPDATE_SPARK
                    #UPDATE_SPARK_MASTER = setup["Spark"]["UpdateSparkMaster"] if "UpdateSparkMaster" in keys else UPDATE_SPARK_MASTER
                    #UPDATE_SPARK_DOCKER = setup["Spark"]["UpdateSparkDocker"] if "UpdateSparkDocker" in keys else UPDATE_SPARK_DOCKER
                    #RAM_DRIVER = setup["Spark"]["RamDriver"] if "RamDriver" in keys else RAM_DRIVER
                    #OFF_HEAP = setup["Spark"]["OffHeap"] if "OffHeap" in keys else OFF_HEAP
                    #OFF_HEAP_BYTES = setup["Spark"]["OffHeapBytes"] if "OffHeapBytes" in keys else OFF_HEAP_BYTES
                    #DISABLE_HT = not setup["Spark"]["HyperThreading"] if "HyperThreading" in keys else DISABLE_HT
                    
                
                if "SparkSeq" in keys:
                    k_SparkSeq = setup["SparkSeq"].keys()
                    if "Home" in k_SparkSeq:
                        self.cfg_dict["SparkSeqHome"] = self.SPARK_SEQ_HOME = setup["SparkSeq"]["Home"]
                
                self.cfg_dict["SparkHome"] = self.SPARK_HOME = self.C_SPARK_HOME
                #if "ControlledMode" in keys:
                #    if setup["ControlledMode"]:
                #        if "xSpark" in keys and "Home"in setup["xSpark"]:
                #            self.cfg_dict["SparkHome"] = self.SPARK_HOME = setup["xSpark"]["Home"] 
                #    elif "Spark" in keys and "Home"in setup["Spark"]:
                #            self.cfg_dict["SparkHome"] = self.SPARK_HOME = setup["Spark"]["Home"]
                    
                print("Configuration from " + filepath + " done")                    
            else: 
                print("Setup file: " + filepath + "  is empty: using defaults")
        else: 
            print("Setup file:  " + filepath + "  found: using defaults")
    
    def config_control(self, filepath):
        control_file = Path(filepath)
        if control_file.exists():
            control = json.load(open(filepath))
            if len(control) > 0:
                keys = control.keys()
                if "Alpha" in keys:
                    self.cfg_dict["Alpha"] = self.ALPHA = control["Alpha"]
                if "Beta" in keys:
                    self.cfg_dict["Beta"] = self.BETA = control["Beta"]
                if "OverScale" in keys:
                    self.cfg_dict["OverScale"] = self.OVER_SCALE = control["OverScale"]
                if "K" in keys:
                    self.cfg_dict["K"] = self.K = control["K"]
                if "Ti" in keys:
                    self.cfg_dict["Ti"] = self.TI = control["Ti"]
                if "TSample" in keys:
                    self.cfg_dict["TSample"] = self.T_SAMPLE = control["TSample"]
                if "Heuristic" in keys:
                    self.HEURISTIC = eval("self.Heuristic." + control["Heuristic"])
                    self.cfg_dict["Heuristic"] = self.HEURISTIC.name
                if "CoreQuantum" in keys:
                    self.cfg_dict["CoreQuantum"] = self.CORE_QUANTUM = control["CoreQuantum"]
                if "CoreMin" in keys:
                    self.cfg_dict["CoreMin"] = self.CORE_MIN = control["CoreMin"]
                if "CpuPeriod" in keys:
                    self.cfg_dict["CpuPeriod"] = self.CPU_PERIOD = control["CpuPeriod"]
                #DEADLINE = control["Deadline"] if "Deadline" in keys else DEADLINE
                #MAX_EXECUTOR = control["MaxExecutor"] if "MaxExecutor" in keys else MAX_EXECUTOR
                #CORE_VM = control["CoreVM"] if "CoreVM" in keys else CORE_VM
                #STAGE_ALLOCATION = control["StageAllocation"] if "StageAllocation" in keys else STAGE_ALLOCATION
                #CORE_ALLOCATION = control["CoreAllocation"] if "CoreAllocation" in keys else CORE_ALLOCATION
                #DEADLINE_ALLOCATION = control["DeadlineAllocation"] if "DeadlineAllocation" in keys else DEADLINE_ALLOCATION
                print("Configuration from " + filepath + " done")
            else: 
                print("Control file: " + filepath + " is empty: using defaults")
        else: 
            print("Control file: " + filepath + "  not found: using defaults")
    
    @staticmethod
    def update_config_parms(self):
        self.cfg_dict["EbsOptimized"] = self.EBS_OPTIMIZED = True if "r3" not in self.INSTANCE_TYPE else False
        self.cfg_dict["Tag"] = self.TAG = [{"Key": "ClusterId", 
                "Value": self.CLUSTER_ID}]
        self.cfg_dict["SparkHome"] = self.SPARK_HOME
        self.RAM_EXEC = '"100g"' if "r3" not in self.INSTANCE_TYPE else '"100g"'
        if self.OFF_HEAP:
            self.RAM_EXEC = '"30g"' if "r3" not in self.INSTANCE_TYPE else '"70g"'
        self.cfg_dict["RamExec"] = self.RAM_EXEC   
        if self.DISABLE_HT:
            self.CORE_HT_VM = self.CORE_VM
        self.cfg_dict["CoreHTVM"] = self.CORE_HT_VM    
        if len(self.BENCHMARK_PERF) + len(self.BENCHMARK_BENCH) > 1 or len(self.BENCHMARK_PERF) + len(
                self.BENCHMARK_BENCH) == 0:
            print("ERROR BENCHMARK SELECTION")
            exit(1)
        
        if len(self.BENCHMARK_PERF) > 0:
            self.cfg_dict["ScaleFactor"] = self.SCALE_FACTOR = self.BENCH_CONF[self.BENCHMARK_PERF[0]]["ScaleFactor"]
            self.cfg_dict["InputRecord"] = self.INPUT_RECORD = 200 * 1000 * 1000 * self.SCALE_FACTOR
            self.cfg_dict["NumTask"] = self.NUM_TASK = self.SCALE_FACTOR
        else:
            self.cfg_dict["ScaleFactor"] = self.SCALE_FACTOR = self.BENCH_CONF[self.BENCHMARK_BENCH[0]]["NUM_OF_PARTITIONS"]
            self.cfg_dict["NumTask"] = self.NUM_TASK = self.SCALE_FACTOR
            try:
                self.cfg_dict["InputRecord"] = self.INPUT_RECORD = self.BENCH_CONF[self.BENCHMARK_BENCH[0]]["NUM_OF_EXAMPLES"]
            except KeyError:
                try:
                    self.cfg_dict["InputRecord"] = self.BENCH_CONF[self.BENCHMARK_BENCH[0]]["NUM_OF_POINTS"]
                except KeyError:
                    self.cfg_dict["InputRecord"] = self.INPUT_RECORD = self.BENCH_CONF[self.BENCHMARK_BENCH[0]]["numV"]
        self.cfg_dict["BenchConf"][self.cfg_dict["BenchmarkPerf"][0] if len(self.cfg_dict["BenchmarkPerf"]) > 0 else self.cfg_dict["BenchmarkBench"][0]]["NumTrials"] = \
            self.BENCH_CONF[self.BENCHMARK_PERF[0] if len(self.BENCHMARK_PERF) > 0 else self.BENCHMARK_BENCH[0]]["NumTrials"] = self.BENCH_NUM_TRIALS
    
        self.cfg_dict["Hdfs"] = self.HDFS = 1 if self.HDFS_MASTER == "" else 0
        self.cfg_dict["DeleteHdfs"] = self.DELETE_HDFS
        #self.cfg_dict["DeleteHdfs"] = self.DELETE_HDFS = 1 if self.SCALE_FACTOR != self.PREV_SCALE_FACTOR else 0
        
        self.cfg_dict["PrivateKeyPath"] = self.PRIVATE_KEY_PATH = self.KEY_PAIR_PATH if self.PROVIDER == "AWS_SPOT" \
            else self.AZ_PRV_KEY_PATH if self.PROVIDER == "AZURE" \
            else None
        self.cfg_dict["PrivateKeyName"] = self.PRIVATE_KEY_NAME = self.KEY_PAIR_PATH.split("/")[-1] if self.PROVIDER == "AWS_SPOT" \
            else self.AZ_PRV_KEY_PATH.split("/")[-1] if self.PROVIDER == "AZURE" \
            else None
        self.cfg_dict["TemporaryStorage"] = self.TEMPORARY_STORAGE = "/dev/xvdb" if self.PROVIDER == "AWS_SPOT" \
            else "/dev/sdb1" if self.PROVIDER == "AZURE" \
            else None
        
        self.cfg_dict["ConfigDict"] = self.CONFIG_DICT = {
                                        "Provider": self.PROVIDER,
                                        "Benchmark": {
                                            "Name": self.BENCHMARK_PERF[0] if len(self.BENCHMARK_PERF) > 0 else self.BENCHMARK_BENCH[0],
                                            "Config": self.BENCH_CONF[self.BENCHMARK_PERF[0] if len(self.BENCHMARK_PERF) > 0 else self.BENCHMARK_BENCH[0]]
                                        },
                                        "Deadline": self.DEADLINE,
                                        "Control": {
                                            "Alpha": self.ALPHA,
                                            "Beta": self.BETA,
                                            "OverScale": self.OVER_SCALE,
                                            "MaxExecutor": self.MAX_EXECUTOR,
                                            "CoreVM": self.CORE_VM,
                                            "K": self.K,
                                            "Ti": self.TI,
                                            "TSample": self.T_SAMPLE,
                                            "CoreQuantum": self.CORE_QUANTUM,
                                            "Heuristic": self.HEURISTIC.name,
                                            "CoreAllocation": self.CORE_ALLOCATION,
                                            "DeadlineAllocation": self.DEADLINE_ALLOCATION,
                                            "StageAllocation": self.STAGE_ALLOCATION
                                        },
                                        "Aws": {
                                            "InstanceType": self.INSTANCE_TYPE,
                                            "HyperThreading": not self.DISABLE_HT,
                                            "Price": self.PRICE,
                                            "AMI": self.DATA_AMI[self.REGION]["ami"],
                                            "Region": self.REGION,
                                            "AZ": self.DATA_AMI[self.REGION]["az"],
                                            "SecurityGroup": self.SECURITY_GROUP,
                                            "KeyPair": self.DATA_AMI[self.REGION]["keypair"],
                                            "EbsOptimized": self.EBS_OPTIMIZED,
                                            "SnapshotId": self.DATA_AMI[self.REGION]["snapid"]
                                        },
                                        "Azure": {
                                            "NodeSize": self.AZ_SIZE,
                                            "NodeImage": self.AZ_VHD_IMAGE,
                                            "Location": self.AZ_LOCATION,
                                            "PubKeyPath": self.AZ_PUB_KEY_PATH,
                                            "ClusterId": self.CLUSTER_ID,
                                            "ResourceGroup": self.AZ_RESOURCE_GROUP,
                                            "StorageAccount": {"Name": self.AZ_STORAGE_ACCOUNT,
                                                               "Sku": self.AZ_SA_SKU,
                                                               "Kind": self.AZ_SA_KIND},
                                            "Network": self.AZ_NETWORK,
                                            "Subnet": self.AZ_SUBNET,
                                            "SecurityGroup": self.AZ_SECURITY_GROUP
                                        },
                                        "Spark": {
                                            "ExecutorCore": self.CORE_VM,
                                            "ExecutorMemory": self.RAM_EXEC,
                                            "ExternalShuffle": self.ENABLE_EXTERNAL_SHUFFLE,
                                            "LocalityWait": self.LOCALITY_WAIT,
                                            "LocalityWaitProcess": self.LOCALITY_WAIT_PROCESS,
                                            "LocalityWaitNode": self.LOCALITY_WAIT_NODE,
                                            "LocalityWaitRack": self.LOCALITY_WAIT_RACK,
                                            "CPUTask": self.CPU_TASK,
                                            "SparkHome": self.SPARK_HOME
                                        },
                                        "HDFS": bool(self.HDFS),
                                    }

    @staticmethod
    def exp_par_map(parm):
        map = {
            "ScaleFactor": "ScaleFactor",
            "NumPartitions": "num-partitions",
            "UniqueKeys": "unique-keys",
            "ReduceTasks": "reduce-tasks",
            "NumOfPartitions": "NUM_OF_PARTITIONS",
            "NumV": "numV",
            "Mu": "mu",
            "Sigma": "sigma",
            "MaxIterations": "MAX_ITERATION",
            "NumTrials": "NumTrials",
            "NumOfPoints": "NUM_OF_POINTS",
            "NumOfClusters": "NUM_OF_CLUSTERS",
            "Dimensions": "DIMENSIONS",
            "Scaling": "SCALING",
            "NumOfExamples": "NUM_OF_EXAMPLES",
            "NumOfFeatures": "NUM_OF_FEATURES",
            "NumOfClassC": "NUM_OF_CLASS_C",
        }
        return map.get(parm, None)
    
    @staticmethod
    def exp_inverse_par_map(parm):
        map = {
            "ScaleFactor": "ScaleFactor",
            "num-partitions": "NumPartitions",
            "unique-keys": "UniqueKeys",
            "reduce-tasks": "ReduceTasks",
            "NUM_OF_PARTITIONS": "NumOfPartitions",
            "numV": "NumV",
            "mu": "Mu",
            "sigma": "Sigma",
            "MAX_ITERATION": "MaxIterations",
            "NumTrials": "NumTrials",
            "NUM_OF_POINTS": "NumOfPoints",
            "NUM_OF_CLUSTERS": "NumOfClusters",
            "DIMENSIONS": "Dimensions",
            "SCALING": "Scaling",
            "NUM_OF_EXAMPLES": "NumOfExamples",
            "NUM_OF_FEATURES": "NumOfFeatures",
            "NUM_OF_CLASS_C": "NumOfClassC",
        }
        return map.get(parm, None)
    
    @staticmethod
    def ini_par_map(parm):
        map = {
            "scalefactor": "ScaleFactor",
            "scale_factor": "ScaleFactor",
            "numpartitions": "NumPartitions",
            "num_partitions": "NumPartitions",
            "numofpartitions": "NumOfPartitions",
            "num_of_partitions": "NumOfPartitions",
            "uniquekeys": "UniqueKeys",
            "unique_keys": "UniqueKeys",
            "reducetasks": "ReduceTasks",
            "reduce_tasks": "ReduceTasks",
            "numv": "NumV",
            "num_v": "NumV",
            "mu": "Mu",
            "sigma": "Sigma",
            "maxiterations": "MaxIterations",
            "max_iterations": "MaxIterations",
            "numtrials": "NumTrials",
            "num_trials": "NumTrials",
            "numofpoints": "NumOfPoints",
            "num_of_points": "NumOfPoints",
            "num_points": "NumOfPoints",
            "numofclusters": "NumOfClusters",
            "num_of_clusters": "NumOfClusters",
            "num_clusters": "NumOfClusters",
            "dimensions": "Dimensions",
            "scaling": "Scaling",
            "numofexamples": "NumOfExamples",
            "num_of_examples": "NumOfExamples",
            "num_examples": "NumOfExamples",
            "numoffeatures": "NumOfFeatures",
            "num_of_features": "NumOfFeatures",
            "num_features": "NumOfFeatures",
            "numofclassc": "NumOfClassC",
            "num_of_class_c": "NumOfClassC",
            "num_class_c": "NumOfClassC",
            "pagerank" : "PageRank",
            "kmeans": "KMeans",
            "decisiontree": "DecisionTree",
            "svm": "SVM",
            "scala-agg-by-key": "scala-agg-by-key", 
            "scala-agg-by-key-int": "scala-agg-by-key-int", 
            "scala-agg-by-key-naive": "scala-agg-by-key-naive", 
            "scala-sort-by-key": "scala-sort-by-key", 
            "scala-sort-by-key-int": "scala-sort-by-key-int", 
            "scala-count": "scala-count", 
            "scala-count-w-fltr": "scala-count-w-fltr",
        }
        return map.get(parm, None)
    
    def ini_lowercase_to_camelcase_par_map(parm):
        map = {
            "scale_factor": "ScaleFactor",
            "num_partitions": "NumOfPartitions",
            "unique_keys": "UniqueKeys",
            "reduce_tasks": "ReduceTasks",
            "num_of_partitions": "NumOfPartitions",
            "num_v": "NumV",
            "mu": "Mu",
            "sigma": "Sigma",
            "max_iterations": "MaxIterations",
            "num_trials": "NumTrials",
            "num_of_points": "NumOfPoints",
            "num_points": "NumOfPoints",
            "num_of_clusters": "NumOfClusters",
            "num_clusters": "NumOfClusters",
            "dimensions": "Dimensions",
            "scaling": "Scaling",
            "num_of_examples": "NumOfExamples",
            "num_examples": "NumOfExamples",
            "num_of_features": "NumOfFeatures",
            "num_features": "NumOfFeatures",
            "num_of_class_c": "NumOfClassC",
            "num_class_c": "NumOfClassC",
        }
        return map.get(parm, None)
    
config_instance = Config()

#print("PROCESS_ON_SERVER: "+ str(self.PROCESS_ON_SERVER))
#print("instantiated config_instance: " + str(config_instance.cfg_dict))