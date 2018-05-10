"""
This module handles the configuration of the instances and the execution of the benchmark on the cluster
"""

import json
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor
from ast import literal_eval
import log
from spark_log_profiling import processing
import metrics
import plot
import pickle
import os
import shutil
#from config import UPDATE_SPARK_DOCKER, DELETE_HDFS, SPARK_HOME, KILL_JAVA, SYNC_TIME, \
#    KEY_PAIR_PATH, \
#    UPDATE_SPARK, \
#    DISABLE_HT, ENABLE_EXTERNAL_SHUFFLE, OFF_HEAP, OFF_HEAP_BYTES, K, T_SAMPLE, TI, CORE_QUANTUM, \
#    CORE_MIN, CPU_PERIOD, HDFS, \
#    CORE_VM, UPDATE_SPARK_MASTER, DEADLINE, MAX_EXECUTOR, ALPHA, BETA, OVER_SCALE, LOCALITY_WAIT, \
#    LOCALITY_WAIT_NODE, CPU_TASK, \
#    LOCALITY_WAIT_PROCESS, LOCALITY_WAIT_RACK, INPUT_RECORD, NUM_TASK, BENCH_NUM_TRIALS, \
#    SCALE_FACTOR, RAM_EXEC, \
#    RAM_DRIVER, BENCHMARK_PERF, BENCH_LINES, HDFS_MASTER, DATA_AMI, REGION, HADOOP_CONF, \
#    CONFIG_DICT, HADOOP_HOME,\
#    SPARK_2_HOME, BENCHMARK_BENCH, BENCH_CONF, LOG_LEVEL, CORE_ALLOCATION, DEADLINE_ALLOCATION,\
#    UPDATE_SPARK_BENCH, UPDATE_SPARK_PERF, SPARK_PERF_FOLDER, NUM_INSTANCE, STAGE_ALLOCATION, HEURISTIC, VAR_PAR_MAP, \
#    PROCESS_ON_SERVER, INSTALL_PYTHON3, AZ_PUB_KEY_PATH, RUN, PREV_SCALE_FACTOR
#from config import PRIVATE_KEY_PATH, PRIVATE_KEY_NAME, TEMPORARY_STORAGE, PROVIDER, INSTANCE_TYPE, PRICE
#import config as c
#from configure import config_instance
from configure import config_instance as c
from util.utils import timing, between, get_cfg, write_cfg, open_cfg, make_sure_path_exists
from util.ssh_client import sshclient_from_node, sshclient_from_ip
from pathlib import Path
import socket #vboxvm
import pprint
pp = pprint.PrettyPrinter(indent=4)
# Modifiche fatte
# - uso di PRIVATE_KEY_PATH anzichè KEY_PAIR_PATH
# - uso di PRIVATE_KEY_NAME anzichè DATA_AMI[REGION]["keypair"]+".pem"
# - uso di TEMPORARY_STORAGE anzichè "/dev/xvdb"
# - uso di ssh_sshclient_from_node per ottenere un client ssh basato su ParamikoSSHClient
# - uso di ip privati in azure, altrimenti fallisce il binding sulle porte di hadoop

def get_ip(node):
    if c.PROVIDER == "AWS_SPOT":
        return node.extra['dns_name']
    if c.PROVIDER == "AZURE":
        return node.private_ips[0]
        
def common_setup(ssh_client):
    """
    Common setup of the instance of the cluster with ssh_client is connected

    :param ssh_client: the ssh client to launch command on the instance
    :return: nothing
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    #  preliminary steps required due to differences between azure and aws
    if c.PROVIDER == "AZURE":

        # todo only if first run
        if c.NUM_INSTANCE > 0:
            print("In common_setup, NUM_INSTANCE=" + str(c.NUM_INSTANCE))
            # add ssh key that matches the public one used during creation
            if not "id_rsa" in ssh_client.listdir("/home/ubuntu/.ssh/"):
                ssh_client.put(localpath=c.PRIVATE_KEY_PATH, remotepath="/home/ubuntu/.ssh/id_rsa")
            ssh_client.run("chmod 400 /home/ubuntu/.ssh/id_rsa")

            # ssh_client.run("sudo groupadd supergroup")
            ssh_client.run("sudo usermod -aG supergroup $USER")
            ssh_client.run("sudo usermod -aG supergroup root")

            # join docker group
            ssh_client.run("sudo usermod -aG docker $USER")

            # ssh_client.run("mkdir /usr/local/spark/spark-events")

            # ssh_client.run("sudo chmod -R 777 /mnt")

            # to refresh groups
            ssh_client.close()
            ssh_client.connect()

            # restore environmental variables lost when creating the image
            ssh_client.run("echo 'export JAVA_HOME=/usr/lib/jvm/java-8-oracle' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_INSTALL=/usr/local/lib/hadoop-2.7.2' >> $HOME/.bashrc")
            ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/bin' >> $HOME/.bashrc")
            ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/sbin' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_MAPRED_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_COMMON_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_HDFS_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export YARN_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native/' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_OPTS=\"-Djava.library.path=$HADOOP_INSTALL/lib/native\"'  >> $HOME/.bashrc")
            ssh_client.run(
                "echo 'export LD_LIBRARY_PATH=$HADOOP_INSTALL/lib/native:$LD_LIBRARY_PATH' >> $HOME/.bashrc")  # to fix "unable to load native hadoop lib" in spark

            ssh_client.run("source $HOME/.bashrc")


        # # PageRank
        # ssh_client.run("rm /usr/local/spark/conf/Spark_PageRank_Application.json")
        # ssh_client.put(localpath="C:\\workspace\\spark-log-profiling\\output_json\\Spark_PageRank_Application_20170523133037.json",
        #                remotepath="/usr/local/spark/conf/Spark_PageRank_Application.json")
        #
        #
        # # DecisionTree
        # ssh_client.run("rm /usr/local/spark/conf/DecisionTree_classification_Example.json")
        # ssh_client.put(
        #     localpath="C:\\workspace\\spark-log-profiling\\output_json\\DecisionTree_classification_Example_20170523134646.json",
        #     remotepath="/usr/local/spark/conf/DecisionTree_classification_Example.json")
        #
        # # Kmeans
        # ssh_client.run("rm /usr/local/spark/conf/Spark_KMeans_Example.json")
        # ssh_client.put(localpath="C:\\workspace\\spark-log-profiling\\output_json\\Spark_KMeans_Example_20170509081738.json",
        #                remotepath="/usr/local/spark/conf/Spark_KMeans_Example.json")
        #
        # # SVM
        # ssh_client.run("rm /usr/local/spark/conf/SVM_Classifier_Example.json")
        # ssh_client.put(localpath="C:\\workspace\\spark-log-profiling\\output_json\\SVM_Classifier_Example_20170509105715.json",
        #                remotepath="/usr/local/spark/conf/SVM_Classifier_Example.json")
        #
        # # Agg by key
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__aggregate-by-key.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__aggregate-by-key_20170511110351.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__aggregate-by-key.json")
        #
        # # Agg by key int
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__aggregate-by-key-int.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__aggregate-by-key-int_20170511112110.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__aggregate-by-key-int.json")
        #
        # # Agg by key naive
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__aggregate-by-key-naive.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__aggregate-by-key-naive_20170511114259.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__aggregate-by-key-naive.json")
        #
        # # Sort by key
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__sort-by-key.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__sort-by-key_20170511131321.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__sort-by-key.json")
        #
        # # Sort by key int
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__sort-by-key-int.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__sort-by-key-int_20170511133334.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__sort-by-key-int.json")
        #
        # # Count
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__count.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__count_20170511135036.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__count.json")
        #
        # # Count with filtr
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__count-with-filter.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__count-with-filter_20170511140627.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__count-with-filter.json")



    if c.PROVIDER == "AWS_SPOT":
        ssh_client.run("echo 'export JAVA_HOME=/usr/lib/jvm/java-8-oracle' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_INSTALL=/usr/local/lib/hadoop-2.7.2' >> $HOME/.bashrc")
        ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/bin' >> $HOME/.bashrc")
        ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/sbin' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_MAPRED_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_COMMON_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_HDFS_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export YARN_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_OPTS=\"-Djava.library.path=$HADOOP_INSTALL/lib/native\"'  >> $HOME/.bashrc")
        ssh_client.run(
            "echo 'export LD_LIBRARY_PATH=$HADOOP_INSTALL/lib/native:$LD_LIBRARY_PATH' >> $HOME/.bashrc")  # to fix "unable to load native hadoop lib" in spark
        ssh_client.run("source $HOME/.bashrc")


    ssh_client.run("export GOMAXPROCS=`nproc`")

    if c.UPDATE_SPARK_DOCKER:
        print("   Updating Spark Docker Image...")
        ssh_client.run("docker pull elfolink/spark:2.0")

    if delete_hdfs:
        ssh_client.run("sudo umount /mnt")
        ssh_client.run(
                "sudo mkfs.ext4 -E nodiscard " + c.TEMPORARY_STORAGE + " && sudo mount -o discard " + c.TEMPORARY_STORAGE + " /mnt")

    ssh_client.run("test -d /mnt/tmp || sudo mkdir -m 1777 /mnt/tmp")
    ssh_client.run("sudo mount --bind /mnt/tmp /tmp")

    ssh_client.run('ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R localhost')

    print("   Stop Spark Slave/Master")
    # ssh_client.run('export SPARK_HOME="{s}" && {s}sbin/stop-slave.sh'.format(s=c.SPARK_HOME))
    ssh_client.run('export SPARK_HOME="{s}" && {s}sbin/stop-master.sh'.format(s=c.SPARK_HOME))
    ssh_client.run('export SPARK_HOME="{s}" && sudo {s}sbin/stop-slave.sh'.format(s=c.SPARK_HOME))
    print("   Set Log Level")
    ssh_client.run(
        "sed -i '19s/.*/log4j.rootCategory={}, console /' {}conf/log4j.properties".format(c.LOG_LEVEL,
                                                                                          c.SPARK_HOME))

    if c.KILL_JAVA:
        print("   Killing Java")
        ssh_client.run('sudo killall java && sudo killall java && sudo killall java')

    print("   Kill SAR CPU Logger")
    ssh_client.run("screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")

    if c.SYNC_TIME:
        print("   SYNC TIME")
        ssh_client.run("sudo ntpdate -s time.nist.gov")

    print("   Removing Stopped Docker")
    ssh_client.run("docker ps -a | awk '{print $1}' | xargs --no-run-if-empty docker rm")


@timing
def setup_slave(node, master_ip, count):
    """

    :param node:
    :param master_ip:
    :return:
    """
    cfg = get_cfg()
    current_cluster = cfg['main']['current_cluster']

    ssh_client = sshclient_from_node(node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')

    print("Setup Slave: PublicIp=" + node.public_ips[0] + " PrivateIp=" + node.private_ips[0])

    slave_ip = get_ip(node)

    #cfg[current_cluster]['slave'+count+'_ip'] = slave_ip
    #write_cfg(cfg)

    common_setup(ssh_client)

    if c.UPDATE_SPARK:
        print("   Updating Spark...")
        ssh_client.run(
            """cd /usr/local/spark && git pull && git checkout """ + c.GIT_BRANCH)
        # CLEAN UP EXECUTORS APP LOGS
        ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")
        ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean && build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")

        '''
        ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean && build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")
        '''
    # CLEAN UP EXECUTORS APP LOGS
    # ssh_client.run("rm -r " + c.SPARK_HOME + "work/*")
    ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")

    if c.DISABLE_HT:
        # DISABLE HT
        ssh_client.put(localpath="./disable-ht-v2.sh", remotepath="$HOME/disable-ht-v2.sh")
        ssh_client.run("chmod +x $HOME/disable-ht-v2.sh")
        stdout, stderr, status = ssh_client.run('sudo $HOME/disable-ht-v2.sh')
        print("   Disabled HyperThreading {}".format(status))

    if current_cluster == 'spark':
    # Modificato questo
        ssh_client.run(
            "sed -i '31s/.*/spark.shuffle.service.enabled {0}/' {1}conf/spark-defaults.conf".format(
                c.ENABLE_EXTERNAL_SHUFFLE, c.SPARK_HOME))

        # ssh_client.run('echo "spark.local.dir /mnt/hdfs" >> '+ c.SPARK_HOME + 'conf/spark-defaults.conf')

        ssh_client.run(
            "sed -i '32s/.*/spark.memory.offHeap.enabled {0}/' {1}conf/spark-defaults.conf".format(
                c.OFF_HEAP, c.SPARK_HOME))
        ssh_client.run(
            "sed -i '33s/.*/spark.memory.offHeap.size {0}/' {1}conf/spark-defaults.conf".format(
                c.OFF_HEAP_BYTES, c.SPARK_HOME))

        ssh_client.run("sed -i '42s/.*/spark.control.k {0}/' {1}conf/spark-defaults.conf".format(
            c.K , c.SPARK_HOME))

        # SAMPLING RATE LINE 43
        ssh_client.run("sed -i '43s/.*/spark.control.tsample {0}/' {1}conf/spark-defaults.conf".format(
            c.T_SAMPLE, c.SPARK_HOME))

        ssh_client.run("sed -i '44s/.*/spark.control.ti {0}/' {1}conf/spark-defaults.conf".format(
            c.TI, c.SPARK_HOME))

        ssh_client.run("sed -i '45s{.*{spark.control.corequantum " + str(
            c.CORE_QUANTUM) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '50s{.*{spark.control.coremin " + str(
            c.CORE_MIN) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '41s{.*{spark.control.cpuperiod " + str(
            c.CPU_PERIOD) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

    if current_cluster == 'spark':
        print("   Starting Spark Slave")
        ssh_client.run(
            # 'export SPARK_HOME="{s}" && {s}sbin/start-slave.sh {0}:7077 -h {1}  --port 9999 -c {2}'.format(
            'export SPARK_HOME="{s}" && sudo {s}sbin/start-slave.sh {0}:7077 -h {1}  --port 9999 -c {2}'.format(

            master_ip, slave_ip, c.CORE_VM, s=c.SPARK_HOME))

        # REAL CPU LOG
        log_cpu_command = 'screen -d -m -S "{0}" bash -c "sar -u 1 > sar-{1}.log"'.format(
            slave_ip, slave_ip)
        print("   Start SAR CPU Logging")
        ssh_client.run(log_cpu_command)


@timing
def setup_master(node, slaves_ip, hdfs_master):
    """
    :param node:
    :return:
    """
    ssh_client = sshclient_from_node(node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm_removed
    #master_public_ip = socket.gethostbyname("XSPARKWORK0") #vboxvm
    #ssh_client = sshclient_from_ip(master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm
    stdout, stderr, status = ssh_client.run("hostname  && pwd") #vboxvm
    print(stdout) #vboxvm
    with open_cfg(mode='w') as cfg:
        current_cluster = cfg['main']['current_cluster'] 
        benchmark = c.CONFIG_DICT['Benchmark']['Name']
        #benchmark = cfg['main']['benchmark'] if 'main' in cfg and 'benchmark' in cfg['main'] else \
        #            cfg['experiment']['benchmarkname'] if 'experiment' in cfg and 'benchmarkname' in cfg['experiment'] else ''
        cfg[current_cluster] = {}
        # TODO check if needed
        # input_record = cfg['pagerank']['num_v'] if 'pagerank' in cfg and 'num_v' in cfg['pagerank'] else c.INPUT_RECORD
        # print("input_record: {}".format(input_record))
        print("Setup Master: BENCH_CONF=" + str(c.BENCH_CONF[benchmark]))
        #master_private_ip = master_public_ip #vboxvm
        #print("Setup Master: PublicIp=" + master_public_ip + " PrivateIp=" + master_private_ip) #vboxvm
        print("Setup Master: PublicIp=" + node.public_ips[0] + " PrivateIp=" + node.private_ips[0]) #vboxvm_removed
        master_private_ip = get_ip(node)  #vboxvm_removed
        master_public_ip = node.public_ips[0]  #vboxvm_removed

        # save private master_ip to cfg file
        print('saving master ip')
        cfg[current_cluster]['master_private_ip'] = master_private_ip
        cfg[current_cluster]['master_public_ip'] = master_public_ip

    common_setup(ssh_client) #vboxvm_removed



    # update spark-bench and spark-perf

    ssh_client.run("sudo mv /usr/local/spark-perf/ /home/ubuntu/")
    ssh_client.run("sudo mv /usr/local/spark-bench/ /home/ubuntu/")
    ssh_client.run("sudo mv /usr/local/wikixmlj/ /home/ubuntu/")


    files = ssh_client.listdir("/home/ubuntu/")
    # download or update
    if c.UPDATE_SPARK_PERF:
        if "wikixmlj" in files:
            ssh_client.run("""cd $HOME/wikixmlj && git status | grep "up-to-date" || eval `git pull && mvn package install`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/synhershko/wikixmlj.git wikixmlj")
            ssh_client.run(
                "cd $HOME/wikixmlj && mvn package install -Dmaven.test.skip=true && cd $HOME")  # install wikixmlj

        if "spark-perf" in files:
            ssh_client.run("""cd $HOME/spark-perf && git status | grep "up-to-date" || eval `git pull && cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/databricks/spark-perf.git spark-perf")
            ssh_client.run(
                "cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py")

    if c.UPDATE_SPARK_BENCH:
        if "spark-bench" in files:
            ssh_client.run("""cd $HOME/spark-bench && git status | grep "up-to-date" || eval `git pull && sed -i '7s{.*{mvn package -P spark2.0{' $HOME/spark-bench/bin/build-all.sh && $HOME/spark-bench/bin/build-all.sh && cp $HOME/spark-bench/conf/env.sh.template $HOME/spark-bench/conf/env.sh`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/gioenn/spark-bench.git spark-bench")
            ssh_client.run("$HOME/spark-bench/bin/build-all.sh")  # build spark-bench
            ssh_client.run("cp $HOME/spark-bench/conf/env.sh.template $HOME/spark-bench/conf/env.sh")  # copy spark-bench config



    # ssh_client.run("cd $HOME/wikixmlj && mvn package install -Dmaven.test.skip=true && cd $HOME")  # install wikixmlj
    # ssh_client.run("sed -i '7s{.*{mvn package -P spark2.0{' $HOME/spark-bench/bin/build-all.sh")
    # ssh_client.run("$HOME/spark-bench/bin/build-all.sh") # build spark-bench
    # ssh_client.run("cp $HOME/spark-bench/conf/env.sh.template $HOME/spark-bench/conf/env.sh")  # copy spark-bench config
    # ssh_client.run(
    #     "cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py")  # copy spark-perf config

    # copia chiave privata
    if not c.PRIVATE_KEY_NAME in files:
        ssh_client.put(localpath=c.PRIVATE_KEY_PATH, remotepath="/home/ubuntu/" + c.PRIVATE_KEY_NAME)
        ssh_client.run("chmod 400 " + "$HOME/" + c.PRIVATE_KEY_NAME)

    if c.UPDATE_SPARK_MASTER:
        print("   Updating Spark...")
        ssh_client.run(
            """cd /usr/local/spark && git pull && git checkout """ + c.GIT_BRANCH)
        ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean &&  build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")

        '''
        ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean &&  build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")
        '''
    print("   Remove Logs")
    # ssh_client.run("rm " + c.SPARK_HOME + "spark-events/*")
    ssh_client.run("sudo rm " + c.SPARK_HOME + "spark-events/*")

    # TODO Check number of lines in spark-defaults.conf

    # SHUFFLE SERVICE EXTERNAL
    ssh_client.run(
        "sed -i '31s/.*/spark.shuffle.service.enabled {0}/' {1}conf/spark-defaults.conf".format(
            c.ENABLE_EXTERNAL_SHUFFLE, c.SPARK_HOME))

    if current_cluster == 'spark':
        
        '''
        #if benchmark == 'sort_by_key':
        if benchmark == 'scala-sort-by-key':
            c.BENCH_CONF['scala-sort-by-key']['ScaleFactor'] = literal_eval(cfg['sort_by_key']['scale_factor'])[1]
            print('setting ScaleFactor as {}'.format(c.BENCH_CONF['scala-sort-by-key']['ScaleFactor']))
            c.BENCH_CONF['scala-sort-by-key']['num-partitions'] = cfg['sort_by_key']['num_partitions']
            print('setting num-partitions as {}'.format(c.BENCH_CONF['scala-sort-by-key']['num-partitions']))
            c.SCALE_FACTOR = c.BENCH_CONF[c.BENCHMARK_PERF[0]]["ScaleFactor"]
            c.INPUT_RECORD = 200 * 1000 * 1000 * c.SCALE_FACTOR
            c.NUM_TASK = c.SCALE_FACTOR
        '''
        # OFF HEAP
        ssh_client.run(
            "sed -i '32s{.*{spark.memory.offHeap.enabled " + str(
                c.OFF_HEAP) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        ssh_client.run(
            "sed -i '33s{.*{spark.memory.offHeap.size " + str(
                c.OFF_HEAP_BYTES) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        print("   Changing Benchmark settings")
        # DEADLINE LINE 35
        ssh_client.run("sed -i '35s{.*{spark.control.deadline " + str(
            c.DEADLINE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        #print("MAX_EXECUTOR in setup_master: " + str(c.MAX_EXECUTOR))
        # MAX EXECUTOR LINE 39
        ssh_client.run("sed -i '39s{.*{spark.control.maxexecutor " + str(
            c.MAX_EXECUTOR) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        # CORE FOR VM LINE 40
        ssh_client.run("sed -i '40s{.*{spark.control.coreforvm " + str(
            c.CORE_VM) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        # ALPHA LINE 36
        ssh_client.run("sed -i '36s{.*{spark.control.alpha " + str(
            c.ALPHA) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        # BETA line 37
        ssh_client.run("sed -i '37s{.*{spark.control.beta " + str(
            c.BETA) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        # OVERSCALE LINE 38
        ssh_client.run("sed -i '38s{.*{spark.control.overscale " + str(
            c.OVER_SCALE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        # HEURISTIC TYPE LINE 56
        ssh_client.run("sed -i '56s{.*{spark.control.heuristic " + str(
            c.HEURISTIC.value) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        # CORE_ALLOCATION
        if c.CORE_ALLOCATION != None and c.DEADLINE_ALLOCATION != None and c.STAGE_ALLOCATION != None:
            ssh_client.run("sed -i '57s{.*{spark.control.stage " + str(c.STAGE_ALLOCATION) + "{' "+c.SPARK_HOME+"conf/spark-defaults.conf")
            ssh_client.run("sed -i '58s{.*{spark.control.stagecores "+ str(c.CORE_ALLOCATION) +"{' "+c.SPARK_HOME + "conf/spark-defaults.conf")
            ssh_client.run("sed -i '59s{.*{spark.control.stagedeadlines " + str(
                c.DEADLINE_ALLOCATION) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        else:
            ssh_client.run("sed -i '57s{.*{#stage{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
            ssh_client.run("sed -i '58s{.*{#stagecores{' "+c.SPARK_HOME + "conf/spark-defaults.conf")
            ssh_client.run("sed -i '59s{.*{#stagedeadlines{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        # CHANGE ALSO IN MASTER FOR THE LOGS
        ssh_client.run(
            "sed -i '43s{.*{spark.control.tsample " + str(
                c.T_SAMPLE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '42s{.*{spark.control.k " + str(
            c.K ) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '44s{.*{spark.control.ti " + str(
            c.TI) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '45s{.*{spark.control.corequantum " + str(
            c.CORE_QUANTUM) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '46s{.*{spark.locality.wait " + str(
                c.LOCALITY_WAIT) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '51s{.*{spark.locality.wait.node " + str(
                c.LOCALITY_WAIT_NODE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '52s{.*{spark.locality.wait.process " + str(
                c.LOCALITY_WAIT_PROCESS) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '53s{.*{spark.locality.wait.rack " + str(
                c.LOCALITY_WAIT_RACK) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '47s{.*{spark.task.cpus " + str(
                c.CPU_TASK) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '48s{.*{spark.control.nominalrate 0.0{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        ssh_client.run(
            "sed -i '49s{.*{spark.control.nominalratedata 0.0{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '50s{.*{spark.control.coremin " + str(
            c.CORE_MIN) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '54s{.*{spark.control.inputrecord " + str(
                c.INPUT_RECORD) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        ssh_client.run(
            "sed -i '55s{.*{spark.control.numtask " + str(
                c.NUM_TASK) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("""sed -i '3s{.*{master=""" + master_private_ip +
                       """{' ./spark-bench/conf/env.sh""")
        ssh_client.run("""sed -i '63s{.*{NUM_TRIALS=""" + str(c.BENCH_NUM_TRIALS) +
                       """{' ./spark-bench/conf/env.sh""")

        # CHANGE SPARK HOME DIR
        ssh_client.run("sed -i '21s{.*{SPARK_HOME_DIR = \"" + c.SPARK_HOME + "\"{' ./spark-perf/config/config.py")

        # CHANGE MASTER ADDRESS IN BENCHMARK
        ssh_client.run("""sed -i '30s{.*{SPARK_CLUSTER_URL = "spark://""" + master_private_ip +
                       """:7077"{' ./spark-perf/config/config.py""")

        # CHANGE SCALE FACTOR LINE 127
        ssh_client.run(
            "sed -i '127s{.*{SCALE_FACTOR = " + str(c.SCALE_FACTOR) + "{' ./spark-perf/config/config.py")

        # NO PROMPT
        ssh_client.run("sed -i '103s{.*{PROMPT_FOR_DELETES = False{' ./spark-perf/config/config.py")
        if len(c.BENCHMARK_PERF) > 0:  # and c.SPARK_PERF_FOLDER == "spark-perf-gioenn":
            # print("   Setting up skewed test")
            # ssh_client.run("""sed -i '164s{.*{OptionSet("skew", [""" + str(
            #     c.BENCH_CONF[c.BENCHMARK_PERF[0]]["skew"]) + """]){' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            print("   Setting up unique-keys, num-partitions and reduce-tasks")
            ssh_client.run("""sed -i '185s{.*{OptionSet("unique-keys",[""" + str(c.BENCH_CONF[c.BENCHMARK_PERF[0]][
                                                                                     "unique-keys"]) + """], False),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            ssh_client.run("""sed -i '170s{.*{OptionSet("num-partitions", [""" + str(c.BENCH_CONF[c.BENCHMARK_PERF[0]][
                                                                                         "num-partitions"]) + """], can_scale=False),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            ssh_client.run("""sed -i '172s{.*{OptionSet("reduce-tasks", [""" + str(c.BENCH_CONF[c.BENCHMARK_PERF[0]][
                                                                                       "reduce-tasks"]) + """], can_scale=False),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")

        # CHANGE RAM EXEC
        ssh_client.run(
            """sed -i '146s{.*{    JavaOptionSet("spark.executor.memory", [""" + c.RAM_EXEC + """]),{' ./spark-perf/config/config.py""")

        ssh_client.run(
            """sed -i '55s{.*{SPARK_EXECUTOR_MEMORY=""" + c.RAM_EXEC + """{' ./spark-bench/conf/env.sh""")

        # CHANGE RAM DRIVER
        ssh_client.run(
            "sed -i '26s{.*{spark.driver.memory " + c.RAM_DRIVER + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '154s{.*{SPARK_DRIVER_MEMORY = \""+c.RAM_DRIVER+"\"{' ./spark-perf/config/config.py")


        print("   Enabling/Disabling Benchmark")
        # ENABLE BENCHMARK
        for bench in c.BENCHMARK_PERF:
            for line_number in c.BENCH_LINES[bench]:
                sed_command_line = "sed -i '" + line_number + " s/[#]//g' ./spark-perf/config/config.py"
                ssh_client.run(sed_command_line)

        # DISABLE BENCHMARK
        for bench in c.BENCH_LINES:
            if bench not in c.BENCHMARK_PERF:
                for line_number in c.BENCH_LINES[bench]:
                    ssh_client.run("sed -i '" + line_number + " s/^/#/' ./spark-perf/config/config.py")

        # ENABLE HDFS
        # if c.HDFS:
        print("   Enabling HDFS in benchmarks")
        ssh_client.run("sed -i '179s%memory%hdfs%g' ./spark-perf/config/config.py")

    #if hdfs_master != "":
        ssh_client.run(
            """sed -i  '50s%.*%HDFS_URL = "hdfs://{0}:9000/test/"%' ./spark-perf/config/config.py""".format(
                hdfs_master))
        ssh_client.run(
            """sed -i  '10s%.*%HDFS_URL="hdfs://{0}:9000"%' ./spark-bench/conf/env.sh""".format(
                hdfs_master))
        ssh_client.run(
            """sed -i  '14s%.*%DATA_HDFS="hdfs://{0}:9000/SparkBench"%' ./spark-bench/conf/env.sh""".format(
                hdfs_master))

        # TODO: additional settings for spark-bench
        # ssh_client.run("""sed -i '8s/.*//' ./spark-bench/conf/env.sh""")
        ssh_client.run("""sed -i '8s{.*{[ -z "$HADOOP_HOME" ] \&\&     export HADOOP_HOME="""+c.HADOOP_HOME+"""{' ./spark-bench/conf/env.sh""")
        # slaves = slaves_ip[0]
        # for slave in slaves_ip[1:]:
        #     slaves = slaves + ", " + slave

        ssh_client.run(
            """sed -i  '5s%.*%MC_LIST=()%' ./spark-bench/conf/env.sh""")

        ssh_client.run(
            """sed -i  '19s%.*%SPARK_VERSION=2.0%' ./spark-bench/conf/env.sh""")

        # ssh_client.run("""sed -i '20s/.*//' ./spark-bench/conf/env.sh""")
        ssh_client.run("""sed -i '20s{.*{[ -z "$SPARK_HOME" ] \&\&     export SPARK_HOME="""+c.SPARK_HOME+"""{' ./spark-bench/conf/env.sh""")

    # START MASTER and HISTORY SERVER
    if current_cluster == 'spark':
        print("   Starting Spark Master")
        ssh_client.run(
            'export SPARK_HOME="{d}" && {d}sbin/start-master.sh -h {0}'.format(
                master_private_ip, d=c.SPARK_HOME))
        print("   Starting Spark History Server")
        ssh_client.run(
            'export SPARK_HOME="{d}" && {d}sbin/start-history-server.sh'.format(d=c.SPARK_HOME))
        
    # SETUP CSPARKWORK MASTER FOR PROCESSING LOGS ON SERVER
    # To be changed: generalize private and public key paths references (currently the Azure ones)
    with open_cfg() as cfg:
        tool_on_master = cfg.getboolean('main', 'tool_on_master')
    if current_cluster == 'spark' and c.PROCESS_ON_SERVER: #vboxvm
        if not tool_on_master:
            if c.INSTALL_PYTHON3 == 1:
                stdout, stderr, status = ssh_client.run("sudo apt-get update && sudo apt-get install -y python3-pip && " +
                                                        "sudo apt-get build-dep -y matplotlib && "+
                                                        "sudo apt-get install -y build-essential libssl-dev libffi-dev python-dev python3-dev && "+
                                                        #"sudo apt-get install pkg-config" +             #vboxvm? (double-check)
                                                        #"sudo apt-get install libfreetype6-dev" +       #vboxvm? (double-check)
                                                        "sudo pip3 install --upgrade setuptools")
                """Install python3 on cSpark master"""
                print("Installing Python3 on cspark master:\n" + stdout)
            stdout, stderr, status = ssh_client.run("sudo rm -r /home/ubuntu/xSpark-bench")
            stdout, stderr, status = ssh_client.run("git clone -b final-cloud --single-branch " +
                                                    "https://github.com/DavideB/xSpark-bench.git /home/ubuntu/xSpark-bench")
            #stdout, stderr, status = ssh_client.run("git clone -b process-on-server --single-branch " +
            #                                        "https://github.com/DavideB/xSpark-bench.git /home/ubuntu/xSpark-bench")
            #                                        "https://github.com/gioenn/xSpark-bench.git /home/ubuntu/xSpark-bench")
            """Clone xSpark-benchmark on cspark master"""
            print("Cloning xSpark-benchmark tool on cspark master:\n" + stdout)
        
            if not "id_rsa" in ssh_client.listdir("/home/ubuntu/"):
                ssh_client.put(localpath=c.PRIVATE_KEY_PATH, remotepath="/home/ubuntu/id_rsa")
                ssh_client.run("chmod 400 /home/ubuntu/id_rsa")
            """upload private key to home directory"""
            
            if not "id_rsa.pub" in ssh_client.listdir("/home/ubuntu/"):
                ssh_client.put(localpath=c.AZ_PUB_KEY_PATH, remotepath="/home/ubuntu/id_rsa.pub")
                ssh_client.run("chmod 400 /home/ubuntu/id_rsa.pub")
            """upload public key to home directory"""
            
            #ssh_client.run("sudo rm /home/ubuntu/xSpark-bench/credentials.py")
            #ssh_client.put(localpath="credentials.py", remotepath="/home/ubuntu/xSpark-bench/credentials.py")
            """upload credentials.py"""
            
            #ssh_client.run("sudo rm /home/ubuntu/xSpark-bench/config.py")
            #ssh_client.put(localpath="config.py", remotepath="/home/ubuntu/xSpark-bench/config.py")
            """upLoad config.py"""
            
            ssh_client.run("sudo rm /home/ubuntu/xSpark-bench/configure.py")
            ssh_client.put(localpath="configure.py", remotepath="/home/ubuntu/xSpark-bench/configure.py")
            """upLoad configure.py"""
            
            credentials_file = Path("credentials.json")
            if credentials_file.exists():
                credentials = json.load(open("credentials.json"))
                try:
                    credentials["AzPubKeyPath"] = "/home/ubuntu/" + credentials["AzPubKeyPath"].split("/")[-1]
                    credentials["AzPrvKeyPath"] = "/home/ubuntu/" + credentials["AzPrvKeyPath"].split("/")[-1]
                    credentials["KeyPairPath"] = "/home/ubuntu/" + credentials["KeyPairPath"].split("/")[-1]
                except KeyError:
                    print("key error in file credentials.json\n")
                with open("credentials_on_server.json", "w") as outfile:
                    json.dump(credentials, outfile, indent=4, sort_keys=True)
            ssh_client.run("sudo rm /home/ubuntu/xSpark-bench/credentials.json")
            ssh_client.put(localpath="credentials_on_server.json", remotepath="/home/ubuntu/xSpark-bench/credentials.json")
            """upLoad credentials.json"""
            
            credentials_on_server_file = Path("credentials_on_server.json")
            if credentials_on_server_file.exists():
                os.remove("credentials_on_server.json")
            
            ssh_client.run("sudo rm /home/ubuntu/setup.json")
            ssh_client.put(localpath="setup.json", remotepath="/home/ubuntu/xSpark-bench/setup.json")
            """upLoad setup.json"""
            
            ssh_client.run("sudo rm /home/ubuntu/control.json")
            ssh_client.put(localpath="control.json", remotepath="/home/ubuntu/xSpark-bench/control.json")
            """upLoad control.json"""
            
            #ssh_client.run("sudo rm /home/ubuntu/experiment.json")
            #ssh_client.put(localpath="experiment.json", remotepath="/home/ubuntu/xSpark-bench/experiment.json")
            """upLoad experiment.json"""
            
            # update cfg_clusters.ini before uploading it to master
            with open_cfg(mode='w') as cfg:
                cfg['main']['tool_on_master'] = 'true'
                write_cfg(cfg)
                #tool_on_master = True
            
            #ssh_client.run("sudo rm /home/ubuntu/cfg_clusters.ini")
            #ssh_client.put(localpath="cfg_clusters.ini", remotepath="/home/ubuntu/xSpark-bench/cfg_clusters.ini")
            #"""upLoad cfg_clusters.ini"""
    
            '''
            stdout, stderr, status = ssh_client.run("""sed -i -r 's{^KEY_PAIR_PATH( *= *["'"'"'].*["'"'"']) *\+ *{KEY_PAIR_PATH = \"/home/ubuntu/\" + {' /home/ubuntu/xSpark-bench/config.py""")
            stdout, stderr, status = ssh_client.run("""sed -i -r 's{^AZ_PRV_KEY_PATH( *= *["'"'"'].*["'"'"']) *\+ *{AZ_PRV_KEY_PATH = \"/home/ubuntu/\" + {' /home/ubuntu/xSpark-bench/credentials.py""")
            stdout, stderr, status = ssh_client.run("""sed -i -r 's{^AZ_PUB_KEY_PATH( *= *["'"'"'].*["'"'"']) *\+ *{AZ_PUB_KEY_PATH = \"/home/ubuntu/\" + {' /home/ubuntu/xSpark-bench/credentials.py""")
            stdout, stderr, status = ssh_client.run("cat /home/ubuntu/xSpark-bench/credentials.py | grep -e ^AZ_PRV_KEY_PATH -e ^AZ_PUB_KEY_PATH")
            print("Re-defining private and public key paths in credentials.py\n" + stdout)
            stdout, stderr, status = ssh_client.run("cat /home/ubuntu/xSpark-bench/config.py | grep -e ^KEY_PAIR_PATH")
            print("Re-defining private and public key paths in config.py\n" + stdout)
            """Re-define private and public key paths"""
            '''
            stdout, stderr, status = ssh_client.run("cd /home/ubuntu/xSpark-bench && " +
                                                    "sudo pip3 install -r requirements.txt")
            """Install xSpark-benchmark tool requirements"""
            print("Installing xSpark-benchmark tool requirements\n" + stdout)    
        
        ssh_client.run("sudo rm /home/ubuntu/xSpark-bench/cfg_clusters.ini")
        ssh_client.put(localpath="cfg_clusters.ini", remotepath="/home/ubuntu/xSpark-bench/cfg_clusters.ini")
        """upLoad cfg_clusters.ini"""
    
    return master_private_ip, node


@timing
def setup_hdfs_ssd(node, hdfs_master):
    """

    :param node:
    :return:
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    ssh_client = sshclient_from_node(node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')

    out, err, status = ssh_client.run(
        """test -d /mnt/hdfs/namenode || sudo mkdir --parents /mnt/hdfs/namenode &&
        sudo mkdir --parents /mnt/hdfs/datanode""")
    if status != 0:
        print(out, err)
    # if c.PROVIDER == "AWS_SPOT":
    #     ssh_client.run("sudo chown ubuntu:hadoop /mnt/hdfs && sudo chown ubuntu:hadoop /mnt/hdfs/*")
    # elif c.PROVIDER == "AZURE":
    ssh_client.run("sudo chown ubuntu:ubuntu /mnt/hdfs && sudo chown ubuntu:ubuntu /mnt/hdfs/*")
    if delete_hdfs or hdfs_master == "":
        # ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("sudo rm /mnt/hdfs/datanode/current/VERSION")


def rsync_folder(ssh_client, slave):
    """

    :param ssh_client:
    :param slave:
    :return:
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    ssh_client.run(
        "eval `ssh-agent -s` && ssh-add " + "$HOME/" + c.PRIVATE_KEY_NAME + " && rsync -a " + c.HADOOP_CONF + " ubuntu@" + slave + ":" + c.HADOOP_CONF)
    if delete_hdfs:
        # ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("sudo rm /mnt/hdfs/datanode/current/VERSION")


@timing
def setup_hdfs_config(master_node, slaves, hdfs_master):
    """

    :param master_node:
    :param slaves:
    :return:
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    ssh_client = sshclient_from_node(master_node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')

    if hdfs_master == "":
        master_ip = get_ip(master_node)
    else:
        master_ip = hdfs_master

    # Setup Config HDFS
    ssh_client.run(
        "sed -i '19s%.*%<configuration>  <property>    <name>fs.default.name</name>    <value>hdfs://" + master_ip + ":9000</value>  </property>%g' " + c.HADOOP_CONF + "core-site.xml")
    # 19 <configuration>  <property>    <name>fs.default.name</name>    <value>hdfs://ec2-54-70-105-139.us-west-2.compute.amazonaws.com:9000</value>  </property>

    ssh_client.run(
        "sed -i '38s%.*%<value>" + master_ip + ":50070</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '43s%.*%<value>" + master_ip + ":50090</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '48s%.*%<value>" + master_ip + ":9000</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
    # 38  <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:50070</value>
    # 43   <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:50090</value>
    # 48  <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:9000</value>

    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/namenode%/mnt/hdfs/namenode%g' " + c.HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/datanode%/mnt/hdfs/datanode%g' " + c.HADOOP_CONF + "hdfs-site.xml")

    print(slaves)
    ssh_client.run("echo -e '" + "\n".join(slaves) + "' > " + c.HADOOP_CONF + "slaves")

    ssh_client.run(
        "echo 'Host *\n  UserKnownHostsFile /dev/null\n  StrictHostKeyChecking no' > ~/.ssh/config")

    # Rsync Config
    with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
        for slave in slaves:
            executor.submit(rsync_folder, ssh_client, slave)

    # Start HDFS
    if delete_hdfs or hdfs_master == "":
        ssh_client.run(
            "eval `ssh-agent -s` && ssh-add " + "$HOME/" + c.PRIVATE_KEY_NAME + " && /usr/local/lib/hadoop-2.7.2/sbin/stop-dfs.sh")
        # ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("sudo rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("echo 'N' | /usr/local/lib/hadoop-2.7.2/bin/hdfs namenode -format")

    out, err, status = ssh_client.run(
        "eval `ssh-agent -s` && ssh-add " + "$HOME/" + c.PRIVATE_KEY_NAME + " && /usr/local/lib/hadoop-2.7.2/sbin/start-dfs.sh && /usr/local/lib/hadoop-2.7.2/bin/hdfs dfsadmin -safemode leave")
    if status != 0:
        print(out, err)
    print("   Started HDFS")

    if delete_hdfs:
        print("   Cleaned HDFS")
        if len(c.BENCHMARK_PERF) > 0:
            out, err, status = ssh_client.run(
                "/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
            print(out, err, status)


def write_config(output_folder):
    """

    :param output_folder:
    :return:
    """
    with open(output_folder + "/config.json", "w") as config_out:
        json.dump(c.CONFIG_DICT, config_out, sort_keys=True, indent=4)


def check_slave_connected_master(ssh_client):
    """

    :param ssh_client:
    :return:
    """
    pass

def upload_profile_to_master(nodes, profile_fname, localfilepath):
    """

    :param nodes, profile_fname, localfilepath:
    :return:
    """
    ssh_client = sshclient_from_node(nodes[0], ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')  #vboxvm_removed
    #master_public_ip = socket.gethostbyname("XSPARKWORK0") #vboxvm
    #ssh_client = sshclient_from_ip(master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm
    print("Uploading benchmark profile: " + profile_fname + "\n")
    if not profile_fname in ssh_client.listdir(c.C_SPARK_HOME + "conf/"):
        ssh_client.put(localpath=localfilepath, remotepath=c.C_SPARK_HOME + "conf/" + profile_fname)
        ssh_client.run("chmod 664 " + c.C_SPARK_HOME + "conf/" + profile_fname)
        ssh_client.run("sudo chown ubuntu:ubuntu " + c.C_SPARK_HOME + "conf/" + profile_fname)
        print("Benchmark profile successfully uploaded\n") 
        """upload profile to spark conf directory"""
    else:
        print("Not uploaded: a benchmark profile with the same name already exists\n")

@timing
def run_benchmark(nodes):
    """

    :return:
    """
    if len(nodes) == 0:
        print("No instances running")
        exit(1)
    
    profile = False
    profile_fname = ""
    profile_option = False
    
    with open_cfg(mode='w') as cfg:
        current_cluster = cfg['main']['current_cluster']
        c.CLUSTER_ID = c.CLUSTER_MAP[current_cluster]
        benchmark = cfg['main']['benchmark'] if 'main' in cfg and 'benchmark' in cfg['main'] else \
                    cfg['experiment']['benchmarkname'] if 'experiment' in cfg and 'benchmarkname' in cfg['experiment'] else ''
        hdfs_master_private_ip = cfg['hdfs']['master_private_ip'] if 'hdfs' in cfg and 'master_private_ip' in cfg['hdfs'] else ''
        c.HDFS_MASTER = hdfs_master_private_ip
        hdfs_master_public_ip = cfg['hdfs']['master_public_ip'] if 'hdfs' in cfg and 'master_public_ip' in cfg['hdfs'] else ''
        delete_hdfs = c.DELETE_HDFS = cfg.getboolean('main', 'delete_hdfs')
        max_executors = int(cfg['main']['max_executors']) if 'main' in cfg and 'max_executors' in cfg['main'] else len(nodes) - 1
        # print('HDFS_MASTER from clusters.ini: ' + hdfs_master)
        end_index = min(len(nodes), max_executors + 1)
        cfg['main']['max_executors'] = str(end_index - 1)
        # TODO: pass slaves ip
        slaves_ip = [get_ip(i) for i in nodes[1:end_index]]
        iter_num = cfg['main']['iter_num'] #vboxvm
        c.MAX_EXECUTOR = end_index - 1
        c.cfg_dict["MaxExecutor"] = c.CONFIG_DICT["Control"]["MaxExecutor"] = c.MAX_EXECUTOR
        #c.CONFIG_DICT["Control"]["MaxExecutor"] = c.MAX_EXECUTOR
        #c.cfg_dict["ConfigDict"] = c.CONFIG_DICT
        if 'main' in cfg and 'benchmark' in cfg['main']:
            c.config_benchmark(benchmark, cfg)
        if 'main' in cfg and 'experiment_file' in cfg['main']:
            exp_filepath = cfg['main']['experiment_file']
            c.config_experiment(exp_filepath, cfg)  
        profile = True if 'profile' in cfg else False
        print("profile mode set to " + str(profile))
        profile_option = cfg.getboolean('main', 'profile') if 'main' in cfg and 'profile' in cfg['main'] else False
        #profile = cfg['main']['profile'] if 'main' in cfg and 'profile' in cfg['main'] else False
        profile_fname = cfg[benchmark]['profile_name'] if profile and \
                                                          benchmark in cfg and \
                                                          'profile_name' in cfg[benchmark] \
                                                       else ""
        print("profile filename set to: " + profile_fname)
        
        #c.SPARK_HOME = c.C_SPARK_HOME
        if profile:
            c.SPARK_HOME = c.SPARK_SEQ_HOME if cfg.getboolean('profile', 'spark_seq') else c.SPARK_2_HOME
        
        c.update_config_parms(c)   
        
    #master_public_ip = socket.gethostbyname("XSPARKWORK0") #vboxvm
    #ssh_client = sshclient_from_ip(master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm
    # following are debug lines to be removed
    with open("cfg_dict.json", "w") as f:
                json.dump(c.cfg_dict, f, indent=4, sort_keys=True)
    #exit(1)
    #end debug lines
    master_ip, master_node = setup_master(nodes[0], slaves_ip, hdfs_master_private_ip)
    
    print("MASTER: " + master_ip)   #vboxvm remove
    ssh_client = sshclient_from_node(master_node, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm remove
    
    if profile and profile_fname != "":
        # delete selected benchmark profile file
        print("Deleting " + benchmark + " benchmark reference profile: " + profile_fname + ".json\n")
        stdout, stderr, status = ssh_client.run("sudo rm " + c.C_SPARK_HOME + "conf/" + profile_fname + ".json")
        print(stdout + stderr) 
          
    if c.SPARK_HOME == c.SPARK_2_HOME:
        print("Check Effectively Executor Running")
    #vboxvm_removed
    count = 1
    with ThreadPoolExecutor(8) as executor:
        for i in nodes[1:end_index]:
            ip = get_ip(i)
            if ip != master_ip:
                executor.submit(setup_slave, i, master_ip, count)
            count += 1

    with ThreadPoolExecutor(8) as executor:
        for i in nodes[end_index:]:
            ip = get_ip(i)
            if ip != master_ip:
                ssh_client = sshclient_from_node(i, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')

                executor.submit(common_setup, ssh_client)

    if current_cluster == 'hdfs' or hdfs_master_private_ip == master_ip:
        print("\nStarting Setup of HDFS cluster")
        # Format instance store SSD for hdfs usage
        with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            for i in nodes:
                executor.submit(setup_hdfs_ssd, i, hdfs_master_private_ip)

        slaves = [get_ip(i) for i in nodes[:end_index]]
        slaves.remove(master_ip)
        setup_hdfs_config(master_node, slaves, hdfs_master_private_ip)
        with open_cfg(mode='w') as cfg:
            count = 1
            for ip in slaves:
                cfg['hdfs']['slave'+str(count)+'_ip'] = ip
                count += 1

    time.sleep(15)

    print("MASTER: " + master_ip)   #vboxvm add
    ssh_client = sshclient_from_node(master_node, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm add

    #  CHECK IF KEY IN MASTER

    # SPOSTATO IN MASTER_SETUP
    # out, err, status = ssh_client.run('[ ! -e %s ]; echo $?' % c.PRIVATE_KEY_NAME)
    # if not int(status):
    # files = ssh_client.listdir("/home/ubuntu/")
    # if not c.PRIVATE_KEY_NAME in files:
    #     ssh_client.put(localpath=c.PRIVATE_KEY_PATH, remotepath="/home/ubuntu/" + c.PRIVATE_KEY_NAME)
    #     ssh_client.run("chmod 400 " + "$HOME/" + c.PRIVATE_KEY_NAME)
    #vboxvm_removed
    # LANCIARE BENCHMARK
    if current_cluster == 'spark':
        #with open_cfg() as cfg:
        ''' 
        with open_cfg(mode='w') as cfg:

            if 'main' in cfg and 'benchmark' in cfg['main']:
                c.config_benchmark(benchmark, cfg)

                if benchmark == 'pagerank':
                    c.BENCH_CONF["PageRank"]["numV"] = literal_eval(cfg['pagerank']['num_v'])
                    num_partitions = cfg['pagerank']['num_partitions']
                    c.BENCH_CONF["PageRank"]["NUM_OF_PARTITIONS"] = num_partitions
                    print('setting numV as {}'.format(c.BENCH_CONF["PageRank"]["numV"]))
                    print('setting NUM_OF_PARTITIONS as {}'.format(c.BENCH_CONF["PageRank"]["NUM_OF_PARTITIONS"]))
        
                if benchmark == 'kmeans':
                    c.BENCH_CONF["KMeans"]["NUM_OF_POINTS"] = literal_eval(cfg['kmeans']['num_of_points'])
                    num_partitions = cfg['kmeans']['num_partitions']
                    c.BENCH_CONF["KMeans"]["NUM_OF_PARTITIONS"] = num_partitions
                    print('setting NUM_OF_POINTS as {}'.format(c.BENCH_CONF["KMeans"]["NUM_OF_POINTS"]))
                    print('setting NUM_OF_PARTITIONS as {}'.format(c.BENCH_CONF["KMeans"]["NUM_OF_PARTITIONS"]))

            if 'main' in cfg and 'experiment_file' in cfg['main']:
                exp_filepath = cfg['main']['experiment_file']
                c.config_experiment(exp_filepath, cfg)  
            
            
            if 'main' in cfg and 'experiment_file' in cfg['main']:    
                exp_filepath = cfg['main']['experiment_file'] if 'experiment_file' in cfg['main'] else ''
                config_experiment(exp_filepath, cfg)
                cfg['experiment'] = {}
                cfg['experiment']['ReuseDataset'] = str(c.PREV_SCALE_FACTOR)
                cfg['experiment']['Deadline'] = str(c.DEADLINE)
                cfg['experiment']['BenchmarkName'] = str((c.BENCHMARK_BENCH + c.BENCHMARK_PERF)[0])
                cfg['BenchmarkConf'] = {}
                #cfg['experiment']['SyncTime'] = str(c.SYNC_TIME)
                #cfg['experiment']['BenchNumTrials'] = str(c.BENCH_NUM_TRIALS)
                #cfg['experiment']['BenchmarkmPerf'] = str(c.BENCHMARK_PERF)
                #cfg['experiment']['BenchmarkConf'] = str(c.BENCH_CONF)
                #for key in c.BENCH_CONF[cfg['experiment']['BenchmarkName']]:
                #    cfg['BenchmarkConf'][key] = str(c.BENCH_CONF[cfg['experiment']['BenchmarkName']][key])
                
            else:
            
        '''
                
        if len(c.BENCHMARK_PERF) > 0:
            if delete_hdfs:
                print("   Cleaning HDFS...")
                print("connecting to hdfs_master:{}".format(hdfs_master_public_ip))
                ssh_client_hdfs = sshclient_from_ip(hdfs_master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu')
                out, err, status = ssh_client_hdfs.run(
                    "/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
                print(out, err, status)
            print("Running Benchmark " + str(c.BENCHMARK_PERF))
            runout, runerr, runstatus = ssh_client.run(
                'export SPARK_HOME="' + c.SPARK_HOME + '" && ./spark-perf/bin/run')
            print('runout\n{}\nrunerr:\n{}\runstatus:{}'.format(runout, runerr, runstatus))

            # FIND APP LOG FOLDER
            print("Finding log folder")
            app_log = between(runout, "2>> ", ".err")
            logfolder = "/home/ubuntu/" + "/".join(app_log.split("/")[:-1])
            print(logfolder)
            output_folder = logfolder[1:]
        '''    
        if  profile and profile_fname != "" :
            # delete selected benchmark profile file
            print("Deleting " + benchmark + " benchmark reference profile: " + profile_fname + "\n")
            stdout, stderr, status = ssh_client.run("sudo rm " + c.SPARK_HOME + "conf/" + profile_fname + ".json")
        '''                

        for bench in c.BENCHMARK_BENCH:

            for bc in c.BENCH_CONF[bench]:
                if bc != "NumTrials":
                    ssh_client.run(
                        "sed -i -r 's/ *{0} *= *.*/{0}={1}/' ./spark-bench/{2}/conf/env.sh""".format(
                            bc, c.BENCH_CONF[bench][bc], bench))
                        #"sed -i '{0}s/.*/{1}={2}/' ./spark-bench/{3}/conf/env.sh""".format(
                        #    c.BENCH_CONF[bench][bc][0], bc, c.BENCH_CONF[bench][bc][1],
                        #    bench))

            if delete_hdfs:
                print("Generating Data Benchmark " + bench)
                ssh_client.run(
                    'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + c.PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + c.SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/gen_data.sh')

            check_slave_connected_master(ssh_client) #vboxvm_removed
            print("Running Benchmark " + bench)
        
        #vboxvm
        '''
        sess_file = Path("session.txt")
        session_no = 0
        if sess_file.exists():
            with open("session.txt", 'r') as f:
                fc = f.read()
                session_no = int(fc)
                f.close()
        '''                
        logfolder = "/home/ubuntu/spark-bench/num"
        output_folder = "home/ubuntu/spark-bench/num/"
        stdout, stderr, status = ssh_client.run("sudo rm -r " + logfolder + "*")
        ssh_client.run(                                                                                                                                                  #vboxvm_removed
           'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + c.PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + c.SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/run.sh') #vboxvm_removed
        # logfolder = "/home/ubuntu/spark-bench/num"
        # output_folder = "home/ubuntu/spark-bench/num/"
        # ensure there is no directory named 'old' in log folder
        stdout, stderr, status = ssh_client.run("cd "+ logfolder + " && sudo rm -r old") 
        #stdout, stderr, status = ssh_client.run("cd "+ c.CONFIG_DICT["Spark"]["SparkHome"] + "spark-events/ && sudo cp -a " +
        #                                         c.CONFIG_DICT["Spark"]["SparkHome"] + "event-logs/. ./ && sudo rename 's/$/_"+ str(session_no) + '_' + iter_num +"/' *") #vboxvm
        stdout, stderr, status = ssh_client.run("sudo chown ubuntu:ubuntu " + c.CONFIG_DICT["Spark"]["SparkHome"] + "conf")#check if needed
        #stdout, stderr, status = ssh_client.run("sudo chown ubuntu:ubuntu /usr/local/spark/conf")#check if needed
        #print("sudo chown ubuntu:ubuntu /usr/local/spark/conf:" +stdout+stderr)
        
        # RODO: DOWNLOAD LOGS
        if c.PROCESS_ON_SERVER:
            #master_public_ip = socket.gethostbyname("XSPARKWORK0") #vboxvm
            #ssh_client = sshclient_from_ip(master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm
            for file in os.listdir("./"):
                if file.endswith(".pickle"):
                    os.remove(file)
            stdout, stderr, status = ssh_client.run("cd xSpark-bench && sudo rm  *.pickle")
            with open('nodes_ids.pickle', 'wb') as f:
                pickle.dump([n.id for n in [i for i in nodes[:end_index]]], f)       
            ssh_client.put(localpath="nodes_ids.pickle", remotepath="/home/ubuntu/xSpark-bench/nodes_ids.pickle")
            with open('logfolder.pickle', 'wb') as f:
                pickle.dump(logfolder, f)       
            ssh_client.put(localpath="logfolder.pickle", remotepath="/home/ubuntu/xSpark-bench/logfolder.pickle")
            with open('output_folder.pickle', 'wb') as f:
                pickle.dump(output_folder, f)       
            ssh_client.put(localpath="output_folder.pickle", remotepath="/home/ubuntu/xSpark-bench/output_folder.pickle")
            with open('master_ip.pickle', 'wb') as f:
                pickle.dump(master_ip, f)       
            ssh_client.put(localpath="master_ip.pickle", remotepath="/home/ubuntu/xSpark-bench/master_ip.pickle")
            
            #with open('config_instance.pickle', 'wb') as f:
            #    pickle.dump(c, f)       
            #ssh_client.put(localpath="config_instance.pickle", remotepath="/home/ubuntu/xSpark-bench/config_instance.pickle") 
            
            with open("cfg_dict.json", "w") as f:
                json.dump(c.cfg_dict, f, indent=4, sort_keys=True)
            ssh_client.put(localpath="cfg_dict.json", remotepath="/home/ubuntu/xSpark-bench/cfg_dict.json")
                          
            stdout, stderr, status = ssh_client.run("cd xSpark-bench && sudo python3 process_on_server.py")
            print("Processing on server:\n" + stdout + stderr)
            master_node = [i for i in nodes if get_ip(i) == master_ip][0] #vboxvm remove
            print("Downloading results from server...")
            for dir in ssh_client.listdir("xSpark-bench/home/ubuntu/spark-bench/num/"):
                print("folder: " + str(dir))
                try:
                    os.makedirs(output_folder + dir)
                    for file in ssh_client.listdir("xSpark-bench/home/ubuntu/spark-bench/num/" + dir + "/"):
                        output_file = (output_folder + dir + "/" + file)
                        print("file: " + output_file)
                        ssh_client.get(remotepath="xSpark-bench/" + output_file, localpath=output_file)
                except FileExistsError:
                    print("Output folder already exists: no files downloaded")
            print("Updating local copy of cfg_clusters.ini")
            ssh_client.get(remotepath="xSpark-bench/cfg_clusters.ini", localpath="cfg_clusters.ini")
            #for dir in ssh_client.listdir("xSpark-bench/spark_log_profiling/"):
            #for dir in ["avg_json", "input_logs", "output_json", "processed_logs", "processed_profiles"]: 
            with open_cfg() as cfg:
                if profile and 'main' in cfg and 'iter_num' in cfg['main'] \
                           and 'num_run' in cfg['main'] and 'benchmarkname' in cfg['experiment'] \
                           and 'experiment_num' in cfg['main'] and 'num_experiments' in cfg['main'] \
                           and int(cfg['main']['iter_num']) == int(cfg['main']['num_run']) \
                           or profile_option:
                           #and int(cfg['main']['experiment_num']) == int(cfg['main']['num_experiments']) \
                           #or profile_option:
                    for dir in ["avg_json", "input_logs", "output_json", "processed_profiles"]: # do not download logfiles in "processed_logs" folder
                        print("folder: " + dir)
                        make_sure_path_exists("spark_log_profiling/" + dir)
                        for file in ssh_client.listdir("xSpark-bench/spark_log_profiling/" + dir + "/"):
                            fpath = "spark_log_profiling/" + dir + "/"
                            if file not in os.listdir(fpath) or dir == "avg_json":
                                ssh_client.get(remotepath="xSpark-bench/" + fpath + file, localpath=fpath + file)
                                print("Remote file: ~/xSpark-bench/" + fpath + file + " downloaded")
                            else:
                                print("File " + fpath + file + " already exists: not downloaded")
        else:    
            output_folder = log.download(logfolder, [i for i in nodes[:end_index]], master_ip,
                                         output_folder, c.CONFIG_DICT)
            if profile:                                                                      # Profiling
                processing.main()                                                                                       # Profiling
                for filename in os.listdir('./spark_log_profiling/output_json/'):                                       # Profiling
                    if output_folder.split("/")[-1].split("-")[-1] in filename:                                         # Profiling
                        shutil.copy('./spark_log_profiling/output_json/' + filename, output_folder + "/" + filename)    # Profiling
            print("Saving output folder {}".format(os.path.abspath(output_folder)))
            with open_cfg(mode='w') as cfg:
                cfg['out_folders']['output_folder_'+str(len(cfg['out_folders']))] = os.path.abspath(output_folder)
                # Saving cfg on project home directory and output folder
                write_cfg(cfg)
                write_cfg(cfg, output_folder)
    
            if not profile:
                # PLOT LOGS
                plot.plot(output_folder + "/")
                # COMPUTE METRICS
                metrics.compute_metrics(output_folder + "/")

        print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")
        