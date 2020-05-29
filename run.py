"""
This module handles the configuration of the instances and the execution of the app_name on the cluster
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
from configure import config_instance as c
from util.utils import timing, between, get_cfg, write_cfg, open_cfg, make_sure_path_exists
from util.ssh_client import sshclient_from_node, sshclient_from_ip
from pathlib import Path
import socket
import pprint
pp = pprint.PrettyPrinter(indent=4)

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
        if c.NUM_INSTANCE > 0 or True:
            print("In common_setup, NUM_INSTANCE=" + str(c.NUM_INSTANCE))
            # add ssh key that matches the public one used during creation
            if not c.PRIVATE_KEY_NAME in ssh_client.listdir("/home/ubuntu/.ssh/"):
                ssh_client.put(localpath=c.PRIVATE_KEY_PATH, remotepath="/home/ubuntu/.ssh/" + c.PRIVATE_KEY_NAME)
            ssh_client.run("chmod 400 /home/ubuntu/.ssh/" + c.PRIVATE_KEY_NAME)

            # ssh_client.run("sudo groupadd supergroup")
            ssh_client.run("sudo usermod -aG supergroup $USER")
            ssh_client.run("sudo usermod -aG supergroup root")

            # join docker group
            ssh_client.run("sudo usermod -aG docker $USER")
            
            # Ensure Maven is globally set to access Maven Central repositories using https protocol (mandatory since January 15, 2020)
            ssh_client.run("sudo rm  /home/ubuntu/maven_settings.xml")
            ssh_client.put(localpath="./maven_settings.xml", remotepath="/home/ubuntu/maven_settings.xml")
            ssh_client.run("sudo mv /etc/maven/settings.xml /etc/maven/settings.xml.backup")
            ssh_client.run("sudo rm /etc/maven/settings.xml")
            ssh_client.run("sudo mv /home/ubuntu/maven_settings.xml /etc/maven/settings.xml")


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
    
    stdout, stderr, status = ssh_client.run(
            "cd " + c.SPARK_HOME + " && cp conf/log4j.properties.template conf/log4j.properties")
    print(stdout, stderr)
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

    # CLEAN UP EXECUTORS APP LOGS
    print("   Cleaning up slave executor app logs...")
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")
    print(stdout + stderr)   
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_2_HOME + "work/*")
    print(stdout + stderr)   
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_HOME + "logs/*")
    print(stdout + stderr)   
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_2_HOME + "logs/*")
    print("   Remove Logs")
    ssh_client.run("sudo rm " + c.SPARK_HOME + "spark-events/*")
    print(stdout + stderr)   
    
    if c.UPDATE_SPARK:
        print("   Updating Spark...")
        stdout, stderr, status = ssh_client.run(
            """cd /usr/local/spark && git remote set-url origin  """ + c.GIT_XSPARK_REPO)
        print(stdout + stderr)
        stdout, stderr, status = ssh_client.run("""cd /usr/local/spark && git pull""")
        print(stdout + stderr)
        stdout, stderr, status = ssh_client.run(
            """cd /usr/local/spark && git checkout """ + c.GIT_XSPARK_BRANCH)
        # stdout, stderr, status = ssh_client.run(
        #    """cd /usr/local/spark && git remote set-url origin  """ + c.GIT_XSPARK_REPO +
        #    """ && git pull && git checkout """ + c.GIT_XSPARK_BRANCH) 
        #    """cd /usr/local/spark && git pull && git checkout """ + c.GIT_XSPARK_BRANCH)
        #    """ && git remote prune origin && git branch --unset-upstream && git pull && git checkout """ + c.GIT_XSPARK_BRANCH)
        print(stdout + stderr)
        # CLEAN UP EXECUTORS APP LOGS
        # print("   Cleaning up slave executor app logs...")
        # stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")
        # print(stdout + stderr)
        stdout, stderr, status = ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean && build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")
        print(stdout + stderr)
        
    
    if c.DISABLE_HT:
        # DISABLE HT
        ssh_client.put(localpath="./disable-ht-v2.sh", remotepath="$HOME/disable-ht-v2.sh")
        ssh_client.run("chmod +x $HOME/disable-ht-v2.sh")
        stdout, stderr, status = ssh_client.run('sudo $HOME/disable-ht-v2.sh')
        print("   Disabled HyperThreading {}".format(status))

    if current_cluster == 'spark':
        # check that line numbers to be substituted are present in file spark-defaults.conf and if not, add 11 empty comment lines
        '''
        stdout, stderr, status = ssh_client.run(
                    "if [ $(wc -l /usr/local/spark/conf/spark-defaults.conf | awk '{ print $1 }') -lt 60 ]; \
                    then echo -e '#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n' >> /usr/local/spark/conf/spark-defaults.conf; fi")
        '''
        stdout, stderr, status = ssh_client.run("cd " + c.SPARK_HOME + " && sudo rm conf/spark-env.sh")
        print(stdout, stderr)
        
        stdout, stderr, status = ssh_client.run(
            "cd " + c.SPARK_HOME + " && cp conf/spark-env.sh.template conf/spark-env.sh && " + 
            "sed -n '1,25p' conf/spark-defaults.conf.template > conf/spark-defaults.conf && " + 
            "echo -e '#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n" + 
                     "#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n' >> conf/spark-defaults.conf")
        print(stdout, stderr)
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '27s/.*/spark.executor.extraJavaOptions -XX:+UseG1GC/' {0}conf/spark-defaults.conf".format(
            c.SPARK_HOME)) # dagsymb - nobench
 
        stdout, stderr, status = ssh_client.run(
            "sed -i '29s/.*/spark.eventLog.enabled true/' {0}conf/spark-defaults.conf".format(
            c.SPARK_HOME)) # dagsymb - nobench
 
        stdout, stderr, status = ssh_client.run(
            "sed -i '30s%.*%spark.eventLog.dir {0}spark-events%' {1}conf/spark-defaults.conf".format(
            c.SPARK_HOME, c.SPARK_HOME)) # dagsymb - nobench
 
        stdout, stderr, status = ssh_client.run(
            "sed -i '31s/.*/spark.shuffle.service.enabled {0}/' {1}conf/spark-defaults.conf".format(
            c.ENABLE_EXTERNAL_SHUFFLE, c.SPARK_HOME))
 
        stdout, stderr, status = ssh_client.run(
            "sed -i '32s/.*/spark.memory.offHeap.enabled {0}/' {1}conf/spark-defaults.conf".format(
            c.OFF_HEAP, c.SPARK_HOME))
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '33s/.*/spark.memory.offHeap.size {0}/' {1}conf/spark-defaults.conf".format(
            c.OFF_HEAP_BYTES, c.SPARK_HOME))
            
        stdout, stderr, status = ssh_client.run(
            "sed -i '41s{.*{spark.control.cpuperiod " + str(
            c.CPU_PERIOD) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        stdout, stderr, status = ssh_client.run(
            "sed -i '42s/.*/spark.control.k {0}/' {1}conf/spark-defaults.conf".format(
            c.K , c.SPARK_HOME))
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '43s/.*/spark.control.tsample {0}/' {1}conf/spark-defaults.conf".format(
            c.T_SAMPLE, c.SPARK_HOME))
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '44s/.*/spark.control.ti {0}/' {1}conf/spark-defaults.conf".format(
            c.TI, c.SPARK_HOME))
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '45s{.*{spark.control.corequantum " + str(
            c.CORE_QUANTUM) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '50s{.*{spark.control.coremin " + str(
            c.CORE_MIN) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
    # if current_cluster == 'spark':
        print("   Starting Spark Slave")
        ssh_client.run(
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
    ssh_client = sshclient_from_node(node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')
    stdout, stderr, status = ssh_client.run("hostname  && pwd") #vboxvm
    print(stdout)
    with open_cfg(mode='w') as cfg:
        current_cluster = cfg['main']['current_cluster'] 
        benchmark_name = c.CONFIG_DICT['Benchmark']['Name'] if 'experiment' in cfg and \
                                                               'benchmarkname' in cfg['experiment'] else ''
        app_name = cfg['main']['app_name'] if 'main' in cfg and 'app_name' in cfg['main'] else '' # 'app'
        app_dir = cfg['main']['app_jar'].split("/")[0] if 'main' in cfg and 'app_jar' in cfg['main'] else \
                        cfg['main']['appdir'] if 'main' in cfg and 'appdir' in cfg['main'] else "" # "application"
        # app_dir = cfg['main']['app_jar'].split("/")[0] if 'main' in cfg and 'app_jar' in cfg['main'] else "application"
        cfg[current_cluster] = {}
        if benchmark_name != '':
            print("Setup Master: BENCH_CONF=" + str(c.BENCH_CONF[benchmark_name]))
        if app_name != '':   
            print("Setup Master: app_name =", app_name)
            print("Setup Master: app_dir =", app_dir)
        print("Setup Master: PublicIp=" + node.public_ips[0] + " PrivateIp=" + node.private_ips[0])
        master_private_ip = get_ip(node)
        master_public_ip = node.public_ips[0]

        # save private master_ip to cfg file
        print('saving master ip')
        cfg[current_cluster]['master_private_ip'] = master_private_ip
        cfg[current_cluster]['master_public_ip'] = master_public_ip

    common_setup(ssh_client)

     # update spark-bench and spark-perf
     
    ssh_client.run("sudo mv /usr/local/spark-perf/ /home/ubuntu/")
    ssh_client.run("sudo mv /usr/local/spark-bench/ /home/ubuntu/")
    ssh_client.run("sudo mv /usr/local/wikixmlj/ /home/ubuntu/")

    files = ssh_client.listdir("/home/ubuntu/")

    if "wikixmlj" in files:
        ssh_client.run("""cd $HOME/wikixmlj && git status | grep "up-to-date" || eval `git pull && mvn package install`""")
        ssh_client.run("cd $HOME")
    else:
        ssh_client.run("git clone https://github.com/synhershko/wikixmlj.git wikixmlj")
        ssh_client.run(
            "cd $HOME/wikixmlj && mvn package install -Dmaven.test.skip=true && cd $HOME")  # install wikixmlj
        
    # download or update
    if c.UPDATE_SPARK_PERF:
        '''
        if "wikixmlj" in files:
            ssh_client.run("""cd $HOME/wikixmlj && git status | grep "up-to-date" || eval `git pull && mvn package install`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/synhershko/wikixmlj.git wikixmlj")
            ssh_client.run(
                "cd $HOME/wikixmlj && mvn package install -Dmaven.test.skip=true && cd $HOME")  # install wikixmlj
        '''
        if "spark-perf" in files:
            ssh_client.run("""cd $HOME/spark-perf && git status | grep "up-to-date" || eval `git pull && cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/databricks/spark-perf.git spark-perf")
            ssh_client.run(
                "cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py")
    elif c.SKEW_TEST:
        print("Removing default sparf-perf")
        stdout, stderr, status = ssh_client.run("sudo rm -r ./spark-perf")
        print(stdout, stderr)
        print("Cloning master branch from https://github.com/gioenn/spark-perf.git")
        stdout, stderr, status = ssh_client.run("git clone -b master --single-branch https://github.com/gioenn/spark-perf.git")
        #print("Cloning master branch from https://github.com/DavideB/spark-perf.git")
        #stdout, stderr, status = ssh_client.run("git clone -b master --single-branch https://github.com/DavideB/spark-perf.git")
        #stdout, stderr, status = ssh_client.run("git clone -b log_skew --single-branch https://github.com/DavideB/spark-perf.git")
        print(stdout, stderr)
        '''
        stdout, stderr, status = ssh_client.run(
                "cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py")
        print(stdout, stderr)
        stdout, stderr, status = ssh_client.run(
                "cd $HOME/spark-perf/config && sed -i '134,137d' config.py")
        print(stdout, stderr)
        '''
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

    # CLEAN UP EXECUTORS APP LOGS
    print("   Cleaning up master app logs...")
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")
    print(stdout + stderr)   
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_2_HOME + "work/*")
    print(stdout + stderr)   
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_HOME + "logs/*")
    print(stdout + stderr)   
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_2_HOME + "logs/*")
    print("   Remove Logs")
    ssh_client.run("sudo rm " + c.SPARK_HOME + "spark-events/*")
    print(stdout + stderr)   

    if c.UPDATE_SPARK_MASTER:
        print("   Updating Spark...")
        '''stdout, stderr, status = ssh_client.run(
            """cd /usr/local/spark && git remote set-url origin  """ + c.GIT_XSPARK_REPO +
            """ && git pull && git checkout """ + c.GIT_XSPARK_BRANCH) 
        '''
        stdout, stderr, status = ssh_client.run(
            """cd /usr/local/spark && git remote set-url origin  """ + c.GIT_XSPARK_REPO)
        print(stdout + stderr)
        stdout, stderr, status = ssh_client.run("""cd /usr/local/spark && git pull""")
        print(stdout + stderr)
        stdout, stderr, status = ssh_client.run(
            """cd /usr/local/spark && git checkout """ + c.GIT_XSPARK_BRANCH)
        #    """cd /usr/local/spark && git pull && git checkout """ + c.GIT_XSPARK_BRANCH)
        #    """&& git remote prune origin && git branch --unset-upstream && git pull && git checkout """ + c.GIT_XSPARK_BRANCH)    
        
        print(stdout + stderr)
        # print("   Cleaning up executor app logs...")
        # stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")
        # print(stdout + stderr)
        # stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_2_HOME + "work/*")
        # print(stdout + stderr)   
        # ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")
        stdout, stderr, status = ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean && build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")
        #print(stdout + stderr)
    
    '''
    print("   Cleaning up executor app logs...")
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_HOME + "work/*")
    # print(stdout + stderr)   
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_2_HOME + "work/*")
    # print(stdout + stderr)   
    print("   Remove Logs")
    ssh_client.run("sudo rm " + c.SPARK_HOME + "spark-events/* && sudo rm " + c.SPARK_HOME + "logs/*")
    stdout, stderr, status = ssh_client.run("sudo rm -r " + c.SPARK_2_HOME + "logs/*")
    '''
        
    if current_cluster == 'spark':
        if app_dir in files:
                ssh_client.run("""cd $HOME/""" + app_dir + """ && git status | grep "up-to-date" || eval `git pull && git checkout """ + c.GIT_APPLICATION_BRANCH + """ && mvn clean && mvn package `""") # update application
                ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone " + c.GIT_APPLICATION_REPO + " " +  app_dir)
            ssh_client.run("cd $HOME/" + app_dir + " && git pull && git checkout " + c.GIT_APPLICATION_BRANCH + " && mvn package && cd $HOME")  # install application
        
        if c.UPDATE_APPLICATION:
            print("   Updating Application ", app_dir, " ...")
            stdout, stderr, status = ssh_client.run("cd $HOME/" + app_dir + " && git pull && git checkout " + c.GIT_APPLICATION_BRANCH + " && mvn clean && mvn package ") # update application
            print(stdout + stderr)
            stdout, stderr, status = ssh_client.run("cd $HOME/" + app_dir + " && mvn clean && mvn package")  # install application
            print(stdout + stderr)
        
        '''
        # check that line numbers to be substituted are present in file spark-defaults.conf and if not, add 11 empty comment lines
        stdout, stderr, status = ssh_client.run(
                    "if [ $(wc -l /usr/local/spark/conf/spark-defaults.conf | awk '{ print $1 }') -le 60 ]; \
                    then echo -e '#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n' >> /usr/local/spark/conf/spark-defaults.conf; fi")
        '''
        stdout, stderr, status = ssh_client.run(
            "cd " + c.SPARK_HOME + " && cp conf/spark-env.sh.template conf/spark-env.sh && " + 
            "sed -n '1,25p' conf/spark-defaults.conf.template > conf/spark-defaults.conf && " + 
            "echo -e '#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n" + 
                     "#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n' >> conf/spark-defaults.conf")
        print(stdout, stderr)   
        # SPARK MASTER
        stdout, stderr, status = ssh_client.run(
            "sed -i '22s{.*{spark.master " + master_private_ip + ":7077" + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '27s/.*/spark.executor.extraJavaOptions -XX:+UseG1GC/' {0}conf/spark-defaults.conf".format(
            c.SPARK_HOME))
 
        stdout, stderr, status = ssh_client.run(
            "sed -i '29s/.*/spark.eventLog.enabled true/' {0}conf/spark-defaults.conf".format(
            c.SPARK_HOME))
 
        stdout, stderr, status = ssh_client.run(
            "sed -i '30s%.*%spark.eventLog.dir {0}spark-events%' {1}conf/spark-defaults.conf".format(
            c.SPARK_HOME, c.SPARK_HOME))
        # SHUFFLE SERVICE EXTERNAL
        stdout, stderr, status = ssh_client.run(
            "sed -i '31s/.*/spark.shuffle.service.enabled {0}/' {1}conf/spark-defaults.conf".format(
                c.ENABLE_EXTERNAL_SHUFFLE, c.SPARK_HOME))
        # OFF HEAP
        stdout, stderr, status = ssh_client.run(
            "sed -i '32s{.*{spark.memory.offHeap.enabled " + str(
                c.OFF_HEAP) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '33s{.*{spark.memory.offHeap.size " + str(
                c.OFF_HEAP_BYTES) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        if benchmark_name != '' :
            print("   Changing Benchmark " + benchmark_name + " settings")
        if app_name != '' :
            print("   Changing Application " + app_name + " settings")
            
        # DEADLINE LINE 35
        stdout, stderr, status = ssh_client.run(
            "sed -i '35s{.*{spark.control.deadline " + str(
                c.DEADLINE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        # MAX EXECUTOR LINE 39
        stdout, stderr, status = ssh_client.run(
            "sed -i '39s{.*{spark.control.maxexecutor " + str(
                c.MAX_EXECUTOR) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        # CORE FOR VM LINE 40
        stdout, stderr, status = ssh_client.run(
            "sed -i '40s{.*{spark.control.coreforvm " + str(
                c.CORE_VM) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        # ALPHA LINE 36
        stdout, stderr, status = ssh_client.run(
            "sed -i '36s{.*{spark.control.alpha " + str(
                c.ALPHA) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        # BETA line 37
        stdout, stderr, status = ssh_client.run(
            "sed -i '37s{.*{spark.control.beta " + str(
                c.BETA) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        # OVERSCALE LINE 38
        stdout, stderr, status = ssh_client.run(
            "sed -i '38s{.*{spark.control.overscale " + str(
                c.OVER_SCALE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        # HEURISTIC TYPE LINE 56
        stdout, stderr, status = ssh_client.run(
            "sed -i '56s{.*{spark.control.heuristic " + str(
                c.HEURISTIC.value) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        # CORE_ALLOCATION
        if c.CORE_ALLOCATION != None and c.DEADLINE_ALLOCATION != None and c.STAGE_ALLOCATION != None:
            stdout, stderr, status = ssh_client.run("sed -i '57s{.*{spark.control.stage " + str(c.STAGE_ALLOCATION) + "{' "+c.SPARK_HOME+"conf/spark-defaults.conf")
            # print("setup_master, spark-defaults.conf 57 str(c.STAGE_ALLOCATION): " + str(c.STAGE_ALLOCATION) + stdout + stderr)
            stdout, stderr, status = ssh_client.run("sed -i '58s{.*{spark.control.stagecores "+ str(c.CORE_ALLOCATION) +"{' "+c.SPARK_HOME + "conf/spark-defaults.conf")
            # print("setup_master, spark-defaults.conf 58 str(c.CORE_ALLOCATION): " + str(c.CORE_ALLOCATION) + stdout + stderr)
            stdout, stderr, status = ssh_client.run("sed -i '59s{.*{spark.control.stagedeadlines " + str(
                c.DEADLINE_ALLOCATION) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
            # print("setup_master, spark-defaults.conf 59 str(c.DEADLINE_ALLOCATION): " + str(c.DEADLINE_ALLOCATION) + stdout + stderr)
        else:
            stdout, stderr, status = ssh_client.run("sed -i '57s{.*{#stage{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
            # print("setup_master, spark-defaults.conf 57 c.SPARK_HOME: " + c.SPARK_HOME + stdout + stderr)
            stdout, stderr, status = ssh_client.run("sed -i '58s{.*{#stagecores{' "+c.SPARK_HOME + "conf/spark-defaults.conf")
            # print("setup_master, spark-defaults.conf 58 c.SPARK_HOME: " + c.SPARK_HOME + stdout + stderr)
            stdout, stderr, status = ssh_client.run("sed -i '59s{.*{#stagedeadlines{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
            # print("setup_master, spark-defaults.conf 59 c.SPARK_HOME: " + c.SPARK_HOME + stdout + stderr)
        
        # CHANGE ALSO IN MASTER FOR THE LOGS
        stdout, stderr, status = ssh_client.run(
            "sed -i '43s{.*{spark.control.tsample " + str(
                c.T_SAMPLE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '42s{.*{spark.control.k " + str(
                c.K ) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '44s{.*{spark.control.ti " + str(
                c.TI) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '45s{.*{spark.control.corequantum " + str(
                c.CORE_QUANTUM) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '46s{.*{spark.locality.wait " + str(
                c.LOCALITY_WAIT) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '51s{.*{spark.locality.wait.node " + str(
                c.LOCALITY_WAIT_NODE) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '52s{.*{spark.locality.wait.process " + str(
                c.LOCALITY_WAIT_PROCESS) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '53s{.*{spark.locality.wait.rack " + str(
                c.LOCALITY_WAIT_RACK) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '47s{.*{spark.task.cpus " + str(
                c.CPU_TASK) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '48s{.*{spark.control.nominalrate 0.0{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '49s{.*{spark.control.nominalratedata 0.0{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '50s{.*{spark.control.coremin " + str(
            c.CORE_MIN) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '54s{.*{spark.control.inputrecord " + str(
                c.INPUT_RECORD) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run(
            "sed -i '55s{.*{spark.control.numtask " + str(
                c.NUM_TASK) + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")
        
        stdout, stderr, status = ssh_client.run("""sed -i '3s{.*{master=""" + master_private_ip +
                       """{' ./spark-bench/conf/env.sh""")
        
        stdout, stderr, status = ssh_client.run("""sed -i '63s{.*{NUM_TRIALS=""" + str(c.BENCH_NUM_TRIALS) +
                       """{' ./spark-bench/conf/env.sh""")
        
        # CHANGE SPARK HOME DIR
        stdout, stderr, status = ssh_client.run("sed -i '21s{.*{SPARK_HOME_DIR = \"" + c.SPARK_HOME + "\"{' ./spark-perf/config/config.py")
        #print("setup_master, ./spark-perf/config/config.py 21: " + stdout + stderr)
        
        # CHANGE MASTER ADDRESS IN BENCHMARK
        stdout, stderr, status = ssh_client.run("""sed -i '30s{.*{SPARK_CLUSTER_URL = "spark://""" + master_private_ip +
                       """:7077"{' ./spark-perf/config/config.py""")
        
        # CHANGE SCALE FACTOR LINE 127
        stdout, stderr, status = ssh_client.run(
            "sed -i '127s{.*{SCALE_FACTOR = " + str(c.SCALE_FACTOR) + "{' ./spark-perf/config/config.py")
        
        # CHANGE IGNORE_TRIALS LINE 134
        stdout, stderr, status = ssh_client.run(
            "sed -i '134s{.*{IGNORED_TRIALS = " + str(c.IGNORED_TRIALS) + "{' ./spark-perf/config/config.py")
        
        # NO PROMPT
        stdout, stderr, status = ssh_client.run("sed -i '103s{.*{PROMPT_FOR_DELETES = False{' ./spark-perf/config/config.py")
        if len(c.BENCHMARK_PERF) > 0:  # and c.SPARK_PERF_FOLDER == "spark-perf-gioenn":
            if c.SKEW_TEST:
                print("   Setting up skewed test")
                ssh_client.run("""sed -i '169s{.*{    OptionSet("skew", [""" + str(
                    c.BENCH_CONF[c.BENCHMARK_PERF[0]]["skew"]) + """]),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            else:
                # Restore "no-skew test" line content
                ssh_client.run("""sed -i '169s{.*{# The number of input partitions.{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            
            # ssh_client.run("""sed -i '164s{.*{OptionSet("skew", [""" + str(
            #     c.BENCH_CONF[c.BENCHMARK_PERF[0]]["skew"]) + """]){' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            
            print("   Setting up num_trials")
            ssh_client.run("""sed -i '160s{.*{    OptionSet("num-trials", [1]),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            
            print("   Setting up unique-keys, num-partitions and reduce-tasks")
            ssh_client.run("""sed -i '185s{.*{OptionSet("unique-keys",[""" + str(c.BENCH_CONF[c.BENCHMARK_PERF[0]][
                                                                                     "unique-keys"]) + """], False),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            ssh_client.run("""sed -i '170s{.*{OptionSet("num-partitions", [""" + str(c.BENCH_CONF[c.BENCHMARK_PERF[0]][
                                                                                         "num-partitions"]) + """], can_scale=False),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
            ssh_client.run("""sed -i '172s{.*{OptionSet("reduce-tasks", [""" + str(c.BENCH_CONF[c.BENCHMARK_PERF[0]][
                                                                                       "reduce-tasks"]) + """], can_scale=False),{' ./""" + c.SPARK_PERF_FOLDER + "/config/config.py")
        # CHANGE RAM EXEC
        stdout, stderr, status =  ssh_client.run(
            """sed -i '146s{.*{    JavaOptionSet("spark.executor.memory", [""" + c.RAM_EXEC + """]),{' ./spark-perf/config/config.py""")

        stdout, stderr, status = ssh_client.run(
            """sed -i '55s{.*{SPARK_EXECUTOR_MEMORY=""" + c.RAM_EXEC + """{' ./spark-bench/conf/env.sh""")

        # CHANGE RAM DRIVER
        stdout, stderr, status = ssh_client.run(
            "sed -i '26s{.*{spark.driver.memory " + c.RAM_DRIVER + "{' " + c.SPARK_HOME + "conf/spark-defaults.conf")

        stdout, stderr, status = ssh_client.run(
            "sed -i '154s{.*{SPARK_DRIVER_MEMORY = \""+c.RAM_DRIVER+"\"{' ./spark-perf/config/config.py")

        if benchmark_name != '': 
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
        stdout, stderr, status = ssh_client.run("sed -i '179s%memory%hdfs%g' ./spark-perf/config/config.py")

    #if hdfs_master != "":
        stdout, stderr, status = ssh_client.run(
            """sed -i  '50s%.*%HDFS_URL = "hdfs://{0}:9000/test/"%' ./spark-perf/config/config.py""".format(
                hdfs_master))
        stdout, stderr, status = ssh_client.run(
            """sed -i  '10s%.*%HDFS_URL="hdfs://{0}:9000"%' ./spark-bench/conf/env.sh""".format(
                hdfs_master))
        stdout, stderr, status = ssh_client.run(
            """sed -i  '14s%.*%DATA_HDFS="hdfs://{0}:9000/SparkBench"%' ./spark-bench/conf/env.sh""".format(
                hdfs_master))

        # TODO: additional settings for spark-bench
        # ssh_client.run("""sed -i '8s/.*//' ./spark-bench/conf/env.sh""")
        stdout, stderr, status = ssh_client.run("""sed -i '8s{.*{[ -z "$HADOOP_HOME" ] \&\&     export HADOOP_HOME="""+c.HADOOP_HOME+"""{' ./spark-bench/conf/env.sh""")
        # slaves = slaves_ip[0]
        # for slave in slaves_ip[1:]:
        #     slaves = slaves + ", " + slave

        stdout, stderr, status = ssh_client.run(
            """sed -i  '5s%.*%MC_LIST=()%' ./spark-bench/conf/env.sh""")

        stdout, stderr, status = ssh_client.run(
            """sed -i  '19s%.*%SPARK_VERSION=2.0%' ./spark-bench/conf/env.sh""")

        # ssh_client.run("""sed -i '20s/.*//' ./spark-bench/conf/env.sh""")
        stdout, stderr, status = ssh_client.run("""sed -i '20s{.*{[ -z "$SPARK_HOME" ] \&\&     export SPARK_HOME="""+c.SPARK_HOME+"""{' ./spark-bench/conf/env.sh""")

        
        # CHANGE RAM DRIVER
        stdout, stderr, status = ssh_client.run(
            """sed -i '26s{.*{spark.driver.memory """ + c.RAM_DRIVER + """{' """ + c.SPARK_HOME + """conf/spark-defaults.conf""")
        stdout, stderr, status = ssh_client.run(
            """sed -i '28s{.*{spark.driver.maxResultSize """ + c.RAM_DRIVER_MAX_RESULT_SIZE + """{' """ + c.SPARK_HOME + """conf/spark-defaults.conf""")
        # CHANGE RAM EXEC
        stdout, stderr, status = ssh_client.run(
            """sed -i '41s{.*{SPARK_EXECUTOR_MEMORY=""" + c.RAM_EXEC + """{' """ + c.SPARK_HOME + """conf/spark-env.sh""")
        # SET HADOOP CONF DIR
        stdout, stderr, status = ssh_client.run(
            """sed -i '25s{.*{HADOOP_CONF_DIR=""" + c.HADOOP_CONF + """{' """ + c.SPARK_HOME + """conf/spark-env.sh""")
        
        # Setup HDFS config files
        ssh_client.run(
            "sed -i '19s%.*%<configuration>  <property>    <name>fs.default.name</name>    <value>hdfs://" + c.HDFS_MASTER + ":9000</value>  </property>%g' " + c.HADOOP_CONF + "core-site.xml")
        #TODO: check if setup of file hdfs-site.xml is strictly needed here or not
        ssh_client.run(
            "sed -i '38s%.*%<value>" + c.HDFS_MASTER + ":50070</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
        ssh_client.run(
            "sed -i '43s%.*%<value>" + c.HDFS_MASTER + ":50090</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
        ssh_client.run(
            "sed -i '48s%.*%<value>" + c.HDFS_MASTER + ":9000</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
        ssh_client.run(
            "sed -i 's%/var/lib/hadoop/hdfs/namenode%/mnt/hdfs/namenode%g' " + c.HADOOP_CONF + "hdfs-site.xml")
        ssh_client.run(
            "sed -i 's%/var/lib/hadoop/hdfs/datanode%/mnt/hdfs/datanode%g' " + c.HADOOP_CONF + "hdfs-site.xml")
        
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
    if current_cluster == 'spark' and c.PROCESS_ON_SERVER:
        if not tool_on_master:
            if c.INSTALL_PYTHON3 == 1:
                """Install python3 on cSpark master"""
                stdout, stderr, status = ssh_client.run("sudo apt-get update")
                stdout, stderr, status = ssh_client.run("sudo apt-get install -y python3-pip")
                print("Installing Python3-pip on cspark master:\n" + stdout + stderr)
                stdout, stderr, status = ssh_client.run("sudo apt-get build-dep -y matplotlib")
                print("Installing matplotlib on cspark master:\n" + stdout + stderr)
                stdout, stderr, status = ssh_client.run("sudo apt-get install -y build-essential libssl-dev libffi-dev python-dev python3-dev")
                print("Installing Python3 build-essential libssl-dev libffi-dev python-dev python3-dev on cspark master:\n" + stdout + stderr)
                '''
                stdout, stderr, status = ssh_client.run("sudo apt-get update && sudo apt-get install -y python3-pip && " +
                                                        "sudo apt-get build-dep -y matplotlib && "+
                                                        "sudo apt-get install -y build-essential libssl-dev libffi-dev python-dev python3-dev && "+
                                                        #"sudo apt-get install pkg-config" +             #vboxvm? (double-check)
                                                        #"sudo apt-get install libfreetype6-dev" +       #vboxvm? (double-check)
                                                        "" # "sudo pip3 install --upgrade setuptools"    # do not run this as it makes pip3 to require python 3.5 
                                                        )
                print("Installing Python3 on cspark master:\n" + stdout)
                '''
            stdout, stderr, status = ssh_client.run("sudo rm -r /home/ubuntu/xSpark-dagsymb")
            stdout, stderr, status = ssh_client.run("git clone -b " + c.GIT_XSPARK_DAGSYMB_BRANCH + " --single-branch " +
                                                    c.GIT_XSPARK_DAGSYMB_REPO + " /home/ubuntu/xSpark-dagsymb")
            #stdout, stderr, status = ssh_client.run("git clone -b master --single-branch " +
            #                                        "https://github.com/gioenn/xSpark-dagsymb.git /home/ubuntu/xSpark-dagsymb")
            """Clone xSpark-dagsymb on cspark master"""
            print("Cloning xSpark-dagsymb tool on cspark master:\n" + stdout + stderr)
            
            stdout, stderr, status = ssh_client.run("cd /home/ubuntu/xSpark-dagsymb/spark_log_profiling " + 
                                                    "&& mkdir input_logs && mkdir avg_json && mkdir output_json " + 
                                                    "&& mkdir processed_logs && mkdir processed_profiles")
            
            if not c.PRIVATE_KEY_NAME in ssh_client.listdir("/home/ubuntu/"):
                ssh_client.put(localpath=c.PRIVATE_KEY_PATH, remotepath="/home/ubuntu/" + c.PRIVATE_KEY_NAME)
                ssh_client.run("chmod 400 /home/ubuntu/" + c.PRIVATE_KEY_NAME)
            """upload private key to home directory"""
            
            if not c.PRIVATE_KEY_NAME + ".pub" in ssh_client.listdir("/home/ubuntu/"):
                ssh_client.put(localpath=c.AZ_PUB_KEY_PATH, remotepath="/home/ubuntu/" + c.PRIVATE_KEY_NAME + ".pub")
                ssh_client.run("chmod 400 /home/ubuntu/" + c.PRIVATE_KEY_NAME + ".pub")
            """upload public key to home directory"""
            
            ssh_client.run("sudo rm /home/ubuntu/xSpark-dagsymb/configure.py")
            ssh_client.put(localpath="configure.py", remotepath="/home/ubuntu/xSpark-dagsymb/configure.py")
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
            ssh_client.run("sudo rm /home/ubuntu/xSpark-dagsymb/credentials.json")
            ssh_client.put(localpath="credentials_on_server.json", remotepath="/home/ubuntu/xSpark-dagsymb/credentials.json")
            """upLoad credentials.json"""
            
            credentials_on_server_file = Path("credentials_on_server.json")
            if credentials_on_server_file.exists():
                os.remove("credentials_on_server.json")
            
            ssh_client.run("sudo rm /home/ubuntu/setup.json")
            ssh_client.put(localpath="setup.json", remotepath="/home/ubuntu/xSpark-dagsymb/setup.json")
            """upLoad setup.json"""
            
            ssh_client.run("sudo rm /home/ubuntu/control.json")
            ssh_client.put(localpath="control.json", remotepath="/home/ubuntu/xSpark-dagsymb/control.json")
            """upLoad control.json"""
            
            # update cfg_clusters.ini before uploading it to master
            with open_cfg(mode='w') as cfg:
                cfg['main']['tool_on_master'] = 'true'
                write_cfg(cfg)
                #tool_on_master = True
            
            stdout, stderr, status = ssh_client.run("cd /home/ubuntu/xSpark-dagsymb && " +
                                                    "sudo pip3 install -r requirements.txt")
            """Install xSpark-dagsymb tool requirements"""
            print("Installing xSpark-dagsymb tool requirements\n" + stdout)    
        
        ssh_client.run("sudo rm /home/ubuntu/xSpark-dagsymb/cfg_clusters.ini")
        ssh_client.put(localpath="cfg_clusters.ini", remotepath="/home/ubuntu/xSpark-dagsymb/cfg_clusters.ini")
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
    
    ssh_client.run(
        "sed -i '38s%.*%<value>" + master_ip + ":50070</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '43s%.*%<value>" + master_ip + ":50090</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '48s%.*%<value>" + master_ip + ":9000</value>%g' " + c.HADOOP_CONF + "hdfs-site.xml")
    
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

def upload_profile_to_master(nodes, profile_fname, localfilepath, overwrite=False):
    """

    :param nodes, profile_fname, localfilepath:
    :return:
    """
    ssh_client = sshclient_from_node(nodes[0], ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')
    print("Uploading profile: " + profile_fname + "\n")
    
    if overwrite: 
        print("Removing existing : " + profile_fname + "\n")
        ssh_client.run("sudo rm " + c.C_SPARK_HOME + "conf/" + profile_fname)
        
    if not profile_fname in ssh_client.listdir(c.C_SPARK_HOME + "conf/"):
        ssh_client.put(localpath=localfilepath, remotepath=c.C_SPARK_HOME + "conf/" + profile_fname)
        ssh_client.run("chmod 664 " + c.C_SPARK_HOME + "conf/" + profile_fname)
        ssh_client.run("sudo chown ubuntu:ubuntu " + c.C_SPARK_HOME + "conf/" + profile_fname)
        print("Profile  " + profile_fname + " successfully uploaded\n") 
        """upload profile to spark conf directory"""
    else:
        print("Profile  " + profile_fname + " not uploaded: a profile with the same name already exists\n")

@timing
def run_symexapp(nodes):
    """

    :return:
    """
    if len(nodes) == 0:
        print("No instances running")
        exit(1)
    
    profile = False
    profile_fname = ""
    profile_option = False
    app_name = ""
    app_jar = ""
    app_class = ""
    meta_profile_name = ""
    gen_data_option = "-no_data_gen";
    
    with open_cfg(mode='w') as cfg:
        current_cluster = cfg['main']['current_cluster']
        c.CLUSTER_ID = c.CLUSTER_MAP[current_cluster]
        
        if 'main' in cfg and 'experiment_file' in cfg['main']:
            exp_filepath = cfg['main']['experiment_file']
            c.config_experiment(exp_filepath, cfg)  
        
        setup = True if 'main' in cfg and 'setup' in cfg['main'] else False
        app_name = cfg['main']['app_name'] if 'main' in cfg and 'app_name' in cfg['main'] else "app"
        meta_profile_name = cfg['experiment']['meta_profile_name'] if 'experiment' in cfg and 'meta_profile_name' in cfg['experiment'] else ""
        app_jar = cfg['main']['app_jar'] if 'main' in cfg and 'app_jar' in cfg['main'] else "app.jar"
        app_class = cfg['main']['app_class'] if 'main' in cfg and 'app_class' in cfg['main'] else "Main"
        guard_evaluator_class = cfg['main']['guard_evaluator_class'] if 'main' in cfg and \
            'guard_evaluator_class' in cfg['main'] else "GuardEvaluator"
        #    'guard_evaluator_class' in cfg['main'] else "it.polimi.deepse.dagsymb.examples.GuardEvaluatorPromoCallsFile"
        child_args_string = cfg['main']['child_args_string'] if 'main' in cfg and 'child_args_string' in cfg['main'] else ""
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
        iter_num = cfg['main']['iter_num'] if 'main' in cfg and 'iter_num' in cfg['main'] else str(0)
        # iter_num = cfg['main']['iter_num'] #vboxvm
        c.MAX_EXECUTOR = end_index - 1
        c.cfg_dict["MaxExecutor"] = c.CONFIG_DICT["Control"]["MaxExecutor"] = c.MAX_EXECUTOR
        #c.CONFIG_DICT["Control"]["MaxExecutor"] = c.MAX_EXECUTOR
        #c.cfg_dict["ConfigDict"] = c.CONFIG_DICT
        if 'main' in cfg and 'app_name' in cfg['main']:
            c.config_app(app_name, cfg)
        profile = True if 'profile' in cfg else False
        print("profile mode set to " + str(profile))
        profile_option = cfg.getboolean('main', 'profile') if 'main' in cfg and 'profile' in cfg['main'] else False
        print("app_name: " + app_name)
        profile_fname = cfg['main']['app_name'] if profile and 'main' in cfg and 'app_name' in cfg['main'] else ""
        print("profile filename set to: " + profile_fname)
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
        # delete selected app_name profile file
        print("Deleting " + app_name + " app_name reference profile: " + profile_fname + ".json\n")
        stdout, stderr, status = ssh_client.run("sudo rm " + c.C_SPARK_HOME + "conf/" + profile_fname + ".json")
        print(stdout + stderr) 
          
    if c.SPARK_HOME == c.SPARK_2_HOME:
        print("Check Effectively Executor Running")
    
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

    # LANCIARE app_name
    
    if current_cluster == 'spark' and not setup :
        ''' 
        if len(c.app_name_PERF) > 0:
            if delete_hdfs:
                print("   Cleaning HDFS...")
                print("connecting to hdfs_master:{}".format(hdfs_master_public_ip))
                ssh_client_hdfs = sshclient_from_ip(hdfs_master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu')
                out, err, status = ssh_client_hdfs.run(
                    "/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
                print(out, err, status)
            print("Running app_name " + str(c.app_name_PERF))
            runout, runerr, runstatus = ssh_client.run(
                'export SPARK_HOME="' + c.SPARK_HOME + '" && ./spark-perf/bin/run')
            print('runout\n{}\nrunerr:\n{}\runstatus:{}'.format(runout, runerr, runstatus))

            # FIND APP LOG FOLDER
            print("Finding log folder")
            app_log = between(runout, "2>> ", ".err")
            logfolder = "/home/ubuntu/" + "/".join(app_log.split("/")[:-1])
            print(logfolder)
            # output_folder = logfolder[1:] # this is the canonical name
            output_folder = "home/ubuntu/spark-bench/num/" # this is to match where current version log-processing searches for app logfiles 
        '''
        '''    
        if  profile and profile_fname != "" :
            # delete selected app_name profile file
            print("Deleting " + app_name + " app_name reference profile: " + profile_fname + "\n")
            stdout, stderr, status = ssh_client.run("sudo rm " + c.SPARK_HOME + "conf/" + profile_fname + ".json")
        '''                
        '''
        for bench in c.BENCHMARK_BENCH:
            ssh_client.run('rm -r ./spark-bench/num/*')
            for bc in c.BENCH_CONF[bench]:
                if bc != "NumTrials":
                    ssh_client.run(
                        "sed -i -r 's/ *{0} *= *.*/{0}={1}/' ./spark-bench/{2}/conf/env.sh""".format(
                            bc, c.BENCH_CONF[bench][bc], bench))
                        #"sed -i '{0}s/.*/{1}={2}/' ./spark-bench/{3}/conf/env.sh""".format(
                        #    c.BENCH_CONF[bench][bc][0], bc, c.BENCH_CONF[bench][bc][1],
                        #    bench))

            if delete_hdfs:
                print("Generating Data benchmark " + bench)
                ssh_client.run(
                    'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + c.PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + c.SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/gen_data.sh')

            check_slave_connected_master(ssh_client) #vboxvm_removed
            print("Running benchmark " + bench)
            ssh_client.run(                                                                                                                                                  #vboxvm_removed
               'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + c.PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + c.SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/run.sh') #vboxvm_removed
            logfolder = "/home/ubuntu/spark-bench/num"
            
            output_folder = "home/ubuntu/spark-bench/num/"
        '''
        if delete_hdfs:
            gen_data_option = "-gendata"   
        check_slave_connected_master(ssh_client)
        print("Running application " + app_name + " with gen_data_option = '" + gen_data_option + "'")
        sc_app_name = meta_profile_name if c.HEURISTIC.value == 3 else app_name
        stdout, stderr, status = ssh_client.run(                                                                                                                                                  
           'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + c.PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + c.SPARK_HOME + 
           '" && /usr/local/spark/bin/spark-submit --master spark://' + master_ip + ':7077' +
                                          ' --class ' + app_class + #it.polimi.deepse.dagsymb.launchers.Launcher ' +
                                          ' --conf spark.eventLog.enabled=true ' + app_jar + #./dagsymb/target/dagsymb-1.0-jar-with-dependencies.jar ' +
                                          ' ' + guard_evaluator_class + #'it.polimi.deepse.dagsymb.examples.GuardEvaluatorPromoCallsFile' + 
                                          ' ' + child_args_string + 
                                          ' ' + gen_data_option +  
                                          ' ' + sc_app_name + ' > ' + c.SPARK_HOME + 'logs/app.dat 2>&1 ') #it.polimi.deepse.dagsymb.examples.GuardEvaluatorPromoCallsFile 2003 2003 1610 1614 2990 3000 1006 1006 1006 2') 
                                          #' it.polimi.deepse.dagsymb.examples.GuardEvaluatorPromoCallsFile 2003 2003 1610 1614 2990 3000 1006 1006 1006 2') 
        print(stderr + stdout)
        #logfolder = "/home/ubuntu/dagsymb/num"
        logfolder = c.SPARK_HOME + "logs"
        output_folder = "home/ubuntu/dagsymb/num/"
        # stdout, stderr, status = ssh_client.run("sudo rm -r " + logfolder + "/*")
        # logfolder = "/home/ubuntu/spark-bench/num"
        # output_folder = "home/ubuntu/spark-bench/num/"
        # ensure there is no directory named 'old' in log folder
        stdout, stderr, status = ssh_client.run("cd "+ logfolder + " && sudo rm -r old") 
        #stdout, stderr, status = ssh_client.run("cd "+ c.CONFIG_DICT["Spark"]["SparkHome"] + "spark-events/ && sudo cp -a " +
        #                                         c.CONFIG_DICT["Spark"]["SparkHome"] + "event-logs/. ./ && sudo rename 's/$/_"+ str(session_no) + '_' + iter_num +"/' *") #vboxvm
        stdout, stderr, status = ssh_client.run("sudo chown ubuntu:ubuntu " + c.CONFIG_DICT["Spark"]["SparkHome"] + "conf")#check if needed
        #stdout, stderr, status = ssh_client.run("sudo chown ubuntu:ubuntu /usr/local/spark/conf")#check if needed
        #print("sudo chown ubuntu:ubuntu /usr/local/spark/conf:" +stdout+stderr)
        
        #DOWNLOAD LOGS
        if c.PROCESS_ON_SERVER:
            #master_public_ip = socket.gethostbyname("XSPARKWORK0") #vboxvm
            #ssh_client = sshclient_from_ip(master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm
            for file in os.listdir("./"):
                if file.endswith(".pickle"):
                    os.remove(file)
            stdout, stderr, status = ssh_client.run("cd xSpark-dagsymb && sudo rm  *.pickle")
            with open('nodes_ids.pickle', 'wb') as f:
                pickle.dump([n.id for n in [i for i in nodes[:end_index]]], f)       
            ssh_client.put(localpath="nodes_ids.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/nodes_ids.pickle")
            with open('logfolder.pickle', 'wb') as f:
                pickle.dump(logfolder, f)       
            ssh_client.put(localpath="logfolder.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/logfolder.pickle")
            with open('output_folder.pickle', 'wb') as f:
                pickle.dump(output_folder, f)       
            ssh_client.put(localpath="output_folder.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/output_folder.pickle")
            with open('master_ip.pickle', 'wb') as f:
                pickle.dump(master_ip, f)       
            ssh_client.put(localpath="master_ip.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/master_ip.pickle")
            
            #with open('config_instance.pickle', 'wb') as f:
            #    pickle.dump(c, f)       
            #ssh_client.put(localpath="config_instance.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/config_instance.pickle") 
            
            with open("cfg_dict.json", "w") as f:
                json.dump(c.cfg_dict, f, indent=4, sort_keys=True)
            ssh_client.put(localpath="cfg_dict.json", remotepath="/home/ubuntu/xSpark-dagsymb/cfg_dict.json")
                          
            stdout, stderr, status = ssh_client.run("cd xSpark-dagsymb && sudo python3 process_on_server.py")
            print("Processing on server:\n" + stdout + stderr)
            master_node = [i for i in nodes if get_ip(i) == master_ip][0] #vboxvm remove
            print("Downloading results from server...")
            for dir in ssh_client.listdir("xSpark-dagsymb/home/ubuntu/dagsymb/num/"):
                print("folder: " + str(dir))
                try:
                    os.makedirs(output_folder + "/" + dir)
                    for file in ssh_client.listdir("xSpark-dagsymb/home/ubuntu/dagsymb/num/" + dir + "/"):
                        output_file = (output_folder + dir + "/" + file)
                        print("file: " + output_file)
                        ssh_client.get(remotepath="xSpark-dagsymb/" + output_file, localpath=output_file)
                except FileExistsError:
                    print("Output folder already exists: no files downloaded")
            print("Updating local copy of cfg_clusters.ini")
            ssh_client.get(remotepath="xSpark-dagsymb/cfg_clusters.ini", localpath="cfg_clusters.ini")
            #for dir in ssh_client.listdir("xSpark-dagsymb/spark_log_profiling/"):
            #for dir in ["avg_json", "input_logs", "output_json", "processed_logs", "processed_profiles"]: 
            with open_cfg() as cfg:
                if profile and 'main' in cfg and 'iter_num' in cfg['main'] \
                           and 'num_run' in cfg['main'] and 'app_name' in cfg['experiment'] \
                           and 'experiment_num' in cfg['main'] and 'num_experiments' in cfg['main'] \
                           and int(cfg['main']['iter_num']) == int(cfg['main']['num_run']) \
                           or profile_option:
                           #and int(cfg['main']['experiment_num']) == int(cfg['main']['num_experiments']) \
                           #or profile_option:
                    for dir in ["avg_json", "input_logs", "output_json", "processed_profiles"]: # do not download logfiles in "processed_logs" folder
                        print("folder: " + dir)
                        make_sure_path_exists("spark_log_profiling/" + dir)
                        for file in ssh_client.listdir("xSpark-dagsymb/spark_log_profiling/" + dir + "/"):
                            fpath = "spark_log_profiling/" + dir + "/"
                            if file not in os.listdir(fpath) or dir == "avg_json":
                                ssh_client.get(remotepath="xSpark-dagsymb/" + fpath + file, localpath=fpath + file)
                                print("Remote file: ~/xSpark-dagsymb/" + fpath + file + " downloaded")
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
                try:
                    # PLOT LOGS
                    plot.plot(output_folder + "/")
                except Exception as e:
                    print("Plot failed: ", e)
                try:    
                    # COMPUTE METRICS
                    metrics.compute_metrics(output_folder + "/")
                except Exception as e:
                    print("Compute metrics failed: ", e)

        print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")
        
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
    benchmark = ""
    
    with open_cfg(mode='w') as cfg:
        current_cluster = cfg['main']['current_cluster']
        c.CLUSTER_ID = c.CLUSTER_MAP[current_cluster]
        
        if 'main' in cfg and 'experiment_file' in cfg['main']:
            exp_filepath = cfg['main']['experiment_file']
            c.config_experiment(exp_filepath, cfg)  
        
        setup = True if 'main' in cfg and 'setup' in cfg['main'] else False
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
        profile = True if 'profile' in cfg else False
        print("profile mode set to " + str(profile))
        profile_option = cfg.getboolean('main', 'profile') if 'main' in cfg and 'profile' in cfg['main'] else False
        #profile = cfg['main']['profile'] if 'main' in cfg and 'profile' in cfg['main'] else False
        print("Benchmark: " + benchmark)
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
    if current_cluster == 'spark' and not setup :
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
            # output_folder = logfolder[1:] # this is the canonical name
            output_folder = "home/ubuntu/spark-bench/num/" # this is to match where current version log-processing searches for app logfiles 
        '''    
        if  profile and profile_fname != "" :
            # delete selected benchmark profile file
            print("Deleting " + benchmark + " benchmark reference profile: " + profile_fname + "\n")
            stdout, stderr, status = ssh_client.run("sudo rm " + c.SPARK_HOME + "conf/" + profile_fname + ".json")
        '''                

        for bench in c.BENCHMARK_BENCH:
            ssh_client.run('rm -r ./spark-bench/num/*')
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

            check_slave_connected_master(ssh_client)
            print("Running Benchmark " + bench)
            ssh_client.run(
               'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + c.PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + c.SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/run.sh')
            logfolder = "/home/ubuntu/spark-bench/num"
            
            output_folder = "home/ubuntu/dagsymb/num/" #"/home/ubuntu/spark-bench/num"
            
        '''
        sess_file = Path("session.txt")
        session_no = 0
        if sess_file.exists():
            with open("session.txt", 'r') as f:
                fc = f.read()
                session_no = int(fc)
                f.close()
        '''                
        # stdout, stderr, status = ssh_client.run("sudo rm -r " + logfolder + "/*")
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
            stdout, stderr, status = ssh_client.run("cd xSpark-dagsymb && sudo rm  *.pickle")
            with open('nodes_ids.pickle', 'wb') as f:
                pickle.dump([n.id for n in [i for i in nodes[:end_index]]], f)       
            ssh_client.put(localpath="nodes_ids.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/nodes_ids.pickle")
            with open('logfolder.pickle', 'wb') as f:
                pickle.dump(logfolder, f)       
            ssh_client.put(localpath="logfolder.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/logfolder.pickle")
            with open('output_folder.pickle', 'wb') as f:
                pickle.dump(output_folder, f)       
            ssh_client.put(localpath="output_folder.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/output_folder.pickle")
            with open('master_ip.pickle', 'wb') as f:
                pickle.dump(master_ip, f)       
            ssh_client.put(localpath="master_ip.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/master_ip.pickle")
            
            #with open('config_instance.pickle', 'wb') as f:
            #    pickle.dump(c, f)       
            #ssh_client.put(localpath="config_instance.pickle", remotepath="/home/ubuntu/xSpark-dagsymb/config_instance.pickle") 
            
            with open("cfg_dict.json", "w") as f:
                json.dump(c.cfg_dict, f, indent=4, sort_keys=True)
            ssh_client.put(localpath="cfg_dict.json", remotepath="/home/ubuntu/xSpark-dagsymb/cfg_dict.json")
                          
            stdout, stderr, status = ssh_client.run("cd xSpark-dagsymb && sudo python3 process_on_server.py")
            print("Processing on server:\n" + stdout + stderr)
            master_node = [i for i in nodes if get_ip(i) == master_ip][0] #vboxvm remove
            print("Downloading results from server...")
            for dir in ssh_client.listdir("xSpark-dagsymb/home/ubuntu/dagsymb/num/"):
                print("folder: " + str(dir))
                try:
                    os.makedirs(output_folder + dir)
                    for file in ssh_client.listdir("xSpark-dagsymb/home/ubuntu/dagsymb/num/" + dir + "/"):
                        output_file = (output_folder + dir + "/" + file)
                        print("file: " + output_file)
                        ssh_client.get(remotepath="xSpark-dagsymb/" + output_file, localpath=output_file)
                except FileExistsError:
                    print("Output folder already exists: no files downloaded")
            print("Updating local copy of cfg_clusters.ini")
            ssh_client.get(remotepath="xSpark-dagsymb/cfg_clusters.ini", localpath="cfg_clusters.ini")
            #for dir in ssh_client.listdir("xSpark-dagsymb/spark_log_profiling/"):
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
                        for file in ssh_client.listdir("xSpark-dagsymb/spark_log_profiling/" + dir + "/"):
                            fpath = "spark_log_profiling/" + dir + "/"
                            if file not in os.listdir(fpath) or dir == "avg_json":
                                ssh_client.get(remotepath="xSpark-dagsymb/" + fpath + file, localpath=fpath + file)
                                print("Remote file: ~/xSpark-dagsymb/" + fpath + file + " downloaded")
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
                try:
                    # PLOT LOGS
                    plot.plot(output_folder + "/")
                except Exception as e:
                    print("Plot failed: ", e)
                try:    
                    # COMPUTE METRICS
                    metrics.compute_metrics(output_folder + "/")
                except Exception as e:
                    print("Compute metrics failed: ", e)

        print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")
  
        