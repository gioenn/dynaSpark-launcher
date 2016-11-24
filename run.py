"""
This module handle the configuration of the instances and the execution of  the benchmark on the cluster
"""

import json
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from boto.manage.cmdshell import sshclient_from_instance

import log
import metrics
import plot
from config import UPDATE_SPARK_DOCKER, DELETE_HDFS, SPARK_HOME, KILL_JAVA, SYNC_TIME, \
    KEYPAIR_PATH, \
    UPDATE_SPARK, \
    DISABLEHT, ENABLE_EXTERNAL_SHUFFLE, OFF_HEAP, OFF_HEAP_BYTES, K, TSAMPLE, TI, COREQUANTUM, \
    COREMIN, CPU_PERIOD, HDFS, \
    COREVM, UPDATE_SPARK_MASTER, DEADLINE, MAXEXECUTOR, ALPHA, OVERSCALE, LOCALITY_WAIT, \
    LOCALITY_WAIT_NODE, CPU_TASK, \
    LOCALITY_WAIT_PROCESS, LOCALITY_WAIT_RACK, INPUT_RECORD, NUM_TASK, BENCH_NUM_TRIALS, \
    SCALE_FACTOR, RAM_EXEC, \
    RAM_DRIVER, BENCHMARK_PERF, BENCH_LINES, HDFS_MASTER, DATA_AMI, REGION, HADOOP_CONF, \
    CONFIG_DICT, CREDENTIAL_PROFILE, \
    CLUSTER_ID, SPARK_2, BENCHMARK_BENCH, BENCH_CONF, LOG_LEVEL
from util.utils import timing, between


def common_setup(ssh_client):
    """
    Common setup of the instance of the cluster with ssh_client is connected

    :param ssh_client: the ssh client to launch command on the instance
    :return: nothing
    """
    ssh_client.run("export GOMAXPROCS=`nproc`")

    if UPDATE_SPARK_DOCKER:
        print("   Updating Spark Docker Image...")
        ssh_client.run("docker pull elfolink/spark:2.0")

    if DELETE_HDFS:
        ssh_client.run("sudo umount /mnt")
        ssh_client.run(
            "sudo mkfs.ext4 -E nodiscard /dev/xvdb && sudo mount -o discard /dev/xvdb /mnt")
    ssh_client.run("test -d /mnt/tmp || sudo mkdir -m 1777 /mnt/tmp")
    ssh_client.run("sudo mount --bind /mnt/tmp /tmp")

    ssh_client.run('ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R localhost')

    print("   Stop Spark Slave/Master")
    ssh_client.run('export SPARK_HOME="' + SPARK_HOME + '" && ' + SPARK_HOME + 'sbin/stop-slave.sh')
    ssh_client.run(
        'export SPARK_HOME="' + SPARK_HOME + '" && ' + SPARK_HOME + 'sbin/stop-master.sh')

    print("   Set Log Level")
    ssh_client.run(
        "sed -i '31s{.*{log4j.rootCategory=" + str(
            LOG_LEVEL) + ", console {' " + SPARK_HOME + "conf/log4j.properties")

    if KILL_JAVA:
        print("   Killing Java")
        ssh_client.run('sudo killall java && sudo killall java && sudo killall java')

    print("   Kill SAR CPU Logger")
    ssh_client.run("screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")

    if SYNC_TIME:
        print("   SYNC TIME")
        ssh_client.run("sudo ntpdate -s time.nist.gov")

    print("   Removing Stopped Docker")
    ssh_client.run("docker ps -a | awk '{print $1}' | xargs --no-run-if-empty docker rm")


@timing
def setup_slave(instance, master_dns):
    """

    :param instance:
    :param master_dns:
    :return:
    """
    ssh_client = sshclient_from_instance(instance, KEYPAIR_PATH, user_name='ubuntu')

    print("Setup Slave: " + instance.public_dns_name)

    common_setup(ssh_client)

    if UPDATE_SPARK:
        print("   Updating Spark...")
        ssh_client.run(
            "cd /usr/local/spark && git pull &&  build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package")

    # CLEAN UP EXECUTORS APP LOGS
    ssh_client.run("rm -r " + SPARK_HOME + "work/*")

    if DISABLEHT:
        # DISABLE HT
        ssh_client.put_file("./disable-ht-v2.sh", "./disable-ht-v2.sh")
        ssh_client.run("chmod +x disable-ht-v2.sh")
        status, stdout, stderr = ssh_client.run('sudo ./disable-ht-v2.sh')
        # status, stdout, stderr = ssh_client.run('sudo ./disable-ht.sh')
        print("   Disabled HyperThreading")

    # ssh_client.run("sed -i '47d' "+ SPARK_HOME + "conf/spark-defaults.conf")
    # ssh_client.run("sed -i '47d' " + SPARK_HOME + "conf/spark-defaults.conf")
    ssh_client.run(
        "sed -i '47s{.*{spark.shuffle.service.enabled " + ENABLE_EXTERNAL_SHUFFLE + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    # ssh_client.run('echo "spark.local.dir /mnt/hdfs" >> '+ SPARK_HOME + 'conf/spark-defaults.conf')

    ssh_client.run(
        "sed -i '31s{.*{spark.memory.offHeap.enabled " + str(
            OFF_HEAP) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
    ssh_client.run(
        "sed -i '32s{.*{spark.memory.offHeap.size " + str(
            OFF_HEAP_BYTES) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '42s{.*{spark.control.k " + str(
        K) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    # SAMPLING RATE LINE 43
    ssh_client.run(
        "sed -i '43s{.*{spark.control.tsample " + str(
            TSAMPLE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '44s{.*{spark.control.ti " + str(
        TI) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '45s{.*{spark.control.corequantum " + str(
        COREQUANTUM) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '50s{.*{spark.control.coremin " + str(
        COREMIN) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '50s{.*{spark.control.cpuperiod " + str(
        CPU_PERIOD) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    if HDFS == 0:
        print("   Starting Spark Slave")
        ssh_client.run(
            'export SPARK_HOME="' + SPARK_HOME + '" && ' + SPARK_HOME + 'sbin/start-slave.sh ' + master_dns + ':7077 -h ' +
            instance.public_dns_name + ' --port 9999 -c ' + str(
                COREVM))

        # REAL CPU LOG
        logcpucommand = 'screen -d -m -S "' + instance.private_ip_address + '" bash -c "sar -u 1 > sar-' + instance.private_ip_address + '.log"'
        # print(logcpucommand)
        print("   Start SAR CPU Logging")
        ssh_client.run(logcpucommand)


@timing
def setup_master(instance):
    """

    :param instance:
    :return:
    """
    ssh_client = sshclient_from_instance(instance, KEYPAIR_PATH, user_name='ubuntu')

    print("Setup Master: " + instance.public_dns_name)

    common_setup(ssh_client)

    if UPDATE_SPARK_MASTER:
        print("   Updating Spark...")
        ssh_client.run(
            "cd /usr/local/spark && git pull &&  build/mvn -T 1C -Phive  -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package")

    print("   Remove Logs")
    ssh_client.run("rm " + SPARK_HOME + "spark-events/*")

    # TODO Check number of lines in spark-defaults.conf

    # SHUFFLE SERVICE EXTERNAL
    ssh_client.run(
        "sed -i '31s{.*{spark.shuffle.service.enabled " + ENABLE_EXTERNAL_SHUFFLE + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    # OFF HEAP
    ssh_client.run(
        "sed -i '32s{.*{spark.memory.offHeap.enabled " + str(
            OFF_HEAP) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
    ssh_client.run(
        "sed -i '33s{.*{spark.memory.offHeap.size " + str(
            OFF_HEAP_BYTES) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    print("   Changing Benchmark settings")
    # DEADLINE LINE 35
    ssh_client.run("sed -i '35s{.*{spark.control.deadline " + str(
        DEADLINE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
    # SED "sed -i 's/^\(spark\.control\.deadline*\).*$/\1 140000/' "+SPARK_HOME+"conf/spark-defaults.conf"
    # MAX EXECUTOR LINE 39
    ssh_client.run("sed -i '39s{.*{spark.control.maxexecutor " + str(
        MAXEXECUTOR) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
    # SED "sed -i 's/^\(spark\.control\.maxexecutor*\).*$/\1 4/' "+SPARK_HOME+"conf/spark-defaults.conf"
    # CORE FOR VM LINE 40
    ssh_client.run("sed -i '40s{.*{spark.control.coreforvm " + str(
        COREVM) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
    # SED "sed -i 's/^\(spark\.control\.coreforvm*\).*$/\1 8/' "+SPARK_HOME+"conf/spark-defaults.conf"

    # ALPHA LINE 36
    ssh_client.run("sed -i '36s{.*{spark.control.alpha " + str(
        ALPHA) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
    # OVERSCALE LINE 38
    ssh_client.run("sed -i '38s{.*{spark.control.overscale " + str(
        OVERSCALE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    # CHANGE ALSO IN MASTER FOR THE LOGS
    ssh_client.run(
        "sed -i '43s{.*{spark.control.tsample " + str(
            TSAMPLE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '42s{.*{spark.control.k " + str(
        K) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '44s{.*{spark.control.ti " + str(
        TI) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '45s{.*{spark.control.corequantum " + str(
        COREQUANTUM) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '46s{.*{spark.locality.wait " + str(
            LOCALITY_WAIT) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '51s{.*{spark.locality.wait.node " + str(
            LOCALITY_WAIT_NODE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '52s{.*{spark.locality.wait.process " + str(
            LOCALITY_WAIT_PROCESS) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '53s{.*{spark.locality.wait.rack " + str(
            LOCALITY_WAIT_RACK) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '47s{.*{spark.task.cpus " + str(
            CPU_TASK) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '48s{.*{spark.control.nominalrate 0.0{' " + SPARK_HOME + "conf/spark-defaults.conf")
    ssh_client.run(
        "sed -i '49s{.*{spark.control.nominalratedata 0.0{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("sed -i '50s{.*{spark.control.coremin " + str(
        COREMIN) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '54s{.*{spark.control.inputrecord " + str(
            INPUT_RECORD) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '55s{.*{spark.control.numtask " + str(
            NUM_TASK) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("""sed -i '3s{.*{master=""" + instance.public_dns_name +
                   """{' ./spark-bench/conf/env.sh""")
    ssh_client.run("""sed -i '63s{.*{NUM_TRIALS=""" + str(BENCH_NUM_TRIALS) +
                   """{' ./spark-bench/conf/env.sh""")

    # CHANGE MASTER ADDRESS IN BENCHMARK
    ssh_client.run("""sed -i '31s{.*{SPARK_CLUSTER_URL = "spark://""" + instance.public_dns_name +
                   """:7077"{' ./spark-perf/config/config.py""")

    # CHANGE SCALE FACTOR LINE 127
    ssh_client.run(
        "sed -i '127s{.*{SCALE_FACTOR = " + str(SCALE_FACTOR) + "{' ./spark-perf/config/config.py")

    # NO PROMPT
    ssh_client.run("sed -i '103s{.*{PROMPT_FOR_DELETES = False{' ./spark-perf/config/config.py")

    # CHANGE RAM EXEC
    ssh_client.run(
        """sed -i '147s{.*{JavaOptionSet("spark.executor.memory", [""" + RAM_EXEC + """]),{' ./spark-perf/config/config.py""")

    ssh_client.run(
        """sed -i '55s{.*{SPARK_EXECUTOR_MEMORY=""" + RAM_EXEC + """{' ./spark-bench/conf/env.sh""")

    # CHANGE RAM DRIVER
    ssh_client.run(
        "sed -i '26s{.*{spark.driver.memory " + RAM_DRIVER + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    print("   Enabling/Disabling Benchmark")
    # ENABLE BENCHMARK
    for bench in BENCHMARK_PERF:
        for line_number in BENCH_LINES[bench]:
            sed_command_line = "sed -i '" + line_number + " s/[#]//g' ./spark-perf/config/config.py"
            ssh_client.run(sed_command_line)

    # DISABLE BENCHMARK
    for bench in BENCH_LINES.keys():
        if bench not in BENCHMARK_PERF:
            for line_number in BENCH_LINES[bench]:
                ssh_client.run("sed -i '" + line_number + " s/^/#/' ./spark-perf/config/config.py")

    # ENABLE HDFS
    # if HDFS:
    print("   Enabling HDFS in benchmarks")
    ssh_client.run("sed -i '180s%memory%hdfs%g' ./spark-perf/config/config.py")
    ssh_client.run(
        """sed -i  '50s%.*%HDFS_URL = "hdfs://""" + instance.public_dns_name + """:9000/test/"%' ./spark-perf/config/config.py""")
    if HDFS_MASTER != "":
        ssh_client.run(
            """sed -i  '50s%.*%HDFS_URL = "hdfs://""" + HDFS_MASTER + """:9000/test/"%' ./spark-perf/config/config.py""")
        ssh_client.run(
            """sed -i  '10s%.*%HDFS_URL="hdfs://""" + HDFS_MASTER + """:9000"%' ./spark-bench/conf/env.sh""")
        ssh_client.run(
            """sed -i  '14s%.*%DATA_HDFS="hdfs://""" + HDFS_MASTER + """:9000/SparkBench"%' ./spark-bench/conf/env.sh""")

    # START MASTER
    if HDFS == 0:
        print("   Starting Spark Master")
        ssh_client.run(
            'export SPARK_HOME="' + SPARK_HOME + '" && ' + SPARK_HOME + 'sbin/start-master.sh -h ' + instance.public_dns_name)

    return instance.public_dns_name, instance


@timing
def setup_hdfs_ssd(instance):
    """

    :param instance:
    :return:
    """
    ssh_client = sshclient_from_instance(instance, KEYPAIR_PATH, user_name='ubuntu')
    status, out, err = ssh_client.run(
        "test -d /mnt/hdfs/namenode || sudo mkdir --parents /mnt/hdfs/namenode && sudo mkdir --parents /mnt/hdfs/datanode")
    if status != 0:
        print(out, err)
    ssh_client.run("sudo chown ubuntu:hadoop /mnt/hdfs && sudo chown ubuntu:hadoop /mnt/hdfs/*")
    if DELETE_HDFS or HDFS_MASTER == "":
        ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")


def rsync_folder(ssh_client, slave):
    """

    :param ssh_client:
    :param slave:
    :return:
    """
    ssh_client.run(
        "eval `ssh-agent -s` && ssh-add " + DATA_AMI[REGION][
            "keypair"] + ".pem && rsync -a " + HADOOP_CONF + " ubuntu@" + slave + ":" + HADOOP_CONF)
    if DELETE_HDFS:
        ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")


@timing
def setup_hdfs_config(master_instance, slaves):
    """

    :param master_instance:
    :param slaves:
    :return:
    """
    ssh_client = sshclient_from_instance(master_instance, KEYPAIR_PATH, user_name='ubuntu')
    if HDFS_MASTER == "":
        master_dns = master_instance.public_dns_name
    else:
        master_dns = HDFS_MASTER

    # Setup Config HDFS
    ssh_client.run(
        "sed -i '19s%.*%<configuration>  <property>    <name>fs.default.name</name>    <value>hdfs://" + master_dns + ":9000</value>  </property>%g' " + HADOOP_CONF + "core-site.xml")
    # 19 <configuration>  <property>    <name>fs.default.name</name>    <value>hdfs://ec2-54-70-105-139.us-west-2.compute.amazonaws.com:9000</value>  </property>

    ssh_client.run(
        "sed -i '38s%.*%<value>" + master_dns + ":50070</value>%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '43s%.*%<value>" + master_dns + ":50090</value>%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '48s%.*%<value>" + master_dns + ":9000</value>%g' " + HADOOP_CONF + "hdfs-site.xml")
    # 38  <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:50070</value>
    # 43   <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:50090</value>
    # 48  <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:9000</value>

    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/namenode%/mnt/hdfs/namenode%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/datanode%/mnt/hdfs/datanode%g' " + HADOOP_CONF + "hdfs-site.xml")

    print(slaves)
    ssh_client.run("echo -e '" + "\n".join(slaves) + "' > " + HADOOP_CONF + "slaves")

    ssh_client.run(
        "echo 'Host *\n  UserKnownHostsFile /dev/null\n  StrictHostKeyChecking no' > ~/.ssh/config")

    # Rsync Config
    with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
        for slave in slaves:
            executor.submit(rsync_folder, ssh_client, slave)

    # Start HDFS
    if DELETE_HDFS or HDFS_MASTER == "":
        ssh_client.run(
            "eval `ssh-agent -s` && ssh-add " + DATA_AMI[REGION][
                "keypair"] + ".pem && /usr/local/lib/hadoop-2.7.2/sbin/stop-dfs.sh")
        ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("echo 'N' | /usr/local/lib/hadoop-2.7.2/bin/hdfs namenode -format")

    status, out, err = ssh_client.run(
        "eval `ssh-agent -s` && ssh-add " + DATA_AMI[REGION][
            "keypair"] + ".pem && /usr/local/lib/hadoop-2.7.2/sbin/start-dfs.sh && /usr/local/lib/hadoop-2.7.2/bin/hdfs dfsadmin -safemode leave")
    if status != 0:
        print(out, err)
    print("   Started HDFS")

    if DELETE_HDFS:
        print("   Cleaned HDFS")
        if len(BENCHMARK_PERF) > 0:
            status, out, err = ssh_client.run(
                "/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
            print(status, out, err)


def write_config(output_folder):
    """

    :param output_folder:
    :return:
    """
    with open(output_folder + "/config.json", "w") as config_out:
        json.dump(CONFIG_DICT, config_out, sort_keys=True, indent=4)


def check_slave_connected_master(ssh_client):
    pass


@timing
def run_benchmark():
    """

    :return:
    """
    session = boto3.Session(profile_name=CREDENTIAL_PROFILE)
    ec2 = session.resource('ec2', region_name=REGION)
    instances = ec2.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                 {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}
                 ])

    instance_list = list(instances)
    print("Instance Found: " + str(len(instance_list)))
    if len(list(instances)) == 0:
        print("No instances running")
        exit(1)

    master_dns, master_instance = setup_master(instance_list[0])
    if SPARK_HOME == SPARK_2:
        print("Check Effectively Executor Running")

    end_index = min(len(instance_list), MAXEXECUTOR + 1)
    with ThreadPoolExecutor(8) as executor:
        for i in instance_list[1:end_index]:
            if i.public_dns_name != master_dns:
                executor.submit(setup_slave, i, master_dns)

    with ThreadPoolExecutor(8) as executor:
        for i in instance_list[end_index:]:
            if i.public_dns_name != master_dns:
                ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')
                executor.submit(common_setup, ssh_client)

    if HDFS or HDFS_MASTER == master_dns:
        print("\nStarting Setup of HDFS cluster")
        # Format instance store SSD for hdfs usage
        with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            for i in instances:
                executor.submit(setup_hdfs_ssd, i)

        slaves = [i.public_dns_name for i in instance_list[:end_index]]
        slaves.remove(master_dns)
        setup_hdfs_config(master_instance, slaves)

    time.sleep(15)

    print("MASTER: " + master_dns)
    ssh_client = sshclient_from_instance(master_instance, KEYPAIR_PATH, user_name='ubuntu')
    #  CHECK IF KEY IN MASTER
    status = ssh_client.run('[ ! -e %s ]; echo $?' % (DATA_AMI[REGION]["keypair"] + ".pem"))
    if not int(status[1].decode('utf8').replace("\n", "")):
        ssh_client.put_file(KEYPAIR_PATH, "/home/ubuntu/" + DATA_AMI[REGION]["keypair"] + ".pem")

    # LANCIARE BENCHMARK
    if HDFS == 0:
        if len(BENCHMARK_PERF) > 0:
            print("Running Benchmark " + str(BENCHMARK_PERF))
            runstatus, runout, runerr = ssh_client.run(
                'export SPARK_HOME="' + SPARK_HOME + '" && ./spark-perf/bin/run')

            # FIND APP LOG FOLDER
            print("Finding log folder")
            app_log = between(runout, b"2>> ", b".err").decode(encoding='UTF-8')
            logfolder = "./" + "/".join(app_log.split("/")[:-1])
            print(logfolder)
            output_folder = logfolder

        for bench in BENCHMARK_BENCH:
            ssh_client.run('rm -r ./spark-bench/num/*')

            for config in BENCH_CONF[bench].keys():
                if config != "NumTrials":
                    ssh_client.run(
                        """sed -i '""" + str(
                            BENCH_CONF[bench][config][0]) + """s{.*{""" + config + """=""" + str(
                            BENCH_CONF[bench][config][1]) +
                        """{' ./spark-bench/""" + bench + """/conf/env.sh""")

            if DELETE_HDFS:
                print("Generating Data Benchmark " + bench)
                ssh_client.run(
                    'eval `ssh-agent -s` && ssh-add ' + DATA_AMI[REGION][
                        "keypair"] + '.pem && export SPARK_HOME="' + SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/gen_data.sh')

            check_slave_connected_master(ssh_client)
            print("Running Benchmark " + bench)
            ssh_client.run(
                'eval `ssh-agent -s` && ssh-add ' + DATA_AMI[REGION][
                    "keypair"] + '.pem && export SPARK_HOME="' + SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/run.sh')
            logfolder = "./spark-bench/num"
            output_folder = "./spark-bench/num/"

        # DOWNLOAD LOGS
        output_folder = log.download(logfolder, [i for i in instance_list[:end_index]], master_dns,
                                     output_folder)

        write_config(output_folder)

        # PLOT LOGS
        plot.plot(output_folder + "/")

        # COMPUTE METRICS
        metrics.compute_metrics(output_folder + "/")

        print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")
