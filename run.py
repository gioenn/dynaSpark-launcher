import multiprocessing
import os
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from boto.manage.cmdshell import sshclient_from_instance

import log
import plot
from config import *


def between(value, a, b):
    print("Finding log folder")
    # Find and validate before-part.
    pos_a = value.find(a)
    if pos_a == -1: return ""
    # Find and validate after part.
    pos_b = value.find(b)
    if pos_b == -1: return ""
    # Return middle part.
    adjusted_pos_a = pos_a + len(a)
    if adjusted_pos_a >= pos_b: return ""
    return value[adjusted_pos_a:pos_b]


def common_setup(ssh_client):
    if UPDATE_SPARK:
        ssh_client.run(
            "cd /usr/local/spark && git pull &&  build/mvn -T 1C -Phive  -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package")
    if UPDATE_SPARK_DOCKER:
        ssh_client.run("docker pull elfolink/spark:2.0")

    if DELETE_HDFS:
        ssh_client.run("sudo umount /mnt")
        ssh_client.run("sudo mkfs.ext4 -E nodiscard /dev/xvdb && sudo mount -o discard /dev/xvdb /mnt")
    ssh_client.run("test -d /mnt/tmp || sudo mkdir -m 1777 /mnt/tmp")
    ssh_client.run("sudo mount --bind /mnt/tmp /tmp")

    ssh_client.run('ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R localhost')

    print("   Killing Java")
    ssh_client.run('sudo killall java && sudo killall java && sudo killall java')

    print("   Kill SAR CPU Logger")
    ssh_client.run("screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")

    print("   SYNC TIME")
    ssh_client.run("sudo ntpdate -s time.nist.gov")


def setup_slave(instance, master_dns, z):
    ssh_client = sshclient_from_instance(instance, KEYPAIR_PATH, user_name='ubuntu')

    common_setup(ssh_client)

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

    if z < MAXEXECUTOR:
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


def setup_master(instance):
    ssh_client = sshclient_from_instance(instance, KEYPAIR_PATH, user_name='ubuntu')

    print("Setup Master: " + instance.public_dns_name)

    common_setup(ssh_client)

    print("   Remove Logs")
    ssh_client.run("rm " + SPARK_HOME + "spark-events/*")

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
        "sed -i '46s{.*{spark.locality.wait " + str(LOCALITY_WAIT) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '47s{.*{spark.task.cpus " + str(CPU_TASK) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run(
        "sed -i '48s{.*{spark.control.nominalrate 0.0{' " + SPARK_HOME + "conf/spark-defaults.conf")
    ssh_client.run(
        "sed -i '49s{.*{spark.control.nominalratedata 0.0{' " + SPARK_HOME + "conf/spark-defaults.conf")

    ssh_client.run("""sed -i '3s{.*{master=""" + instance.public_dns_name +
                   """{' ./spark-bench/conf/env.sh""")
    ssh_client.run("""sed -i '63s{.*{NUM_TRIALS=""" + str(BENCH_NUM_TRIALS) +
                   """{' ./spark-bench/conf/env.sh""")

    # CHANGE MASTER ADDRESS IN BENCHMARK
    ssh_client.run("""sed -i '31s{.*{SPARK_CLUSTER_URL = "spark://""" + instance.public_dns_name +
                   """:7077"{' ./spark-perf/config/config.py""")

    # CHANGE SCALE FACTOR LINE 127
    ssh_client.run("sed -i '127s{.*{SCALE_FACTOR = " + str(SCALE_FACTOR) + "{' ./spark-perf/config/config.py")

    # NO PROMPT
    ssh_client.run("sed -i '103s{.*{PROMPT_FOR_DELETES = False{' ./spark-perf/config/config.py")

    # CHANGE RAM EXEC
    ssh_client.run(
        """sed -i '147s{.*{JavaOptionSet("spark.executor.memory", [""" + RAM_EXEC + """]),{' ./spark-perf/config/config.py""")

    ssh_client.run(
        """sed -i '55s{.*{SPARK_EXECUTOR_MEMORY=""" + RAM_EXEC + """{' ./spark-bench/conf/env.sh""")

    print("   Enabling/Disabling Benchmark")
    # ENABLE BENCHMARK
    for bench in BENCHMARK_PERF:
        for lineNumber in linesBench[bench]:
            commandLineSed = "sed -i '" + lineNumber + " s/[#]//g' ./spark-perf/config/config.py"
            ssh_client.run(commandLineSed)

    # DISABLE BENCHMARK
    for bench in linesBench.keys():
        if bench not in BENCHMARK_PERF:
            for lineNumber in linesBench[bench]:
                ssh_client.run("sed -i '" + lineNumber + " s/^/#/' ./spark-perf/config/config.py")

    # ENABLE HDFS
    if HDFS:
        print("   Enabling HDFS in benchmarks")
        ssh_client.run("sed -i '180s%memory%hdfs%g' ./spark-perf/config/config.py")
        ssh_client.run(
            """sed -i  '50s%.*%HDFS_URL = "hdfs://""" + instance.public_dns_name + """:9000/test/"%' ./spark-perf/config/config.py""")

    # START MASTER
    print("   Starting Spark Master")
    ssh_client.run(
        'export SPARK_HOME="' + SPARK_HOME + '" && ' + SPARK_HOME + 'sbin/start-master.sh -h ' + instance.public_dns_name)

    return instance.public_dns_name, instance


def setup_hdfs_ssd(instance):
    ssh_client = sshclient_from_instance(instance, KEYPAIR_PATH, user_name='ubuntu')
    ssh_client.run(
        "test -d /mnt/hdfs/namenode || sudo mkdir --parents /mnt/hdfs/namenode && sudo mkdir --parents /mnt/hdfs/datanode")
    ssh_client.run("sudo chown ubuntu:hadoop /mnt/hdfs && sudo chown ubuntu:hadoop /mnt/hdfs/*")


def setup_hdfs_config(master_instance, slaves):
    ssh_client = sshclient_from_instance(master_instance, KEYPAIR_PATH, user_name='ubuntu')
    master_dns = master_instance.public_dns_name
    HADOOP_CONF = "/usr/local/lib/hadoop-2.7.2/etc/hadoop/"

    # Setup Config
    ssh_client.run(
        "sed -i '19s%hdfs://ec2-54-70-105-139.us-west-2.compute.amazonaws.com:9000%hdfs://" + master_dns + ":9000%g' " + HADOOP_CONF + "core-site.xml")
    ssh_client.run(
        "sed -i 's%ec2-54-70-105-139.us-west-2.compute.amazonaws.com%" + master_dns + "%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/namenode%/mnt/hdfs/namenode%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/datanode%/mnt/hdfs/datanode%g' " + HADOOP_CONF + "hdfs-site.xml")

    print(slaves)
    ssh_client.run("echo -e '" + "\n".join(slaves) + "' > " + HADOOP_CONF + "slaves")

    ssh_client.run(
        "echo 'Host *\n  UserKnownHostsFile /dev/null\n  StrictHostKeyChecking no' > ~/.ssh/config")
    # Rsync Config
    for slave in slaves:
        status, out, err = ssh_client.run(
            "eval `ssh-agent -s` && ssh-add gazzetta.pem && rsync -a " + HADOOP_CONF + " ubuntu@" + slave + ":" + HADOOP_CONF)
        # print(status, out, err)

    # Start HDFS
    if DELETE_HDFS:
        ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("echo 'N' | /usr/local/lib/hadoop-2.7.2/bin/hdfs namenode -format")

    status, out, err = ssh_client.run(
        "eval `ssh-agent -s` && ssh-add gazzetta.pem && /usr/local/lib/hadoop-2.7.2/sbin/start-dfs.sh && /usr/local/lib/hadoop-2.7.2/bin/hdfs dfsadmin -safemode leave")
    # print(status, out, err)
    print("   Started HDFS")

    if DELETE_HDFS:
        print("   Cleaned HDFS")
        if len(BENCHMARK_PERF) > 0:
            status, out, err = ssh_client.run(
                "/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
            print(status, out, err)


def runbenchmark():
    ec2 = boto3.resource('ec2', region_name=REGION)
    instances = ec2.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

    if len(list(instances)) == 0:
        print("No instances running")
        exit(1)

    master_dns, master_instance = setup_master(list(instances)[0])
    z = 0
    with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
        for i in instances:
            if i.public_dns_name != master_dns:
                executor.submit(setup_slave, i, master_dns, z)
                z += 1

    if HDFS:
        print("\nStarting Setup of HDFS cluster")
        # Format instance store SSD for hdfs usage
        with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            for i in instances:
                executor.submit(setup_hdfs_ssd, i)

        slaves = [i.public_dns_name for i in instances]
        slaves.remove(master_dns)
        setup_hdfs_config(master_instance, slaves)

    time.sleep(15)

    print("MASTER: " + master_dns)
    ssh_client = sshclient_from_instance(master_instance, KEYPAIR_PATH, user_name='ubuntu')

    # LANCIARE BENCHMARK
    if len(BENCHMARK_PERF) > 0:
        print("Running Benchmark...")
        runstatus, runout, runerr = ssh_client.run('export SPARK_HOME="' + SPARK_HOME + '" && ./spark-perf/bin/run')

        # FIND APP LOG FOLDER
        app_log = between(runout, b"2>> ", b".err").decode(encoding='UTF-8')
        logfolder = "./" + "/".join(app_log.split("/")[:-1])
        print(logfolder)
        output_folder = logfolder

    for bench in BENCHMARK_BENCH:
        ssh_client.run('rm -r ./spark-bench/num/*')

        for config in benchConf[bench].keys():
            ssh_client.run("""sed -i '"""+ str(benchConf[bench][config][0])+ """s{.*{"""+config+""""=""" + str(benchConf[bench][config][1]) +
                           """{' ./spark-bench/"""+bench+"""/conf/env.sh""")

        if DELETE_HDFS:
            print("Generating Data Benchmark " + bench)
            ssh_client.run(
                'eval `ssh-agent -s` && ssh-add gazzetta.pem && export SPARK_HOME="' + SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/gen_data.sh')

        print("Running Benchmark " + bench)
        ssh_client.run(
            'eval `ssh-agent -s` && ssh-add gazzetta.pem && export SPARK_HOME="' + SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/run.sh')
        logfolder = "./spark-bench/num"
        output_folder = "./spark-bench/num/"

    # DOWNLOAD LOGS
    output_folder = log.download(logfolder, instances, master_dns, output_folder)

    # PLOT LOGS
    plot.plot(output_folder + "/")

    print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")
