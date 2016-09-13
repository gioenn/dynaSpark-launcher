import boto3
import time
import plot
from boto.manage.cmdshell import sshclient_from_instance
import os

from config import *


def between(value, a, b):
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


def runbenchmark():
    ec2 = boto3.resource('ec2', region_name=REGION)
    instances = ec2.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

    z = 0
    master_dns = ""
    for i in instances:
        print(z, i.public_dns_name)
        ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')

        ssh_client.run('ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R localhost')

        print("   Killing Java")
        ssh_client.run('sudo killall java && sudo killall java && sudo killall java')

        print("   Kill SAR CPU Logger")
        ssh_client.run("screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")

        print("   SYNC TIME")
        ssh_client.run("sudo ntpdate -s time.nist.gov")
        if z == 0:
            master_instance = i
            master_dns = i.public_dns_name

            print("   Remove Logs")
            ssh_client.run("rm /usr/local/spark/spark-events/*")

            print("   Changing Benchmark settings")
            # DEADLINE LINE 35
            ssh_client.run("sed -i '35s{.*{spark.control.deadline " + str(
                DEADLINE) + "{' /usr/local/spark/conf/spark-defaults.conf")
            # SED "sed -i 's/^\(spark\.control\.deadline*\).*$/\1 140000/' /usr/local/spark/conf/spark-defaults.conf"
            # MAX EXECUTOR LINE 39
            ssh_client.run("sed -i '39s{.*{spark.control.maxexecutor " + str(
                MAXEXECUTOR) + "{' /usr/local/spark/conf/spark-defaults.conf")
            # SED "sed -i 's/^\(spark\.control\.maxexecutor*\).*$/\1 4/' /usr/local/spark/conf/spark-defaults.conf"
            # CORE FOR VM LINE 40
            ssh_client.run("sed -i '40s{.*{spark.control.coreforvm " + str(
                COREVM) + "{' /usr/local/spark/conf/spark-defaults.conf")
            # SED "sed -i 's/^\(spark\.control\.coreforvm*\).*$/\1 8/' /usr/local/spark/conf/spark-defaults.conf"

            # ALPHA LINE 36
            ssh_client.run("sed -i '36s{.*{spark.control.alpha " + str(
                ALPHA) + "{' /usr/local/spark/conf/spark-defaults.conf")
            # OVERSCALE LINE 38
            ssh_client.run("sed -i '38s{.*{spark.control.overscale " + str(
                OVERSCALE) + "{' /usr/local/spark/conf/spark-defaults.conf")

            # CHANGE MASTER ADDRESS IN BENCHMARK
            ssh_client.run("""sed -i '31s{.*{SPARK_CLUSTER_URL = "spark://""" + i.public_dns_name +
                           """:7077"{' ./spark-perf/config/config.py""")
            # CHANGE SCALE FACTOR LINE 127
            ssh_client.run("sed -i '127s{.*{SCALE_FACTOR =" + str(SCALE_FACTOR) + "{' ./spark-perf/config/config.py")

            # NO PROMPT
            ssh_client.run("sed -i '103s{.*{PROMPT_FOR_DELETES = False{' ./spark-perf/config/config.py")

            # CHANGE RAM EXEC
            ssh_client.run(
                """sed -i '147s{.*{JavaOptionSet("spark.executor.memory", [""" + RAM_EXEC + """]),{' ./spark-perf/config/config.py""")

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
                ssh_client.run("""sed -i  '50s%.*%HDFS_URL = "hdfs://"""+ master_dns +""":9000/test/"%' ./spark-perf/config/config.py""")

            # START MASTER
            print("   Starting Spark Master")
            ssh_client.run('/usr/local/spark/sbin/start-master.sh -h ' + i.public_dns_name)
        else:
            # DISABLE HT
            # ssh_client.put_file("./disable-ht-v2.sh", "./disable-ht-v2.sh")
            # ssh_client.run("chmod +x disable-ht-v2.sh")
            # status, stdout, stderr = ssh_client.run('sudo ./disable-ht-v2.sh')
            # status, stdout, stderr = ssh_client.run('sudo ./disable-ht.sh')
            # print(status, stdout, stderr)

            # SAMPLING RATE LINE 43
            ssh_client.run(
                "sed -i '43s{.*{spark.control.tsample " + str(TSAMPLE) + "{' /usr/local/spark/conf/spark-defaults.conf")

            ssh_client.run("sed -i '42s{.*{spark.control.k " + str(
                K) + "{' /usr/local/spark/conf/spark-defaults.conf")

            ssh_client.run("sed -i '44s{.*{spark.control.ti " + str(
                TI) + "{' /usr/local/spark/conf/spark-defaults.conf")

            ssh_client.run("sed -i '45s{.*{spark.control.corequantum " + str(
                COREQUANTUM) + "{' /usr/local/spark/conf/spark-defaults.conf")

            # SED "sed -i 's/^\(spark\.control\.tsample*\).*$/\1 2000/' /usr/local/spark/conf/spark-defaults.conf"
            print("   Starting Spark Slave")
            ssh_client.run(
                '/usr/local/spark/sbin/start-slave.sh ' + master_dns + ':7077 -h ' + i.public_dns_name + ' --port 9999')
            # REAL CPU LOG
            logcpucommand = 'screen -d -m -S "' + i.private_ip_address + '" bash -c "sar -u 1 > sar-' + i.private_ip_address + '.log"'
            # print(logcpucommand)
            print("   Start SAR CPU Logging")
            ssh_client.run(logcpucommand)
        z += 1


    if HDFS:
        print("\nStarting Setup of HDFS cluster")
        # Format instance store SSD for hdfs usage
        for i in instances:
            ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')
            ssh_client.run(
                "sudo mkfs.ext4 -E nodiscard /dev/xvdb && sudo mount -o discard /dev/xvdb /mnt/hdfs && sudo mkdir /mnt/hdfs/namenode && sudo mkdir /mnt/hdfs/datanode")
            ssh_client.run("sudo chown ubuntu:ubuntu /mnt/hdfs/namenode && sudo chown ubuntu:ubuntu /mnt/hdfs/datanode")

        for i in instances:
            if i.public_dns_name == master_dns:
                ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')

                HADOOP_CONF = "/usr/local/lib/hadoop-2.7.2/etc/hadoop/"

                # Setup Config
                ssh_client.run(
                    "sed -i '19s%hdfs://ec2-52-40-192-53.us-west-2.compute.amazonaws.com:9000%hdfs://" + master_dns + ":9000%g' "+HADOOP_CONF + "core-site.xml")
                ssh_client.run(
                    "sed -i 's%ec2-52-40-192-53.us-west-2.compute.amazonaws.com%" + master_dns + "%g' " + HADOOP_CONF + "hdfs-site.xml")
                ssh_client.run("sed -i 's%/var/lib/hadoop/hdfs/namenode%/mnt/hdfs/namenode%g' " + HADOOP_CONF + "hdfs-site.xml")
                ssh_client.run(
                    "sed -i 's%/var/lib/hadoop/hdfs/datanode%/mnt/hdfs/datanode%g' " + HADOOP_CONF + "hdfs-site.xml")
                slaves = [i.public_dns_name for i in instances]
                slaves.remove(master_dns)
                print(slaves)
                ssh_client.run("echo -e '"+"\n".join(slaves)+"' > "+HADOOP_CONF+"slaves")

                ssh_client.run("echo 'Host *\n  UserKnownHostsFile /dev/null\n  StrictHostKeyChecking no' > ~/.ssh/config")
                # Rsync Config
                for slave in slaves:
                    status, out, err = ssh_client.run("eval `ssh-agent -s` && ssh-add gazzetta.pem && rsync -a "+ HADOOP_CONF + " ubuntu@"+slave+":"+HADOOP_CONF)
                    # print(status, out, err)

                # Start HDFS
                if DELETE_HDFS:
                    ssh_client.run("echo 'N' | /usr/local/lib/hadoop-2.7.2/bin/hdfs namenode -format")
                status, out, err =ssh_client.run("eval `ssh-agent -s` && ssh-add gazzetta.pem && /usr/local/lib/hadoop-2.7.2/sbin/start-dfs.sh && /usr/local/lib/hadoop-2.7.2/bin/hdfs dfsadmin -safemode leave")
                # print(status, out, err)
                print("   Started HDFS")
                if DELETE_HDFS:
                    print("   Cleaned HDFS")
                    status, out, err = ssh_client.run("/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
                    print(status, out, err)

    time.sleep(15)

    print("MASTER: " + master_dns)
    ssh_client = sshclient_from_instance(master_instance, KEYPAIR_PATH, user_name='ubuntu')

    # LANCIARE BENCHMARK
    if len(BENCHMARK_PERF) > 0:
        runstatus, runout, runerr = ssh_client.run('./spark-perf/bin/run')

        # FIND APP LOG FOLDER
        app_log = between(runout, b"2>> ", b".err").decode(encoding='UTF-8')
        logfolder = "./" + "/".join(app_log.split("/")[:-1])
        print(logfolder)
        os.makedirs(logfolder)

    for bench in BENCHMARK_BENCH:
        ssh_client.run('./spark-bench/' + bench + 'bin/gen_data.sh')
        ssh_client.run('./spark-bench/' + bench + 'bin/run.sh')

    # WORKER LOGS AND SAR LOG
    for i in instances:
        ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')

        if i.public_dns_name != master_dns:
            worker_log = "/usr/local/spark/logs/spark-ubuntu-org.apache.spark.deploy.worker.Worker-1-ip-" + i.private_ip_address.replace(
                ".", "-") + ".out"
            print(worker_log)
            ssh_client.run("screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")
            ssh_client.get_file(worker_log,
                                logfolder + "/spark-ubuntu-org.apache.spark.deploy.worker.Worker-1-ip-" + i.private_ip_address + ".out")
            ssh_client.get_file("sar-" + i.private_ip_address + ".log",
                                logfolder + "/" + "sar-" + i.private_ip_address + ".log")
        else:
            for file in ssh_client.listdir("/usr/local/spark/spark-events/"):
                print(file)
                ssh_client.get_file("/usr/local/spark/spark-events/" + file, logfolder + "/" + file)
            for file in ssh_client.listdir(logfolder):
                print(file)
                ssh_client.get_file(logfolder + "/" + file, logfolder + "/" + file)

    # PLOT
    plot.plot(logfolder + "/")
