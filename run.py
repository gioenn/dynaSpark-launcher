import boto3
import time
import copy
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
        ssh_client = sshclient_from_instance(i,
                                             "C:\\Users\\Matteo\\Downloads\\" + dataAMI[REGION]["keypair"] + ".pem",
                                             user_name='ubuntu')

        ssh_client.run('sudo killall java')
        ssh_client.run('sudo killall java')
        ssh_client.run('sudo killall java')
        if z == 0:
            master_instance = i
            master_dns = i.public_dns_name
            # START MASTER
            status, stdout, stderr = ssh_client.run('/usr/local/spark/sbin/start-master.sh -h ' + i.public_dns_name)

            # DEADLINE LINE 35
            ssh_client.run("sed -i '35s{.*{spark.control.deadline 70000{' /usr/local/spark/conf/spark-defaults.conf")
            # SED "sed -i 's/^\(spark\.control\.deadline*\).*$/\1 140000/' /usr/local/spark/conf/spark-defaults.conf"
            # MAX EXECUTOR LINE 39
            ssh_client.run("sed -i '39s{.*{spark.control.maxexecutor 4{' /usr/local/spark/conf/spark-defaults.conf")
            # SED "sed -i 's/^\(spark\.control\.maxexecutor*\).*$/\1 4/' /usr/local/spark/conf/spark-defaults.conf"
            # CORE FOR VM LINE 40
            ssh_client.run("sed -i '40s{.*{spark.control.coreforvm 8{' /usr/local/spark/conf/spark-defaults.conf")
            # SED "sed -i 's/^\(spark\.control\.coreforvm*\).*$/\1 8/' /usr/local/spark/conf/spark-defaults.conf"

            #  ALPHA LINE 36
            # OVERSCALE LINE 38

            # CHANGE MASTER ADDRESS IN BENCHMARK
            ssh_client.run("""sed -i '31s{.*{SPARK_CLUSTER_URL = "spark://""" + i.public_dns_name +
                           """:7077"{' ./spark-perf/config/config.py""")
            # CHANGE SCALE FACTOR LINE 127
            ssh_client.run("sed -i '127s{.*{SCALE_FACTOR =" + str(SCALE_FACTOR) + "{' ./spark-perf/config/config.py")

            # DISABLE AGG-BY-KEY-NAIVE BENCHMARK
            #ssh_client.run("sed -i '233 s/^/#/' ./spark-perf/config/config.py")
            #ssh_client.run("sed -i '234 s/^/#/' ./spark-perf/config/config.py")


        else:
            #ssh_client.put_file("./disable-ht-v2.sh", "./disable-ht-v2.sh")
            #ssh_client.run("chmod +x disable-ht-v2.sh")
            #status, stdout, stderr = ssh_client.run('sudo ./disable-ht-v2.sh')
            status, stdout, stderr = ssh_client.run('sudo ./disable-ht.sh')
            print(status, stdout, stderr)
            # SAMPLING RATE LINE 43
            ssh_client.run("sed -i '40s{.*{spark.control.tsample "+ str(TSAMPLE)+"{' /usr/local/spark/conf/spark-defaults.conf")
            # SED "sed -i 's/^\(spark\.control\.tsample*\).*$/\1 2000/' /usr/local/spark/conf/spark-defaults.conf"
            ssh_client.run(
                '/usr/local/spark/sbin/start-slave.sh ' + master_dns + ':7077 -h ' + i.public_dns_name + ' --port 9999')
            # REAL CPU LOG
            logcpucommand = 'screen -d -m -S "' + i.private_ip_address + '" bash -c "sar -u 1 > sar-' + i.private_ip_address + '.log"'
            print(logcpucommand)
            ssh_client.run(logcpucommand)
        z += 1
        print(z, i.public_dns_name)

    time.sleep(20)

    print(master_dns)
    ssh_client = sshclient_from_instance(master_instance,
                                         "C:\\Users\\Matteo\\Downloads\\" + dataAMI[REGION]["keypair"] + ".pem",
                                         user_name='ubuntu')
    # LANCIARE BENCHMARK
    runstatus, runout, runerr = ssh_client.run('./spark-perf/bin/run')

    # APP LOG
    app_log = between(runout, b"2>> ", b".err").decode(encoding='UTF-8')

    logfolder = "./" + "/".join(app_log.split("/")[:-1])
    print(logfolder)
    os.makedirs(logfolder)
    # WORKER LOGS AND SAR LOG
    for i in instances:
        ssh_client = sshclient_from_instance(i,
                                             "C:\\Users\\Matteo\\Downloads\\" + dataAMI[REGION]["keypair"] + ".pem",
                                             user_name='ubuntu')
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
            for file in ssh_client.listdir(logfolder):
                print(file)
                ssh_client.get_file(logfolder + "/" + file, logfolder + "/" + file)

    # PLOT
    plot.plot(logfolder+"/")
