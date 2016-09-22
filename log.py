from boto.manage.cmdshell import sshclient_from_instance
import os
from config import *


def download(logfolder, instances, master_dns, output_folder):
    appid = ""
    # WORKER LOGS AND SAR LOG
    for i in instances:
        ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')
        print(i.public_dns_name)
        if i.public_dns_name != master_dns:
            try:
                worker_log = "" + SPARK_HOME + "logs/spark-ubuntu-org.apache.spark.deploy.worker.Worker-1-ip-" + i.private_ip_address.replace(
                    ".", "-") + ".out"
                print(worker_log)
                ssh_client.run("screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")
                ssh_client.get_file(worker_log,
                                    output_folder + "/spark-ubuntu-org.apache.spark.deploy.worker.Worker-1-ip-" + i.private_ip_address + ".out")
                ssh_client.get_file("sar-" + i.private_ip_address + ".log",
                                    output_folder + "/" + "sar-" + i.private_ip_address + ".log")
            except FileNotFoundError:
                print("worker log not found")
            try:
                for file in ssh_client.listdir(SPARK_HOME + "work/" + appid + "/"):
                    print(file)
                    ssh_client.get_file(SPARK_HOME + "work/" + appid + "/" + file + "/stderr",
                                        output_folder + "/" + i.public_dns_name + "-" + file + ".stderr")
            except FileNotFoundError:
                print("stderr not found")
        else:
            for file in ssh_client.listdir("" + SPARK_HOME + "spark-events/"):
                print(file)
                print(logfolder)
                print(output_folder)
                appid = file
                if logfolder != output_folder:
                    output_folder = output_folder + appid
                try:
                    os.makedirs(output_folder)
                except FileExistsError:
                    print("Output folder already exists")
                if len(BENCHMARK_PERF) > 0:
                    ssh_client.get_file("" + SPARK_HOME + "spark-events/" + file, output_folder + "/" + file)
            for file in ssh_client.listdir(logfolder):
                print(file)
                if file != "bench-report.dat":
                    output_file = (output_folder + "/" + file).replace(":", "-")
                    ssh_client.get_file(logfolder + "/" + file, output_file)

    return output_folder
