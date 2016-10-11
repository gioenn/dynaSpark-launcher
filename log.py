import multiprocessing
import os
import time
from concurrent.futures import ThreadPoolExecutor

from boto.manage.cmdshell import sshclient_from_instance

from config import *


def timing(f):
    def wrap(*args):
        tstart = time.time()
        ret = f(*args)
        tend = time.time()
        print('\n%s function took %0.3f ms' % (f.__name__, (tend - tstart) * 1000.0))
        return ret

    return wrap


def download_master(i, output_folder, logfolder):
    ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')
    for file in ssh_client.listdir("" + SPARK_HOME + "spark-events/"):
        print("BENCHMARK: " + file)
        print("LOG FOLDER: " + logfolder)
        print("OUTPUT FOLDER: " + output_folder)
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
    return output_folder, appid


def download_slave(i, output_folder, appid):
    ssh_client = sshclient_from_instance(i, KEYPAIR_PATH, user_name='ubuntu')
    print("Downloading log from slave: " + i.public_dns_name)
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
            print("Executor ID: " + file)
            ssh_client.get_file(SPARK_HOME + "work/" + appid + "/" + file + "/stderr",
                                output_folder + "/" + i.public_dns_name + "-" + file + ".stderr")
    except FileNotFoundError:
        print("stderr not found")
    return output_folder


@timing
def download(logfolder, instances, master_dns, output_folder):
    # MASTER
    print("Downloading log from Master: "+ master_dns)
    master_instance = [i for i in instances if i.public_dns_name == master_dns][0]
    output_folder, appid = download_master(master_instance, output_folder, logfolder)

    # SLAVE
    with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
        for i in instances:
            if i.public_dns_name != master_dns:
                F = executor.submit(download_slave, i, output_folder, appid)
                output_folder = F.result()
    return output_folder
