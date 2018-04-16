"""
Module that handles the cluster log:

* Download from master and slaves
* Extract app data
* Extract worker data
"""

import multiprocessing
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime as dt
from datetime import timedelta

#from config import PRIVATE_KEY_PATH, PROVIDER, PROCESS_ON_SERVER
from configure import config_instance as c
from util.utils import timing, string_to_datetime
from util.ssh_client import sshclient_from_node, sshclient_from_ip #vboxvm

import run
import shutil

import socket #vboxvm

#ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
#NORM_ROOT_DIR = ROOT_DIR.split(":")[-1].replace("\\", "/")

def download_master(node, output_folder, log_folder, config):
    """Download log from master instance

    :param node: master instance
    :param output_folder: output folder where save the log
    :param log_folder: log folder on the master instance
    :return: output_folder and the app_id: the application id
    """

    #ssh_client = sshclient_from_node(node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm_removed
    master_public_ip = socket.gethostbyname("XSPARKWORK0") #vboxvm
    ssh_client = sshclient_from_ip(master_public_ip, c.PRIVATE_KEY_PATH, user_name='ubuntu') #vboxvm

    app_id = ""
    #most_recent_events_logfile = ""
    #most_recent_events_logfile_folder = ""
    previous_file = ""
    files_list = ssh_client.listdir("" + config["Spark"]["SparkHome"] + "spark-events/")
    # get the most recent app id and download all the files in the folder having that name.
    most_recent_events_logfile = max(files_list)
    most_recent_events_logfile_folder = output_folder + "/" + most_recent_events_logfile
    for file in files_list:
        print("BENCHMARK: " + file)
        print("LOG FOLDER: " + log_folder)
        print("OUTPUT FOLDER: " + most_recent_events_logfile_folder)
        app_id = file
        #if log_folder != output_folder:
        #    output_folder = output_folder + app_id
        try:
            os.makedirs(most_recent_events_logfile_folder)
        except FileExistsError:
            print("Output folder already exists")
        #if most_recent_events_logfile < file:
        #    most_recent_events_logfile = file
        #    most_recent_events_logfile_folder = output_folder
            #if previous_file != "":
            #    os.remove(previous_file)
        input_file = config["Spark"]["SparkHome"] + "spark-events/" + file
        output_bz = file + ".bz"
        print("Bzipping event log...")
        #ssh_client.run("pbzip2 -9 -p" + str( #vboxvm_removed
        #    config["Control"]["CoreVM"]) + " -c " + input_file + " > " + output_bz) #vboxvm_removed
        stdout, stderr, status = ssh_client.run("pbzip2 -9 -p1 -c " + input_file + " > " + "/home/ubuntu/" + output_bz) #vboxvm
        print('ssh_client.run("pbzip2 -9 -p1 -c ' + input_file + ' > ' + '/home/ubuntu/' +  output_bz + '"): ' + stdout + stderr) #vboxvm
        #ssh_client.get(remotepath=output_bz, localpath=most_recent_events_logfile_folder + "/" + output_bz)
        if c.PROCESS_ON_SERVER:
            if file == most_recent_events_logfile:
                shutil.copyfile(input_file, "spark_log_profiling/input_logs/" + file)
            # ssh_client.get(remotepath=input_file, localpath=output_folder + "/" + file)
            shutil.copyfile("/home/ubuntu/" + output_bz, most_recent_events_logfile_folder + "/" + output_bz)
            #previous_file = output_folder + "/" + file
        else:    
            #print("Bzipping event log...")
            #ssh_client.run("pbzip2 -9 -p" + str( #vboxvm_removed
            #    config["Control"]["CoreVM"]) + " -c " + input_file + " > " + output_bz) #vboxvm_removed
            #stdout, stderr, status = ssh_client.run("pbzip2 -9 -p1 -c " + input_file + " > " + output_bz) #vboxvm
            #print('ssh_client.run("pbzip2 -9 -p1 -c "' + input_file + '" > "' + output_bz + '): ' + stdout + stderr) #vboxvm
            ssh_client.get(remotepath=output_bz, localpath=most_recent_events_logfile_folder + "/" + output_bz)
            if file == most_recent_events_logfile:
                shutil.copyfile(most_recent_events_logfile_folder + "/" + output_bz, "spark_log_profiling/input_logs/" + output_bz)
            #previous_file = output_folder + "/" + file + ".bz"
        # Removing unneeded copy of .bz logfile
        stdout, stderr, status = ssh_client.run("sudo rm /home/ubuntu/" + output_bz) #vboxvm
    ###if not c.PROCESS_ON_SERVER:
    ###    most_recent_events_logfile += ".bz"
    print("most_recent_events_logfile: " + most_recent_events_logfile_folder + "/" + most_recent_events_logfile)
    # ssh_client.get(remotepath="xSpark-bench/" + most_recent_events_logfile_folder + "/" + most_recent_events_logfile, localpath="input_logs/" + most_recent_events_logfile)
    #shutil.move(most_recent_events_logfile_folder + "/" + most_recent_events_logfile, "spark_log_profiling/input_logs/" + most_recent_events_logfile)
    ###shutil.copyfile(most_recent_events_logfile_folder + "/" + most_recent_events_logfile, "spark_log_profiling/input_logs/" + most_recent_events_logfile)
    # shutil.copy(most_recent_events_logfile_folder + "/" + most_recent_events_logfile, "input_logs/" + most_recent_events_logfile)
    # os.remove(most_recent_events_logfile_folder + "/" + most_recent_events_logfile)
    for file in ssh_client.listdir(log_folder):
        print(file)
        if file != "bench-report.dat":
            output_file = (most_recent_events_logfile_folder + "/" + file).replace(":", "-")
            ssh_client.get(remotepath=log_folder + "/" + file, localpath=output_file)
    return most_recent_events_logfile_folder, app_id
'''
def download_master(node, output_folder, log_folder, config):
    """Download log from master instance

    :param node: master instance
    :param output_folder: output folder where save the log
    :param log_folder: log folder on the master instance
    :return: output_folder and the app_id: the application id
    """
    bzip_output_folder = "spark_log_profiling/input_logs"
    ssh_client = sshclient_from_node(node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')

    app_id = ""
    files_list = ssh_client.listdir("" + config["Spark"]["SparkHome"] + "spark-events/")
    # get the latest app id and download all the files in the folder having that name.
    latest_app_id = max(files_list)
    download_folder = os.path.join(output_folder, latest_app_id)
    for file in files_list:
        print("BENCHMARK: " + file)
        print("LOG FOLDER: " + log_folder)
        print("DOWNLOAD FOLDER: " + download_folder)
        app_id = file

#        if log_folder != output_folder:
#            output_folder = output_folder + app_id
        try:
            os.makedirs(download_folder)
        except FileExistsError:
            print("Output folder already exists")
        input_file = config["Spark"]["SparkHome"] + "spark-events/" + file
        output_bz = input_file + ".bz"
        print("Bzipping event log...")
        ssh_client.run("pbzip2 -9 -p" + str(
            config["Control"]["CoreVM"]) + " -c " + input_file + " > " + output_bz)
        ssh_client.get(remotepath=output_bz, localpath=os.path.join(download_folder, file + ".bz"))
    for file in ssh_client.listdir(log_folder):
        if file != 'old':
            print(file)
            if file != "bench-report.dat":
                default_file = re.sub('.+(_run_.dat)$', 'app.dat', file)
                output_file = (download_folder + "/" + default_file).replace(":", "-")
                ssh_client.get(remotepath=log_folder + "/" + file, localpath=output_file)
    return download_folder, app_id
'''

def download_slave(node, output_folder, app_id, config):
    """Download log from slave instance:
    * The worker log that includes the controller output
    * The cpu monitoring log

    :param node: the slave instance
    :param output_folder: the output folder where to save log
    :param app_id: the application
    :return: output_folder: the output folder
    """
    ssh_client = sshclient_from_node(node, ssh_key_file=c.PRIVATE_KEY_PATH, user_name='ubuntu')

    print("Downloading log from slave: PublicIp=" + node.public_ips[0] + " PrivateIp=" + node.private_ips[0])
    try:
        worker_ip_fixed = node.private_ips[0].replace(".", "-")
        if c.PROVIDER == "AWS_SPOT":
            worker_log = "{0}logs/spark-ubuntu-org.apache.spark.deploy.worker.Worker-1-ip-{1}.out".format(
                config["Spark"]["SparkHome"], worker_ip_fixed)
        elif c.PROVIDER == "AZURE":
            worker_log = "{0}logs/spark-root-org.apache.spark.deploy.worker.Worker-1-{1}.out".format(
                config["Spark"]["SparkHome"], node.extra["name"])
        print(worker_log)
        ssh_client.run(
            "screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")
        if c.PROVIDER == "AWS_SPOT":
            output_worker_log = "{0}/spark-ubuntu-org.apache.spark.deploy.worker.Worker-1-ip-{1}.out".format(
                output_folder, node.private_ips[0])
        elif c.PROVIDER == "AZURE":
            output_worker_log = "{0}/spark-root-org.apache.spark.deploy.worker.Worker-1-{1}.out".format(
                output_folder,  node.extra["name"])
        ssh_client.get(remotepath=worker_log, localpath=output_worker_log)
        ssh_client.get(remotepath="sar-" + node.private_ips[0] + ".log",
                       localpath=output_folder + "/" + "sar-" + node.private_ips[0] + ".log")
    except FileNotFoundError:
        print("worker log not found")
    try:
        for file in ssh_client.listdir(config["Spark"]["SparkHome"] + "work/" + app_id + "/"):
            print("Executor ID: " + file)
            ssh_client.get(
                remotepath=config["Spark"]["SparkHome"] + "work/" + app_id + "/" + file + "/stderr",
                localpath=output_folder + "/" + run.get_ip(node) + "-" + file + ".stderr")
    except FileNotFoundError:
        print("stderr not found")
    return output_folder


@timing
def download(log_folder, nodes, master_ip, output_folder, config):
    """ Download the logs from the master and the worker nodes

    :param log_folder: the log folder of the application
    :param nodes: the nodes of the cluster
    :param master_ip: the ip of the master instances
    :param output_folder: the output folder where to save the logs
    :return: the output folder
    """
    # MASTER

    #master_node = [i for i in nodes if run.get_ip(i) == master_ip][0] #vboxvm_removed
    #print("Downloading log from Master: PublicIp="+master_node.public_ips[0] +" PrivateIp=" + master_node.private_ips[0]) #vboxvm_removed
    master_node = nodes[0] #vboxvm
    master_public_ip = socket.gethostbyname("XSPARKWORK0") #vboxvm
    print("Downloading log from Master: PublicIp="+ master_public_ip  +" PrivateIp=" +  master_public_ip ) #vboxvm
    output_folder, app_id = download_master(master_node, output_folder, log_folder, config)

    # SLAVE
    ''' #vboxvm_removed
    with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
        for i in nodes:
            ip = run.get_ip(i)
            if ip != master_ip:
                worker = executor.submit(download_slave, i, output_folder, app_id, config)
                output_folder = worker.result()
    '''
    return output_folder

def load_app_data(app_log_path):
    """
    Function that parse the application data like stage ids, start, deadline, end,
    tasktimestamps from the app_log

    :param app_log_path: The log of the application with log level INFO
    :return: app_info dictionary
    """
    print("Loading app data from log")
    dict_to_plot = {}
    app_info = {}
    app_id = ""
    with open(app_log_path) as app_log_fp:
        for line in app_log_fp:
            line = line.split(" ")
            if len(line) > 3:
                if line[3] == "TaskSetManager:" and line[4] == "Finished":
                    try:
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                            string_to_datetime(line[1]))
                    except (KeyError, ValueError):
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"] = []
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                            string_to_datetime(line[1]))
                elif line[3] == "StandaloneSchedulerBackend:" and line[4] == "Connected":
                    app_info[line[-1].rstrip()] = {}
                    app_id = line[-1].rstrip()
                    dict_to_plot[app_id] = {}
                    dict_to_plot[app_id]["dealineTimeStages"] = []
                    dict_to_plot[app_id]["startTimeStages"] = []
                    dict_to_plot[app_id]["finishTimeStages"] = []
                elif line[3] == "DAGScheduler:":
                    if line[4] == "Submitting" and line[6] == "missing":
                        stage_id = int(line[10])
                        app_info[app_id][stage_id] = {}
                        app_info[app_id][stage_id]["tasks"] = int(line[5])
                    elif line[-4] == "finished":
                        if app_id != "":
                            stage_id = int(line[5])
                            app_info[app_id][stage_id]["end"] = string_to_datetime(line[1])
                            if len(dict_to_plot[app_id]["startTimeStages"]) > len(
                                    dict_to_plot[app_id]["finishTimeStages"]):
                                dict_to_plot[app_id]["finishTimeStages"].append(
                                    app_info[app_id][stage_id]["end"])
                                print("END {1}: {0}".format(app_info[app_id][stage_id]["end"],
                                                            stage_id))
                elif line[3] == "ControllerJob:":
                    if line[5] == "INIT":
                        size_finish = len(dict_to_plot[app_id]["finishTimeStages"]) + 1
                        if len(dict_to_plot[app_id]["dealineTimeStages"]) < size_finish:
                            stage_id = int(line[12].replace(",", ""))
                            app_info[app_id][stage_id]["start"] = string_to_datetime(line[1])
                            print(
                                "START {1}: {0}".format(app_info[app_id][stage_id]["start"],
                                                        stage_id))
                            dict_to_plot[app_id]["startTimeStages"].append(
                                app_info[app_id][stage_id]["start"])
                            deadline_ms = float(line[16].replace(",", ""))
                            print(deadline_ms)
                            app_info[app_id][stage_id]["deadline"] = \
                                dict_to_plot[app_id]["startTimeStages"][-1] \
                                + timedelta(milliseconds=deadline_ms)
                            dict_to_plot[app_id]["dealineTimeStages"].append(
                                app_info[app_id][stage_id]["deadline"])
                    elif line[5] == "NEEDED" and line[4] == "SEND":
                        next_app_id = line[-1].replace("\n", "")
                        if app_id != next_app_id:
                            app_id = next_app_id
                            dict_to_plot[app_id] = {}
                            dict_to_plot[app_id]["dealineTimeStages"] = []
                            dict_to_plot[app_id]["startTimeStages"] = []
                            dict_to_plot[app_id]["finishTimeStages"] = []
        return app_info


def load_worker_data(worker_log, cpu_log, config):
    """
    Load the controller data from the worker_log and combine with the cpu_real data from cpu_log

    :param worker_log: the path of the log of the worker
    :param cpu_log:  the path of the cpu monitoring tool log of the worker
    :param config: the configuration dictionary
    :return: worker_dict the dictionary of the worker's  data
    """
    print(worker_log)
    print(cpu_log)
    worker_dict = {}
    with open(worker_log) as wlog:
        app_id = ""
        worker_dict["cpu_real"] = []
        worker_dict["time_cpu"] = []
        sid = -1
        for line in wlog:
            line = line.split(" ")
            if len(line) > 3:
                if line[4] == "Created" and app_id != "":
                    if sid != int(line[8]):
                        sid = int(line[8])
                        worker_dict[app_id][sid] = {}
                        worker_dict[app_id][sid]["cpu"] = []
                        worker_dict[app_id][sid]["time"] = []
                        worker_dict[app_id][sid]["sp_real"] = []
                        worker_dict[app_id][sid]["sp"] = []
                    worker_dict[app_id][sid]["cpu"].append(float(line[-1].replace("\n", "")))
                    worker_dict[app_id][sid]["sp_real"].append(0.0)
                    worker_dict[app_id][sid]["time"].append(string_to_datetime(line[1]))
                    worker_dict[app_id][sid]["sp"].append(0.0)
                if line[4] == "Scaled":
                    # print(l)
                    if app_id == "" or app_id != line[10]:
                        next_app_id = line[10]
                        try:
                            worker_dict[next_app_id] = {}
                            app_id = next_app_id
                        except KeyError:
                            print(next_app_id + " NOT FOUND BEFORE IN BENCHMARK LOGS")
                if app_id != "":
                    if line[4] == "CoreToAllocate:":
                        # print(l)
                        worker_dict[app_id][sid]["cpu"].append(float(line[-1].replace("\n", "")))
                    if line[4] == "Real:":
                        worker_dict[app_id][sid]["sp_real"].append(
                            float(line[-1].replace("\n", "")))
                    if line[4] == "SP":
                        worker_dict[app_id][sid]["time"].append(string_to_datetime(line[1]))
                        # print(l[-1].replace("\n", ""))
                        progress = float(line[-1].replace("\n", ""))
                        # print(sp)
                        if progress < 0.0:
                            worker_dict[app_id][sid]["sp"].append(abs(progress) / 100)
                        else:
                            worker_dict[app_id][sid]["sp"].append(progress)

    with open(cpu_log) as cpu_log_fp:
        for line in cpu_log_fp:
            line = line.split("    ")
            if not ("Linux" in line[0].split(" ") or "\n" in line[0].split(" ")) \
                    and line[1] != " CPU" and line[0] != "Average:":
                worker_dict["time_cpu"].append(
                    dt.strptime(line[0], '%I:%M:%S %p').replace(year=2016))
                if config["Aws"]["HyperThreading"]:
                    cpu_real = float(
                        '{0:.2f}'.format((float(line[2]) * config["Control"]["CoreVM"] * 2) / 100))
                else:
                    cpu_real = float(
                        '{0:.2f}'.format((float(line[2]) * config["Control"]["CoreVM"]) / 100))
                worker_dict["cpu_real"].append(cpu_real)
    for app_id in list(worker_dict):
        print(app_id)
        if not len(worker_dict[app_id]) > 0:
            del worker_dict[app_id]
    print(list(worker_dict.keys()))
    return worker_dict
