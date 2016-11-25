"""

"""

import glob
import json
import math
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path

import numpy as np

from config import COREVM, COREHTVM
from log import load_app_data
from util.utils import timing, string_to_datetime

PLOT_SID_STAGE = 0

def load_config(folder):
    """

    :param folder:
    :return:
    """
    config_file = Path(folder + "config.json")
    print(config_file)
    if config_file.exists():
        config = json.load(open(folder + "config.json"))
        if len(config) == 0:
            from config import CONFIG_DICT
            return CONFIG_DICT
        else:
            return config
    else:
        from config import CONFIG_DICT
        return CONFIG_DICT


def compute_cpu_time(app_id, app_info, workers_dict, config, folder):
    """

    :param app_id:
    :param app_info:
    :param workers_dict:
    :param config:
    :param folder:
    :return:
    """
    cpu_time = 0
    cpu_time_max = 0
    cpus = 0.0
    for worker_log in workers_dict:
        worker_dict = workers_dict[worker_log]
        try:
            for sid in worker_dict[app_id]:
                cpus += sum(worker_dict[app_id][sid]["cpu"])
                cpu_time += (config["Control"]["Tsample"] / 1000) * sum(
                    worker_dict[app_id][sid]["cpu"])
                time_cpu = worker_dict["time_cpu"]
                for cpu, time in zip(worker_dict[app_id][sid]["cpu"],
                                     worker_dict[app_id][sid]["time"]):
                    try:
                        index = time_cpu.index(time)
                    except ValueError:
                        index = min(range(len(time_cpu)), key=lambda i: abs(time_cpu[i] - time))
                        # print(index)
                    cpu_time_max += (config["Control"]["Tsample"] / 1000) * max(cpu, worker_dict[
                        "cpu_real"][
                        index + int(config["Control"]["Tsample"] / 1000)])
        except KeyError:
            print(app_id + " not found")
    duration_s = app_info[app_id][max(list(app_info[app_id].keys()))]["end"].timestamp() - \
                 app_info[app_id][PLOT_SID_STAGE]["start"].timestamp()
    if cpus == 0.0:
        speed = config["Control"]["MaxExecutor"] * COREVM
        speed_20 = math.ceil(COREVM * duration_s / 525.8934) * config["Control"]["MaxExecutor"]
        speed_40 = math.ceil(COREVM * duration_s / 613.5423) * config["Control"]["MaxExecutor"]
        print(duration_s)
        print("SPEED NATIVE 0%", speed)
        print("SPEED NATIVE 20% ", speed_20)
        print("SPEED NATIVE 40% ", speed_40)
    else:
        speed = (float(cpus) * (config["Control"]["Tsample"] / 1000)) / duration_s

    num_task = 0.0

    for sid in app_info[app_id]:
        num_task += len(app_info[app_id][sid]["tasktimestamps"])
    throughput = float(num_task) / duration_s
    if cpu_time == 0:
        cpu_time = ((app_info[app_id][max(list(app_info[app_id].keys()))]["end"].timestamp() -
                     app_info[app_id][PLOT_SID_STAGE]["start"].timestamp())) * config["Control"][
                       "MaxExecutor"] * COREVM
        cpu_time_max = cpu_time
    cpu_time_max = math.floor(cpu_time_max)
    print("CPU_TIME: " + str(cpu_time))
    print("CPU TIME MAX: " + str(cpu_time_max))
    print("SID " + str(app_info[app_id].keys()))
    print("CHECK NON CONTROLLED STAGE FOR CPU_TIME")
    with open(folder + "CPU_TIME.txt", "w") as cpu_time_f:
        cpu_time_f.write("CPU_TIME " + str(cpu_time) + "\n")
        cpu_time_f.write("CPU_TIME_MAX " + str(cpu_time_max) + "\n")
        cpu_time_f.write("SPEED " + str(speed) + "\n")
        cpu_time_f.write("THROUGHPUT " + str(throughput) + "\n")


def save_deadline_errors(folder, deadline_error, stage_errors):
    """Save the error of the application's stages in the folder
        with some statistics (mean, stddev, median, max, min)

    :param folder: the output folder
    :param deadline_error: the application's deadline error
    :param stage_errors: the list of the stages errors
    :return: Nothing
    """
    with open(folder + "ERROR.txt", "w") as error_f:
        error_f.write("DEADLINE_ERROR " + str(abs(deadline_error)) + "\n")
        if len(stage_errors) > 0:
            error_f.write("MEAN_ERROR " + str(np.mean(stage_errors)) + "\n")
            error_f.write("DEVSTD_ERROR: " + str(np.std(stage_errors)) + "\n")
            error_f.write("MEDIAN_ERROR: " + str(np.median(stage_errors)) + "\n")
            error_f.write("MAX_ERROR: " + str(max(stage_errors)) + "\n")
            error_f.write("MIN_ERROR: " + str(min(stage_errors)) + "\n")


def load_worker_data(worker_log, cpu_log):
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

    with open(cpu_log) as cpulog:
        for line in cpulog:
            line = line.split("    ")
            if not ("Linux" in line[0].split(" ") or "\n" in line[0].split(" ")) and line[
                1] != " CPU" and line[
                0] != "Average:":
                worker_dict["time_cpu"].append(
                    dt.strptime(line[0], '%I:%M:%S %p').replace(year=2016))
                cpuint = float('{0:.2f}'.format((float(line[2]) * COREHTVM) / 100))
                worker_dict["cpu_real"].append(cpuint)

    return worker_dict


def compute_errors(app_id, app_dict, folder, config):
    if len(app_dict) > 0:
        timestamps = []
        times = []
        app_deadline = 0
        first_ts = app_dict[PLOT_SID_STAGE]["start"].timestamp()
        for sid in sorted(app_dict):
            try:
                app_deadline = app_dict[PLOT_SID_STAGE]["start"] + timedelta(
                    milliseconds=config["Deadline"])
                app_deadline = app_deadline.replace(microsecond=0)
                for timestamp in app_dict[sid]["tasktimestamps"]:
                    if first_ts == 0:
                        timestamps.append(0.0)
                        first_ts = timestamp.timestamp()
                    else:
                        timestamps.append(timestamp.timestamp() - first_ts)
                    if len(times) == 0:
                        times.append(1)
                    else:
                        times.append(times[-1] + 1)
            except KeyError:
                None

        app_alpha_deadline = app_deadline - timedelta(
            milliseconds=((1 - float(config["Control"]["Alpha"])) * float(config["Deadline"])))
        app_alpha_deadline_ts = app_alpha_deadline.timestamp() - first_ts

        # COMPUTE ERRORS
        errors = []
        sorted_sid = sorted(app_dict)
        total_duration = app_alpha_deadline.timestamp() - app_dict[PLOT_SID_STAGE][
            "start"].timestamp()
        for sid in sorted_sid:
            try:
                start_ts = app_dict[sid]["start"].timestamp() - first_ts
                end = app_dict[sid]["end"].timestamp()
                end_ts = end - first_ts
                int_dead = app_dict[sid]["deadline"].timestamp()
                dead_ts = int_dead - first_ts
                if sid == sorted_sid[-1] and start_ts < app_alpha_deadline_ts:
                    dead_ts = app_alpha_deadline_ts
                print(abs(int_dead - end), total_duration, end)
                deadline_error = round(round(((abs(dead_ts - end_ts)) / total_duration), 4) * 100,
                                       3)
                errors.append(deadline_error)
            except KeyError:
                None

        end = app_dict[sorted_sid[-1]]["end"].timestamp()
        print(abs(app_alpha_deadline.timestamp() - end), total_duration, end)
        app_deadline_error = round(
            round(((abs(app_alpha_deadline.timestamp() - end)) / total_duration), 4) * 100, 3)

        stage_errors = np.array(errors)
        print("DEADLINE_ERROR " + str(app_deadline_error))
        if len(stage_errors) > 0:
            print("MEAN ERROR: " + str(np.mean(stage_errors)))
            print("DEVSTD ERROR: " + str(np.std(stage_errors)))
            print("MEDIAN ERROR: " + str(np.median(stage_errors)))
            print("MAX ERROR: " + str(max(stage_errors)))
            print("MIN ERROR: " + str(min(stage_errors)))

        save_deadline_errors(folder, app_deadline_error, stage_errors)


@timing
def compute_metrics(folder):
    """

    :param folder:
    :return:
    """
    print(folder)
    if folder[-1] != "/":
        folder += "/"
    config = load_config(folder)
    print(config)

    global PLOT_SID_STAGE
    PLOT_SID_STAGE = 1 if config["HDFS"] else 0

    app_logs = glob.glob(folder + "*.err") + glob.glob(folder + "*.dat")
    app_info = {}
    for app_log in sorted(app_logs):
        app_info = load_app_data(app_log, config["HDFS"])

        for app_id in app_info:
            compute_errors(app_id, app_info[app_id], folder, config)

    worker_logs = glob.glob(folder + "*worker*.out")
    cpu_logs = glob.glob(folder + "sar*.log")

    if len(worker_logs) == len(cpu_logs):
        workers_dict = {}
        for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
            worker_dict = load_worker_data(worker_log, cpu_log)
            workers_dict[worker_log] = worker_dict
        for app_id in app_info:
            compute_cpu_time(app_id, app_info, workers_dict, config, folder)
    else:
        print("ERROR: SAR != WORKER LOGS")
