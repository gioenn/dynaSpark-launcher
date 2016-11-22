import glob
import json
import math
import time
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path

import numpy as np

from config import COREVM, COREHTVM

PLOT_SID_STAGE = 0


def timing(f):
    def wrap(*args):
        tstart = time.time()
        ret = f(*args)
        tend = time.time()
        print('\n%s function took %0.3f ms' % (f.__name__, (tend - tstart) * 1000.0))
        return ret

    return wrap


def string_to_datetime(time_string):
    split = time_string.split(":")
    if "." in time_string:
        split_2 = split[2].split(".")
        return dt(2016, 1, 1, int(split[0]), int(split[1]), int(split_2[0]), int(split_2[1]))
    else:
        return dt(2016, 1, 1, int(split[0]), int(split[1]), int(split[2]), 0)


def load_config(folder):
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


def load_app_data(app_log, hdfs):
    print("Loading app data from log")
    dict_to_plot = {}
    app_info = {}
    app_id = ""
    with open(app_log) as applog:
        stage_id = -1 if hdfs else 0
        for line in applog:
            line = line.split(" ")
            if len(line) > 3 and line[3] == "TaskSetManager:" and line[4] == "Finished":
                try:
                    app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(string_to_datetime(line[1]))
                except (KeyError, ValueError) as e:
                    app_info[app_id][int(float(line[9]))]["tasktimestamps"] = []
                    app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(string_to_datetime(line[1]))
            if len(line) > 3 and line[3] == "StandaloneSchedulerBackend:" and line[4] == "Connected":
                app_info[line[-1].rstrip()] = {}
                app_id = line[-1].rstrip()
                dict_to_plot[app_id] = {}
                dict_to_plot[app_id]["dealineTimeStages"] = []
                dict_to_plot[app_id]["startTimeStages"] = []
                dict_to_plot[app_id]["finishTimeStages"] = []
            elif len(line) > 12 and line[3] == "ControllerJob:":
                if line[5] == "INIT":
                    if len(dict_to_plot[app_id]["startTimeStages"]) == len(
                            dict_to_plot[app_id]["finishTimeStages"]):
                        stage_id = int(line[12].replace(",", ""))
                        app_info[app_id][stage_id]["start"] = string_to_datetime(line[1])
                        dict_to_plot[app_id]["startTimeStages"].append(app_info[app_id][stage_id]["start"])
                        print("START: " + str(app_info[app_id][stage_id]["start"]))
                        deadline_ms = float(line[16].replace(",", ""))
                        print(deadline_ms)
                        app_info[app_id][stage_id]["deadline"] = dict_to_plot[app_id]["startTimeStages"][-1] \
                                                                 + timedelta(milliseconds=deadline_ms)
                        dict_to_plot[app_id]["dealineTimeStages"].append(app_info[app_id][stage_id]["deadline"])
                if line[5] == "NEEDED" and line[4] == "SEND":
                    next_app_id = line[-1].replace("\n", "")
                    if app_id != next_app_id:
                        app_id = next_app_id
                        dict_to_plot[app_id] = {}
                        dict_to_plot[app_id]["dealineTimeStages"] = []
                        dict_to_plot[app_id]["startTimeStages"] = []
                        dict_to_plot[app_id]["finishTimeStages"] = []
            elif len(line) > 3 and line[3] == "DAGScheduler:":
                if line[4] == "Submitting" and line[6] == "missing":
                    stage_id = int(line[10])
                    app_info[app_id][stage_id] = {}
                    app_info[app_id][stage_id]["tasks"] = int(line[5])
                    app_info[app_id][stage_id]["start"] = string_to_datetime(line[1])
                elif line[-4] == "finished":
                    if app_id != "":
                        stage_id = int(line[5])
                        app_info[app_id][stage_id]["end"] = string_to_datetime(line[1])
                        if len(dict_to_plot[app_id]["startTimeStages"]) > len(
                                dict_to_plot[app_id]["finishTimeStages"]):
                            dict_to_plot[app_id]["finishTimeStages"].append(app_info[app_id][stage_id]["end"])
                            print("END: " + str(app_info[app_id][stage_id]["end"]))
        return app_info


def compute_cputime(app_id, app_info, workers_dict, config, folder):
    cpu_time = 0
    cpu_time_max = 0
    cpus = 0.0
    for worker_log in workers_dict:
        worker_dict = workers_dict[worker_log]
        try:
            for sid in worker_dict[app_id]:
                cpus += sum(worker_dict[app_id][sid]["cpu"])
                cpu_time += (config["Control"]["Tsample"] / 1000) * sum(worker_dict[app_id][sid]["cpu"])
                time_cpu = worker_dict["time_cpu"]
                for cpu, time in zip(worker_dict[app_id][sid]["cpu"], worker_dict[app_id][sid]["time"]):
                    try:
                        index = time_cpu.index(time)
                    except ValueError:
                        index = min(range(len(time_cpu)), key=lambda i: abs(time_cpu[i] - time))
                        # print(index)
                    cpu_time_max += (config["Control"]["Tsample"] / 1000) * max(cpu, worker_dict["cpu_real"][
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
                     app_info[app_id][PLOT_SID_STAGE]["start"].timestamp())) * config["Control"]["MaxExecutor"] * COREVM
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
                        worker_dict[app_id][sid]["sp_real"].append(float(line[-1].replace("\n", "")))
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
            if not ("Linux" in line[0].split(" ") or "\n" in line[0].split(" ")) and line[1] != " CPU" and line[
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
                app_deadline = app_dict[PLOT_SID_STAGE]["start"] + timedelta(milliseconds=config["Deadline"])
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
        total_duration = app_alpha_deadline.timestamp() - app_dict[PLOT_SID_STAGE]["start"].timestamp()
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
                deadline_error = round(round(((abs(dead_ts - end_ts)) / total_duration), 4) * 100, 3)
                errors.append(deadline_error)
            except KeyError:
                None

        end = app_dict[sorted_sid[-1]]["end"].timestamp()
        print(abs(app_alpha_deadline.timestamp() - end), total_duration, end)
        app_deadline_error = round(round(((abs(app_alpha_deadline.timestamp() - end)) / total_duration), 4) * 100, 3)

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
    global STRPTIME_FORMAT
    STRPTIME_FORMAT = '%H:%M:%S.%f'
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
            compute_cputime(app_id, app_info, workers_dict, config, folder)
    else:
        print("ERROR: SAR != WORKER LOGS")
