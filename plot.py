import glob
import json
import math
import random
import time
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path

import matplotlib.colors as colors
import matplotlib.dates as mpdate
import matplotlib.pyplot as plt
import numpy as np

from config import COREVM, COREHTVM
from config import DELETE_HDFS

STRPTIME_FORMAT = '%H:%M:%S'
SECONDLOCATOR = 10
TITLE = True


def timing(f):
    def wrap(*args):
        tstart = time.time()
        ret = f(*args)
        tend = time.time()
        print('\n%s function took %0.3f ms' % (f.__name__, (tend - tstart) * 1000.0))
        return ret

    return wrap


def load_config(folder):
    config_file = Path(folder + "config.json")
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
                    app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                        dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
                except (KeyError, ValueError) as e:
                    app_info[app_id][int(float(line[9]))]["tasktimestamps"] = []
                    try:
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                            dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
                    except ValueError:
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                            dt.strptime(line[1], "%H:%M:%S").replace(year=2016))
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
                        app_info[app_id][stage_id]["start"] = dt.strptime(line[1], STRPTIME_FORMAT).replace(
                            year=2016)
                        dict_to_plot[app_id]["startTimeStages"].append(app_info[app_id][stage_id]["start"])
                        print("START: " + str(dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016)))
                        print(line[16].replace(",", ""))
                        app_info[app_id][stage_id]["deadline"] = dict_to_plot[app_id]["startTimeStages"][
                                                                     -1] + timedelta(
                            milliseconds=float(line[16].replace(",", "")))
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
                    try:
                        app_info[app_id][stage_id]["start"] = dt.strptime(line[1], STRPTIME_FORMAT).replace(
                            year=2016)
                    except ValueError:
                        app_info[app_id][stage_id]["start"] = dt.strptime(line[1], "%H:%M:%S").replace(
                            year=2016)
                elif line[-4] == "finished":
                    if app_id != "":
                        stage_id = int(line[5])
                        try:
                            app_info[app_id][stage_id]["end"] = dt.strptime(line[1], STRPTIME_FORMAT).replace(
                                year=2016)
                        except ValueError:
                            app_info[app_id][stage_id]["end"] = dt.strptime(line[1], "%H:%M:%S").replace(
                                year=2016)
                        if len(dict_to_plot[app_id]["startTimeStages"]) > len(
                                dict_to_plot[app_id]["finishTimeStages"]):
                            dict_to_plot[app_id]["finishTimeStages"].append(app_info[app_id][stage_id]["end"])
                            print("END: " + str(app_info[app_id][stage_id]["end"]))
        return app_info


def compute_cputime(app_id, app_info, workers_dict, config, folder):
    cpu_time = 0
    cpu_time_max = 0
    for worker_dict in workers_dict:
        for sid in worker_dict[app_id]:
            cpu_time += (config["Control"]["Tsample"] / 1000) * sum(worker_dict[app_id][sid]["cpu"])

            for cpu, time in zip(worker_dict[app_id][sid]["cpu"], worker_dict[app_id][sid]["time"]):
                index = worker_dict["time_cpu"].index(time)

                cpu_time_max += (config["Control"]["Tsample"] / 1000) * max(cpu, worker_dict["cpu_real"][
                    index + int(config["Control"]["Tsample"] / 1000)])

    if cpu_time == 0:
        cpu_time = ((app_info[app_id][max(list(app_info[app_id].keys()))]["end"].timestamp() -
                     app_info[app_id][0]["start"].timestamp())) * config["Control"]["MaxExecutor"] * COREVM
        cpu_time_max = cpu_time
    cpu_time_max = math.floor(cpu_time_max)
    print("CPU_TIME: " + str(cpu_time))
    print("CPU TIME MAX: " + str(cpu_time_max))
    print("SID " + str(app_info[app_id].keys()))
    print("CHECK NON CONTROLLED STAGE FOR CPU_TIME")
    with open(folder + "CPU_TIME.txt", "w") as cpu_time_f:
        cpu_time_f.write("CPU_TIME " + str(cpu_time) + "\n")
        cpu_time_f.write("CPU_TIME_MAX " + str(cpu_time_max) + "\n")


def plot_worker(app_id, app_info, worker_log, worker_dict, config):
    colors_stage = list(colors.cnames.values())
    fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)
    for sid in sorted(app_info[app_id]):
        try:
            ax1.plot(worker_dict[app_id][sid]["time"],
                     worker_dict[app_id][sid]["sp"], ".r-", label='PROGRESS')

            ax1.plot(worker_dict[app_id][sid]["time"],
                     worker_dict[app_id][sid]["sp_real"], ".k-",
                     label='PROGRESS REAL')

            color = colors_stage[random.randint(0, len(colors_stage) - 1)]
            ax1.axvline(app_info[app_id][sid]["start"], color=color, linestyle='--')
            ax1.axvline(app_info[app_id][sid]["end"], color=color)
            ax1.axvline(app_info[app_id][sid]["deadline"], color="red", linestyle='--')
        except KeyError:
            print("SID " + str(sid) + " not executed by " + worker_log)

    ax1.set_xlabel('time')
    ax1.set_ylabel('stage progress')
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.get_xaxis().tick_bottom()
    ax1.get_yaxis().tick_left()

    xlim = ax1.get_xlim()
    factor = 0.1
    new_xlim = (xlim[0] + xlim[1]) / 2 + np.array((-0.5, 0.5)) * (xlim[1] - xlim[0]) * (1 + factor)
    ax1.set_xlim(new_xlim)

    ylim = ax1.get_ylim()
    new_ylim = (ylim[0] + ylim[1]) / 2 + np.array((-0.5, 0.5)) * (ylim[1] - ylim[0]) * (1 + factor)
    ax1.set_ylim(new_ylim)

    ax2 = ax1.twinx()

    for sid in sorted(app_info[app_id]):
        try:
            ax2.plot(worker_dict[app_id][sid]["time"], worker_dict[app_id][sid]["cpu"],
                     ".b-",
                     label='CPU')

            start_index = worker_dict["time_cpu"].index(worker_dict[app_id][sid]["time"][0])
            end_index = worker_dict["time_cpu"].index(worker_dict[app_id][sid]["time"][-1])

            ax2.plot(worker_dict["time_cpu"][start_index:end_index],
                     worker_dict["cpu_real"][start_index:end_index], ".g-",
                     label='CPU REAL')
        except KeyError:
            print("SID " + str(sid) + " not executed by " + worker_log)

    handles_ax1, labels_ax1 = ax1.get_legend_handles_labels()
    handles_ax2, labels_ax2 = ax2.get_legend_handles_labels()
    handles = handles_ax1[:2] + handles_ax2[:2]
    labels = labels_ax1[:2] + labels_ax2[:2]
    plt.legend(handles, labels, loc='best', prop={'size': 12})
    ax2.set_ylabel('cpu')
    ax2.set_ylim(0, COREVM)
    labels = ax1.get_xticklabels()
    plt.setp(labels, rotation=90, fontsize=10)
    xlim = ax2.get_xlim()
    # example of how to zoomout by a factor of 0.1
    factor = 0.1
    new_xlim = (xlim[0] + xlim[1]) / 2 + np.array((-0.5, 0.5)) * (xlim[1] - xlim[0]) * (1 + factor)
    ax2.set_xlim(new_xlim)
    ylim = ax2.get_ylim()
    new_ylim = (ylim[0] + ylim[1]) / 2 + np.array((-0.5, 0.5)) * (ylim[1] - ylim[0]) * (1 + factor)
    ax2.set_ylim(new_ylim)
    locator = mpdate.SecondLocator(interval=SECONDLOCATOR)
    plt.gca().xaxis.set_major_locator(locator)
    plt.gca().xaxis.set_major_formatter(mpdate.DateFormatter(STRPTIME_FORMAT))
    plt.gcf().autofmt_xdate()

    if TITLE:
        plt.title(
            app_id + " " + str(config["Deadline"]) + " " + str(
                config["Control"]["Tsample"]) + " " + str(
                config["Control"]["Alpha"]) + " " + str(config["Control"]["K"]))
    plt.savefig(worker_log + "." + app_id + '.png', bbox_inches='tight', dpi=300)
    plt.close()


def save_deadline_errors(folder, deadline_error, stage_errors):
    with open(folder + "ERROR.txt", "w") as error_f:
        error_f.write("DEADLINE_ERROR " + str(abs(deadline_error)) + "\n")
        if len(stage_errors) > 0:
            error_f.write("MEAN_ERROR " + str(np.mean(stage_errors)) + "\n")
            error_f.write("DEVSTD_ERROR: " + str(np.std(stage_errors)) + "\n")
            error_f.write("MEDIAN_ERROR: " + str(np.median(stage_errors)) + "\n")
            error_f.write("MAX_ERROR: " + str(max(stage_errors)) + "\n")
            error_f.write("MIN_ERROR: " + str(min(stage_errors)) + "\n")


def plot_app_overview(app_id, app_dict, folder, config):
    print("Plotting APP Overview")
    if len(app_dict) > 0:
        timestamps = []
        times = []
        app_deadline = 0
        for sid in sorted(app_dict):
            try:
                app_deadline = app_dict[DELETE_HDFS]["start"] + timedelta(milliseconds=config["Deadline"])
                for timestamp in app_dict[sid]["tasktimestamps"]:
                    timestamps.append(timestamp)
                    if len(times) == 0:
                        times.append(1)
                    else:
                        times.append(times[-1] + 1)
            except KeyError:
                None

        fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)
        # PLOT NORMALIZED TASK PROGRESS
        min_times = min(times)
        max_times = max(times)
        normalized = [(z - min_times) / (max_times - min_times) for z in times]
        ax1.plot(timestamps, normalized, ".k-")
        ymin, ymax = ax1.get_ylim()
        # PLOT DEADLINE
        ax1.axvline(app_deadline)
        ax1.text(app_deadline, ymax, 'DEADLINE APP', rotation=90)
        # PLOT ALPHA DEADLINE
        app_alpha_deadline = app_deadline - timedelta(
            milliseconds=((1 - float(config["Control"]["Alpha"])) * float(config["Deadline"])))
        ax1.axvline(app_alpha_deadline)
        ax1.text(app_alpha_deadline, ymax, 'ALPHA DEADLINE', rotation=90)
        ax1.set_xlabel('time')
        ax1.set_ylabel('app progress')

        # COMPUTE ERRORS AND PLOT VERTICAL LINES DEAD, START, END
        errors = []
        for sid in sorted(app_dict):
            int_dead = 0
            try:
                ax1.axvline(app_dict[sid]["deadline"], color="r", linestyle='--')
                ax1.text(app_dict[sid]["deadline"], ymin + 0.15 * ymax, 'DEAD SID ' + str(sid), rotation=90)
                int_dead = app_dict[sid]["deadline"].timestamp()
                ax1.axvline(app_dict[sid]["start"], color="b")
                ax1.text(app_dict[sid]["start"], ymax - 0.02 * ymax, 'START SID ' + str(sid), rotation=90, )
                ax1.axvline(app_dict[sid]["end"], color="r")
                ax1.text(app_dict[sid]["end"], ymax - 0.25 * ymax, 'END SID ' + str(sid), rotation=90)
                end = app_dict[sid]["end"].timestamp()
                duration = app_alpha_deadline.timestamp() - app_dict[DELETE_HDFS]["start"].timestamp()
                deadline_error = round(round(((abs(int_dead - end)) / duration), 3) * 100, 3)
                errors.append(deadline_error)
                ax1.text(app_dict[sid]["end"], ymax - random.uniform(0.4, 0.5) * ymax,
                         "E " + str(deadline_error) + "%", rotation=90)
            except KeyError:
                None

        try:
            end = app_dict[sorted(app_dict.keys())[-1]]["end"].timestamp()
        except KeyError:
            None
        int_dead = app_alpha_deadline.timestamp()
        duration = int_dead - app_dict[DELETE_HDFS]["start"].timestamp()
        app_deadline_error = round(round(((int_dead - end) / duration), 3) * 100, 3)
        ax1.text(app_alpha_deadline, ymax - 0.5 * ymax, "ERROR = " + str(app_deadline_error) + "%")

        stage_errors = np.array(errors)
        print("DEADLINE_ERROR " + str(abs(app_deadline_error)))
        if len(stage_errors) > 0:
            print("MEAN ERROR: " + str(np.mean(stage_errors)))
            print("DEVSTD ERROR: " + str(np.std(stage_errors)))
            print("MEDIAN ERROR: " + str(np.median(stage_errors)))
            print("MAX ERROR: " + str(max(stage_errors)))
            print("MIN ERROR: " + str(min(stage_errors)))

            save_deadline_errors(folder, app_deadline_error, stage_errors)

        labels = ax1.get_xticklabels()
        plt.setp(labels, rotation=90)
        locator = mpdate.SecondLocator(interval=SECONDLOCATOR)
        plt.gca().xaxis.set_major_locator(locator)
        plt.gca().xaxis.set_major_formatter(mpdate.DateFormatter(STRPTIME_FORMAT))
        plt.gcf().autofmt_xdate()
        if TITLE:
            plt.title(app_id + " " + str(config["Deadline"]) + " " + str(config["Control"]["Tsample"]) + " " + str(
                config["Control"]["Alpha"]) + " " + str(config["Control"]["K"]))
        plt.savefig(folder + app_id + ".png", bbox_inches='tight', dpi=300)
        plt.close()


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
                    worker_dict[app_id][sid]["time"].append(
                        dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
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
                        worker_dict[app_id][sid]["time"].append(
                            dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
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


@timing
def plot(folder):
    if folder[-1] != "/":
        folder += "/"
    config = load_config(folder)
    print(config)

    app_logs = glob.glob(folder + "*.err") + glob.glob(folder + "*.dat")
    app_info = {}
    for app_log in sorted(app_logs):
        app_info = load_app_data(app_log, config["HDFS"])

        for app_id in app_info:
            plot_app_overview(app_id, app_info[app_id], folder, config)

    worker_logs = glob.glob(folder + "*worker*.out")
    cpu_logs = glob.glob(folder + "sar*.log")

    if len(worker_logs) == len(cpu_logs):
        workers_dict = []
        for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
            worker_dict = load_worker_data(worker_log, cpu_log)
            workers_dict.append(worker_dict)
            for app_id in app_info:
                plot_worker(app_id, app_info, worker_log, worker_dict, config)
        for app_id in app_info:
            compute_cputime(app_id, app_info, workers_dict, config, folder)
    else:
        print("ERROR: SAR != WORKER LOGS")
