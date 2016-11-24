"""
Plot module to generate figure from the log of the application and worker's logs.

This module can generate two type of figure: Overview, Worker. The Overview figure is the overview
 of the all application with the aggregated cpu metrics from the workers. The Worker figure is the
 detail of the execution of one worker in which each stage as its real progress and estimated progress
 including the allocation of cpu.

"""

import glob
import json
import math
from datetime import datetime as dt, timedelta
from pathlib import Path

import matplotlib
import numpy as np

from util.utils import timing, string_to_datetime

matplotlib.use('PDF')
import matplotlib.ticker as plticker
import matplotlib.pyplot as plt

from config import COREVM, COREHTVM

Y_TICK_AGG = 40
Y_TICK_SORT = 40
TITLE = False
PLOT_SID_STAGE = 0
PLOT_LABEL = True
LABEL_SIZE = 20
TQ_MICRO = 10
TQ_KMEANS = 9
PDF = 1

params = {
    'axes.labelsize': LABEL_SIZE,  # fontsize for x and y labels (was 10)
    'axes.titlesize': 8,
    'font.size': LABEL_SIZE,  # was 10
    'legend.fontsize': 8,  # was 10
    'xtick.labelsize': LABEL_SIZE,
    'ytick.labelsize': LABEL_SIZE,
}

matplotlib.rcParams.update(params)


def load_config(folder):
    """
    Load the config from the folder if present otherwise load the config from config.py

    :param folder: the folder path where find the config file
    :return: the config dictionary
    """
    config_file = Path(folder + "config.json")
    if config_file.exists():
        config = json.load(open(folder + "config.json"))
        if len(config) == 0:
            from csparkbench.config import CONFIG_DICT
            return CONFIG_DICT
        else:
            return config
    else:
        from csparkbench.config import CONFIG_DICT
        return CONFIG_DICT


def load_app_data(app_log, hdfs):
    """
    Function that parse the application data like stage ids, start, deadline, end,
    tasktimestamps from the app_log

    :param app_log: The log of the application with log level INFO
    :param hdfs:
    :return: app_info dictionary
    """
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
                        string_to_datetime(line[1]))
                except (KeyError, ValueError) as e:
                    app_info[app_id][int(float(line[9]))]["tasktimestamps"] = []
                    app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                        string_to_datetime(line[1]))
            if len(line) > 3 and line[3] == "StandaloneSchedulerBackend:" and line[
                4] == "Connected":
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
                        dict_to_plot[app_id]["startTimeStages"].append(
                            app_info[app_id][stage_id]["start"])
                        print("START: " + str(app_info[app_id][stage_id]["start"]))
                        deadline_ms = float(line[16].replace(",", ""))
                        print(deadline_ms)
                        app_info[app_id][stage_id]["deadline"] = \
                            dict_to_plot[app_id]["startTimeStages"][-1] \
                            + timedelta(milliseconds=deadline_ms)
                        dict_to_plot[app_id]["dealineTimeStages"].append(
                            app_info[app_id][stage_id]["deadline"])
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
                            dict_to_plot[app_id]["finishTimeStages"].append(
                                app_info[app_id][stage_id]["end"])
                            print("END: " + str(app_info[app_id][stage_id]["end"]))
        return app_info


def get_text_positions(x_data, y_data, txt_width, txt_height):
    a = list(zip(y_data, x_data))
    text_positions = y_data.copy()
    for index, (y, x) in enumerate(a):
        local_text_positions = [i for i in a if i[0] > (y - txt_height)
                                and (abs(i[1] - x) < txt_width * 2) and i != (y, x)]
        if local_text_positions:
            sorted_ltp = sorted(local_text_positions)
            if abs(sorted_ltp[0][0] - y) < txt_height:  # True == collision
                differ = np.diff(sorted_ltp, axis=0)
                a[index] = (sorted_ltp[-1][0] + txt_height, a[index][1])
                text_positions[index] = sorted_ltp[-1][0] + txt_height
                for k, (j, m) in enumerate(differ):
                    # j is the vertical distance between words
                    if j > txt_height * 2:  # if True then room to fit a word in
                        a[index] = (sorted_ltp[k][0] + txt_height, a[index][1])
                        text_positions[index] = sorted_ltp[k][0] + txt_height
                        break
    return text_positions


def text_plotter(x_data, y_data, text_positions, axis, txt_width, txt_height, labels):
    for i, (x, y, t, l) in enumerate(zip(x_data, y_data, text_positions, labels)):
        if len(x_data) > 3:
            yt = 102 if (i % 2) == 1 else 110
        else:
            yt = 102
        axis.text(x, yt, l, rotation=0, horizontalalignment='center', size=LABEL_SIZE)


def plot_worker(app_id, app_info, worker_log, worker_dict, config, first_ts_worker):
    """
    Plot the progress of each stage in the worker with the controller's data

    :param app_id: the id of the application
    :param app_info: the dictionary with the application's data
    :param worker_log: the path of the worker's log file
    :param worker_dict: the dictionary with the worker's data
    :param config: the dictionary of the config file
    :param first_ts_worker: the first start ts of PLOT_SID_STAGE
    :return: nothing; save the figure in the output folder
    """
    fig, ax1 = plt.subplots(figsize=(16, 5), dpi=300)
    times = []
    cpus = []
    label_to_plot = []
    ts_to_plot = []
    sorted_sid = sorted(app_info[app_id])
    if config["HDFS"]:
        sorted_sid.remove(0)
    for sid in sorted_sid:
        try:
            start_ts = app_info[app_id][sid]["start"].timestamp() - first_ts_worker
            ax1.axvline(start_ts, ymin=0.0, ymax=1.1, color="green", zorder=100)
            # try:
            #     label_to_plot[tq_ts].append("S" + str(sid))
            # except KeyError:
            #     label_to_plot[tq_ts] = []
            #     label_to_plot[tq_ts].append("S" + str(sid))
            # ax1.text(start_ts, 105, "S" + str(sid), style="italic", weight="bold",
            # horizontalalignment='center')
            end_ts = app_info[app_id][sid]["end"].timestamp() - first_ts_worker
            ax1.axvline(end_ts, color="red")
            label_to_plot.append("E" + str(sid))
            ts_to_plot.append(end_ts)
            # ax1.text(end_ts, -0.035, "E"+str(sid), style="italic", weight="bold",
            # horizontalalignment='center')
            dead_ts = app_info[app_id][sid]["deadline"].timestamp() - first_ts_worker
            ax1.axvline(dead_ts, color="green", linestyle='--')
            # try:
            #     label_to_plot[tq_ts].append("D" + str(sid))
            # except KeyError:
            #     label_to_plot[tq_ts] = []
            #     label_to_plot[tq_ts].append("D" + str(sid))
            # ax1.text(dead_ts, 101, "D" + str(sid), style="italic", weight="bold",
            # horizontalalignment='center')
            time_sp = [t.timestamp() - first_ts_worker for t in worker_dict[app_id][sid]["time"]]
            time_sp_real = [t.timestamp() - first_ts_worker for t in
                            worker_dict[app_id][sid]["time"]]
            sp_real = worker_dict[app_id][sid]["sp_real"]
            sp = worker_dict[app_id][sid]["sp"]
            if sp[-1] < 1.0:
                time_sp.append(dead_ts)
                sp.append(1.0)
            if sp_real[-1] < 1.0:
                next_time = time_sp_real[-1] + (int(config["Control"]["Tsample"]) / 1000)
                if next_time <= end_ts:
                    time_sp_real.append(next_time)
                    sp_real.append(1.0)
                else:
                    time_sp_real.append(end_ts)
                    sp_real.append(1.0)
            sp = [x * 100 for x in sp]
            sp_real = [y * 100 for y in sp_real]
            ax1.plot(time_sp, sp, "gray", label='PROGRESS', linewidth=2)
            ax1.plot(time_sp_real, sp_real, "black", label='PROGRESS REAL', linewidth=2)
        except KeyError as e:
            print("SID " + str(sid) + " not executed by " + worker_log)

    if PLOT_LABEL:
        # set the bbox for the text. Increase txt_width for wider text.
        txt_height = 0.2 * (plt.ylim()[1] - plt.ylim()[0])
        txt_width = 0.2 * (plt.xlim()[1] - plt.xlim()[0])
        # Get the corrected text positions, then write the text.
        y_label = np.full(len(label_to_plot), plt.ylim()[1])
        text_positions = get_text_positions(ts_to_plot,
                                            y_label, txt_width, txt_height)
        text_plotter(ts_to_plot, y_label,
                     text_positions, ax1, txt_width, txt_height, label_to_plot)

    ax1.set_xlabel('Time [s]')
    ax1.set_ylabel('Stage Progress [%]')
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.get_xaxis().tick_bottom()
    ax1.get_yaxis().tick_left()

    xlim = ax1.get_xlim()
    ax1.set_xlim(0.0, xlim[1])
    ax1.set_ylim(0.0, 100.0)

    folder_split = worker_log.split("/")
    name = folder_split[-3].lower() + "-worker-" + folder_split[-2].replace("%", "") + "-" + \
           folder_split[-1].split("-")[-1].replace(".out", "")
    if "agg" in name:
        ax1.xaxis.set_major_locator(plticker.MultipleLocator(base=Y_TICK_AGG))
    elif "sort" in name:
        ax1.xaxis.set_major_locator(plticker.MultipleLocator(base=Y_TICK_SORT))
    ax1.yaxis.set_major_locator(plticker.MultipleLocator(base=10.0))

    ax2 = ax1.twinx()

    for sid in sorted_sid:
        try:
            time = [t.timestamp() - first_ts_worker for t in worker_dict[app_id][sid]["time"]]
            times += time
            cpus += worker_dict[app_id][sid]["cpu"]

            end_ts = app_info[app_id][sid]["end"].timestamp() - first_ts_worker
            dead_ts = app_info[app_id][sid]["deadline"].timestamp() - first_ts_worker
            if sid == sorted(app_info[app_id])[-1] and end_ts < dead_ts:
                times.append(dead_ts)
                cpus.append(cpus[-1] * 2 / 3)
            else:
                next_time = time[-1] + (int(config["Control"]["Tsample"]) / 1000)
                if next_time <= end_ts:
                    times.append(next_time)
                    cpus.append(0.0)
                times.append(end_ts)
                cpus.append(0.0)
                index_next = min(sorted(app_info[app_id]).index(sid) + 1, len(app_info[app_id]) - 1)
                times.append(
                    app_info[app_id][sorted(app_info[app_id])[index_next]][
                        "start"].timestamp() - first_ts_worker)
                cpus.append(0.0)
                # start_index = worker_dict["time_cpu"].index(worker_dict[app_id][sid]["time"][0])
                # end_index = worker_dict["time_cpu"].index(worker_dict[app_id][sid]["time"][-1])
                #
                # time_cpu = [t.timestamp() - first_ts_worker for t in worker_dict["time_cpu"]
                # [start_index:end_index]]
                # ax2.plot(time_cpu,
                #          worker_dict["cpu_real"][start_index:end_index], ".g-",
                #          label='CPU REAL')
        except KeyError as e:
            print("SID " + str(sid) + " not executed by " + worker_log)

    ax2.plot(times, cpus, ".b-", label='CPU')
    ax2.fill_between(times, 0.0, cpus, facecolor="b", alpha=0.2)
    # handles_ax1, labels_ax1 = ax1.get_legend_handles_labels()
    # handles_ax2, labels_ax2 = ax2.get_legend_handles_labels()
    # handles = handles_ax1[:2] + handles_ax2[:2]
    # labels = labels_ax1[:2] + labels_ax2[:2]
    # plt.legend(handles, labels, loc='best', prop={'size': 6})
    ax2.set_ylabel('Core [#]')
    ax2.set_ylim(0, COREVM)
    xlim = ax2.get_xlim()
    ax2.set_xlim(0.0, xlim[1])
    # labels = ax1.get_xticklabels()
    # plt.setp(labels, rotation=90, fontsize=10)

    ax2.yaxis.set_major_locator(plticker.MultipleLocator(base=1.0))
    # locator = mpdate.SecondLocator(interval=SECONDLOCATOR)
    # plt.gca().xaxis.set_major_locator(locator)
    # plt.gca().xaxis.set_major_formatter(mpdate.DateFormatter(STRPTIME_FORMAT))
    # plt.gcf().autofmt_xdate()
    if TITLE:
        plt.title(
            app_id + " " + str(config["Deadline"]) + " " + str(
                config["Control"]["Tsample"]) + " " + str(
                config["Control"]["Alpha"]) + " " + str(config["Control"]["K"]))
    ax1.set_zorder(ax2.get_zorder() + 1)
    ax1.patch.set_visible(False)
    folder_split = worker_log.split("/")
    name = folder_split[-3].lower() + "-worker-" + folder_split[-2].replace("%", "") + "-" + \
           folder_split[-1].split("-")[-1].replace(".out", "")
    folder = "/".join(worker_log.split("\\")[:-1])
    labels = ax1.get_xticklabels()
    plt.setp(labels, rotation=45)
    if PDF:
        plt.savefig(folder + "/" + name + ".pdf", bbox_inches='tight', dpi=300)
    else:
        plt.savefig(folder + "/" + name + '.png', bbox_inches='tight', dpi=300)
    plt.close()


def plot_app_overview(app_id, app_dict, folder, config):
    """
    Plot only the application overview without the cpu data from the workers

    :param app_id: the id of the application
    :param app_dict: the data dictionary of the application
    :param folder: the folder where to save the output image
    :param config: the config dict of the application
    :return: nothing; save the figure in the output folder
    """
    print("Plotting APP Overview")
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

        fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)
        # PLOT NORMALIZED TASK PROGRESS
        min_times = min(times)
        max_times = max(times)
        normalized = [(z - min_times) / (max_times - min_times) * 100 for z in times]
        ax1.plot(timestamps, normalized, ".k-")
        ymin, ymax = ax1.get_ylim()
        # PLOT DEADLINE
        app_dead_ts = app_deadline.timestamp() - first_ts
        ax1.axvline(app_dead_ts)
        ax1.text(app_dead_ts, ymax + 0.5, 'D', weight="bold", horizontalalignment='center')
        # PLOT ALPHA DEADLINE
        app_alpha_deadline = app_deadline - timedelta(
            milliseconds=((1 - float(config["Control"]["Alpha"])) * float(config["Deadline"])))
        app_alpha_deadline_ts = app_alpha_deadline.timestamp() - first_ts
        ax1.axvline(app_alpha_deadline_ts)
        ax1.text(app_alpha_deadline_ts, ymax + 0.5, 'AD', weight="bold",
                 horizontalalignment='center')
        ax1.set_xlabel('Time [s]')
        ax1.set_ylabel('App Progress [%]')

        # PLOT VERTICAL LINES DEAD, START, END
        sorted_sid = sorted(app_dict)
        for sid in sorted_sid:
            try:
                start_ts = app_dict[sid]["start"].timestamp() - first_ts
                # ax1.axvline(start_ts, color="black")
                # try:
                #     s_y = normalized[timestamps.index(start_ts)]
                # except ValueError:
                #     s_y = start_ts
                # ax1.text(start_ts + 3, s_y - 2, 'S' + str(sid), style="italic",weight="bold",
                # horizontalalignment='center')
                end = app_dict[sid]["end"].timestamp()
                end_ts = end - first_ts
                #
                ax1.axvline(end_ts, color="r")
                # try:
                #     index_t = timestamps.index(end_ts)
                # except ValueError:
                #     index_t = min(range(len(timestamps)), key=lambda i:
                # abs(timestamps[i] - end_ts))
                # e_y = normalized[index_t]
                # ax1.text(end_ts, ymin - 3.5, 'E' + str(sid), style="italic", weight="bold",
                # horizontalalignment='center', verticalalignment='bottom')
                int_dead = app_dict[sid]["deadline"].timestamp()
                dead_ts = int_dead - first_ts
                if sid == sorted_sid[-1] and start_ts < app_alpha_deadline_ts:
                    dead_ts = app_alpha_deadline_ts
                # print(dead_ts)
                ax1.axvline(dead_ts, color="r", linestyle='--')
            except KeyError:
                None

        if TITLE:
            plt.title(app_id + " " + str(config["Deadline"]) + " " + str(
                config["Control"]["Tsample"]) + " " + str(
                config["Control"]["Alpha"]) + " " + str(config["Control"]["K"]))
        folder_split = folder.split("/")
        name = folder_split[-4].lower() + "-overview-" + folder_split[-3].replace("%", "")
        labels = ax1.get_xticklabels()
        plt.setp(labels, rotation=45)
        if PDF:
            plt.savefig(folder + name + ".png", bbox_inches='tight', dpi=300)
        else:
            plt.savefig(folder + name + ".pdf", bbox_inches='tight', dpi=300)
        plt.close()


def load_worker_data(worker_log, cpu_log):
    """
    Load the controller data from the worker_log and combine with the cpu_real data from cpu_log

    :param worker_log: the path of the log of the worker
    :param cpu_log:  the path of the cpu monitoring tool log of the worker
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
    print(list(worker_dict.keys()))
    return worker_dict


def plot_overview_cpu(app_id, app_info, workers_dict, config, folder):
    """
    Function that plot the overview of the application aggregating the cpu data from all
    the workers showing the application progress and the division by stage

    :param app_id: the id of the application
    :param app_info: the data dictionary of the application
    :param workers_dict: the data dictionary of the workers
    :param config: the dictionary of the config file
    :param folder: the folder to output the image
    :return: nothing only save the figure at the end in the folder
    """
    print("Plotting APP Overview")
    if len(app_info[app_id]) > 0:
        folder_split = folder.split("/")
        name = folder_split[-4].lower() + "-overview-" + folder_split[-3].replace("%", "")
        timestamps = [0]
        times = [0]
        app_deadline = dt.now()
        first_ts = app_info[app_id][PLOT_SID_STAGE]["start"].timestamp()
        sorted_sid = sorted(app_info[app_id])
        if config["HDFS"]:
            sorted_sid.remove(0)
        for sid in sorted_sid:
            try:
                app_deadline = app_info[app_id][PLOT_SID_STAGE]["start"] + timedelta(
                    milliseconds=config["Deadline"])
                for timestamp in app_info[app_id][sid]["tasktimestamps"]:
                    if first_ts == 0:
                        first_ts = timestamp.timestamp()
                    else:
                        timestamps.append(timestamp.timestamp() - first_ts)
                        times.append(times[-1] + 1)
            except KeyError:
                None

        fig, ax1 = plt.subplots(figsize=(16, 5), dpi=300)
        # PLOT NORMALIZED TASK PROGRESS
        min_times = min(times)
        max_times = max(times)
        normalized = [(z - min_times) / (max_times - min_times) * 100 for z in times]
        ax1.plot(timestamps, normalized, "black", linewidth=3, zorder=10)
        ymin, ymax = ax1.get_ylim()
        ax1.set_ylim(0.0, 100.0)
        xmin, xmax = ax1.get_xlim()
        ax1.set_xlim(0.0, xmax)
        # PLOT DEADLINE
        label_to_plot = {}
        original_ts = {}
        app_dead_ts = app_deadline.timestamp() - first_ts
        ax1.axvline(app_dead_ts, color="green", linewidth=3)
        if "agg" in name or "sort" in name:
            TQ = TQ_MICRO
        else:
            TQ = TQ_KMEANS
        # tq_ts = math.floor(app_dead_ts / TQ) * TQ
        # original_ts[tq_ts] = app_dead_ts
        # try:
        #     label_to_plot[tq_ts].append("D")
        # except KeyError:
        #     label_to_plot[tq_ts] = []
        #     label_to_plot[tq_ts].append("D")
        # ax1.text(app_dead_ts, ymax + 0.5, 'D', weight="bold", horizontalalignment='center')
        # PLOT ALPHA DEADLINE
        app_alpha_deadline = app_deadline - timedelta(
            milliseconds=((1 - float(config["Control"]["Alpha"])) * float(config["Deadline"])))
        app_alpha_deadline_ts = app_alpha_deadline.timestamp() - first_ts
        tq_ts = math.floor(app_alpha_deadline_ts / TQ) * TQ
        original_ts[tq_ts] = app_alpha_deadline_ts
        ax1.axvline(app_alpha_deadline_ts, color="green", linewidth=3, zorder=10)
        try:
            label_to_plot[tq_ts].append("AD")
        except KeyError:
            label_to_plot[tq_ts] = []
            label_to_plot[tq_ts].append("AD")
        # ax1.text(app_alpha_deadline_ts, ymax + 0.5, 'AD', weight="bold",
        # horizontalalignment='center')
        ax1.set_xlabel('Time [s]')
        ax1.set_ylabel('App Progress [%]')

        # PLOT VERTICAL LINES DEAD, START, END
        for sid in sorted_sid:
            try:
                int_dead = app_info[app_id][sid]["deadline"].timestamp()
                dead_ts = int_dead - first_ts
                # tq_ts = math.floor(dead_ts / TQ) * TQ
                # original_ts[tq_ts] = dead_ts
                # if sid == sorted(app_info[app_id])[-1] and dead_ts < app_alpha_deadline_ts:
                #     original_ts[tq_ts] = app_alpha_deadline_ts
                #     ax1.axvline(app_alpha_deadline_ts, color="mediumseagreen", linestyle='--')
                # else:
                #     ax1.axvline(dead_ts, color="mediumseagreen", linestyle='--')
                # try:
                #     label_to_plot[tq_ts].append('D' + str(sid))
                # except KeyError:
                #     label_to_plot[tq_ts] = []
                #     label_to_plot[tq_ts].append('D' + str(sid))
                start_ts = app_info[app_id][sid]["start"].timestamp() - first_ts
                # ax1.axvline(start_ts, color="black")
                try:
                    s_y = normalized[timestamps.index(start_ts)]
                except ValueError:
                    s_y = start_ts
                # ax1.text(start_ts + 3, s_y - 2, 'S' + str(sid), style="italic",weight="bold",
                # horizontalalignment='center')
                end = app_info[app_id][sid]["end"].timestamp()
                end_ts = end - first_ts
                tq_ts = math.floor(end_ts / TQ) * TQ
                original_ts[tq_ts] = end_ts
                try:
                    label_to_plot[tq_ts].append("E" + str(sid))
                except KeyError:
                    label_to_plot[tq_ts] = []
                    label_to_plot[tq_ts].append("E" + str(sid))
                ax1.axvline(end_ts, color="r")
                try:
                    index_t = timestamps.index(end_ts)
                except ValueError:
                    index_t = min(range(len(timestamps)), key=lambda i: abs(timestamps[i] - end_ts))
                e_y = normalized[index_t]
                # ax1.text(end_ts, ymin - 3.5, 'E' + str(sid), style="italic", weight="bold",
                # horizontalalignment='center', verticalalignment='bottom')
            except KeyError:
                None

        if PLOT_LABEL:
            # set the bbox for the text. Increase txt_width for wider text.
            txt_height = 0.05 * (plt.ylim()[1] - plt.ylim()[0])
            txt_width = 0.025 * (plt.xlim()[1] - plt.xlim()[0])
            # Get the corrected text positions, then write the text.
            sorted_label_ts = [original_ts[ts] for ts in sorted(label_to_plot)]
            text_positions = get_text_positions(sorted_label_ts,
                                                np.full(len(label_to_plot), ymax), txt_width,
                                                txt_height)
            # print(text_positions)
            text_plotter(sorted_label_ts, np.full(len(label_to_plot), ymax), text_positions,
                         ax1, txt_width, txt_height,
                         ["/".join(label_to_plot[ts]) for ts in sorted(label_to_plot)])

        ax2 = ax1.twinx()
        ax2.set_ylabel("Core [#]")
        ts_cpu = []
        cpus = []
        sid_len = {}
        for worker_log in workers_dict:
            worker_dict = workers_dict[worker_log]
            for sid in sorted(worker_dict[app_id]):
                try:
                    sid_len[sid] = max(sid_len[sid], len(worker_dict[app_id][sid]["cpu"]))
                except KeyError:
                    sid_len[sid] = len(worker_dict[app_id][sid]["cpu"])
        for sid in sorted(sid_len):
            if sid > sorted(sid_len.keys())[0]:
                print(sid, sorted(sid_len)[0])
                sid_len[sid] += sid_len[list(sorted(sid_len))[list(sorted(sid_len)).index(sid) - 1]]
        sid_len_keys = list(sid_len.keys())
        max_cpu = (len(list(workers_dict.keys())) * COREVM)
        # first_ts = app_info[app_id][PLOT_SID_STAGE]["start"].replace(microsecond=0).timestamp()
        for worker_log in workers_dict.keys():
            print(worker_log)
            worker_dict = workers_dict[worker_log]
            first_sid = sorted(worker_dict[app_id])[0]
            for sid in sorted(worker_dict[app_id]):
                s_index = sid_len[
                    sid_len_keys[sid_len_keys.index(sid) - 1]] if sid != first_sid else 0
                for i, (time_cpu, cpu) in enumerate(
                        zip(worker_dict[app_id][sid]["time"], worker_dict[app_id][sid]["cpu"])):
                    time_cpu_ts = time_cpu.replace(microsecond=0).timestamp()
                    if "2016-10-02_16-43-31" in worker_log:
                        if "74.out" in worker_log and sid == 2:
                            time_cpu_ts -= 6
                        if "100.out" in worker_log and sid == 2:
                            time_cpu_ts -= 2
                        elif "2016-10-03_08-01-39" in worker_log:
                            if "38.out" in worker_log and sid == 1:
                                time_cpu_ts -= 1
                    try:
                        if ts_cpu[s_index] != (time_cpu_ts - first_ts) and abs(
                                        ts_cpu[s_index] - (time_cpu_ts - first_ts)) >= (
                                    config["Control"]["Tsample"] / 1000):
                            # print(ts_cpu)
                            # print([tixm.replace(microsecond=0).timestamp() -
                            # first_ts for tixm in worker_dict[app_id][sid]["time"]])
                            # print("QUI", sid, (time_cpu_ts - first_ts),  ts_cpu[s_index])
                            index = ts_cpu.index(time_cpu_ts - first_ts)
                            cpus[index] += cpu
                        else:
                            if (cpus[s_index] + cpu) > max_cpu:
                                print("+0 ERR")
                                if (cpus[s_index + 1] + cpu) > max_cpu:
                                    print("+1 ERR")
                                    if (cpus[s_index + 2] + cpu) > max_cpu: print("+2 ERR")
                                    cpus[s_index + 2] += cpu
                                else:
                                    cpus[s_index + 1] += cpu
                            else:
                                cpus[s_index] += cpu
                            if ts_cpu[s_index] == 0.0:
                                print(time_cpu_ts - first_ts)
                                ts_cpu[s_index] = time_cpu_ts - first_ts
                    except (IndexError, ValueError):
                        ts_cpu.append(time_cpu_ts - first_ts)
                        cpus.append(cpu)
                    s_index += 1
                padding = sid_len[sid] - len(worker_dict[app_id][sid]["cpu"])
                if len(ts_cpu) < sid_len[sid] and padding > 0:
                    for i in range(padding):
                        next_ts = ts_cpu[-1] + (config["Control"]["Tsample"] / 1000)
                        if next_ts <= (end_ts - first_ts):
                            ts_cpu.append(next_ts)
                            cpus.append(0.0)
        for sid in sorted_sid:
            if sid != sorted_sid[-1]:
                end = app_info[app_id][sid]["end"].replace(microsecond=100).timestamp() - first_ts
                next_sid = sorted_sid[sorted_sid.index(sid) + 1]
                next_start = app_info[app_id][next_sid]["start"].replace(
                    microsecond=0).timestamp() - first_ts
                if (next_start - end) > (config["Control"]["Tsample"] / 1000):
                    print("ERR ", sid)
                    ts_cpu.append(end)
                    cpus.append(0.0)
                    ts_cpu.append(next_start)
                    cpus.append(0.0)
        ts_cpu, cpus = (list(t) for t in zip(*sorted(zip(ts_cpu, cpus))))
        ts_cpu.append(end_ts)
        cpus.append(cpus[-1] * 2 / 3)
        ax2.plot(ts_cpu, cpus, zorder=0)
        xmin, xmax = ax2.get_xlim()
        ax2.set_xlim(0.0, xmax)
        ymin, ymax = ax2.get_ylim()
        print(ymax)
        ax2.set_ylim(0.0, len(list(workers_dict.keys())) * COREVM)

        ax2.fill_between(ts_cpu, 0.0, cpus, facecolor="b", alpha=0.2)
        yaxis_multiplier = float(COREVM)
        if len(workers_dict) > 11:
            yaxis_multiplier = float(COREVM) * 2
        ax2.yaxis.set_major_locator(plticker.MultipleLocator(base=yaxis_multiplier))
        ax1.yaxis.set_major_locator(plticker.MultipleLocator(base=10.0))

        if "agg" in name:
            ax1.xaxis.set_major_locator(plticker.MultipleLocator(base=Y_TICK_AGG))
        elif "sort" in name:
            ax1.xaxis.set_major_locator(plticker.MultipleLocator(base=Y_TICK_SORT))
        if TITLE:
            plt.title(app_id + " " + str(config["Deadline"]) + " " + str(
                config["Control"]["Tsample"]) + " " + str(
                config["Control"]["Alpha"]) + " " + str(config["Control"]["K"]))

        ax1.set_zorder(ax2.get_zorder() + 1)
        ax1.patch.set_visible(False)
        labels = ax1.get_xticklabels()
        plt.setp(labels, rotation=45)
        # latexify(columns=1)
        # format_axes(ax1)
        # format_axes(ax2)
        if PDF:
            plt.savefig(folder + name + ".pdf", bbox_inches='tight', dpi=300)
        else:
            plt.savefig(folder + name + ".png", bbox_inches='tight', dpi=300)
        plt.close()


def find_first_ts_worker(app_id, workers_dict):
    """
    Find the first start ts of the stage PLOT_SID_STAGE of all the workers

    :param app_id: the id of the application
    :param workers_dict:  the dictionary data of the workers
    :return: the timestamp of the first worker to start the PLOT_SID_STAGE
    """
    first_ts_worker = float('inf')
    for worker_log in workers_dict:
        try:
            first_ts_worker = min(first_ts_worker,
                                  workers_dict[worker_log][app_id][PLOT_SID_STAGE]["time"][
                                      0].timestamp())
        except KeyError:
            None
    return first_ts_worker


@timing
def plot(folder):
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
            plot_app_overview(app_id, app_info[app_id], folder, config)

    worker_logs = glob.glob(folder + "*worker*.out")
    cpu_logs = glob.glob(folder + "sar*.log")

    if len(worker_logs) == len(cpu_logs):
        workers_dict = {}
        for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
            worker_dict = load_worker_data(worker_log, cpu_log)
            workers_dict[worker_log] = worker_dict

        first_ts_worker = -1
        for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
            for app_id in app_info:
                if first_ts_worker == -1: first_ts_worker = find_first_ts_worker(app_id,
                                                                                 workers_dict)
                plot_worker(app_id, app_info, worker_log, workers_dict[worker_log], config,
                            first_ts_worker)
        for app_id in app_info:
            plot_overview_cpu(app_id, app_info, workers_dict, config, folder)
    else:
        print("ERROR: SAR != WORKER LOGS")
