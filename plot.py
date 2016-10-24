import glob
import math
import random
import time
from datetime import datetime as dt
from datetime import timedelta

import matplotlib.colors as colors
import matplotlib.dates as mpdate
import matplotlib.pyplot as plt
import numpy as np

from config import HDFS, ALPHA, DELETE_HDFS, DEADLINE, SCALE_FACTOR, K, TSAMPLE, MAXEXECUTOR, COREVM, COREHTVM

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


@timing
def plot(folder):
    # COREVM = 12
    # COREHTVM = 12
    # DEADLINE = 82800
    # SCALE_FACTOR = 0.1
    # K = 50
    # TSAMPLE = 500
    # STRPTIME_FORMAT = '%H:%M:%S,%f'
    bench_log = glob.glob(folder + "*.err") + glob.glob(folder + "*.dat")
    dict_to_plot = {}
    app_info = {}
    for bench in sorted(bench_log):
        # 16/08/30 21:45:51 INFO ControllerJob: SEND INIT TO EXECUTOR CONTROLLER EID 0, SID 2, TASK 150, DL 81322, C 12
        # 16/08/30 21:46:13 INFO DAGScheduler: ResultStage 2 (count at KVDataTest.scala:151) finished in 22.195 s
        # 16/08/31 14:30:28 INFO ControllerJob: SEND NEEDED CORE TO MASTER spark://ec2-52-42-181-165.us-west-2.compute.amazonaws.com:7077, 0, Vector(4, 4, 4, 4), app-20160831142852-0000
        # 16/09/11 12:39:19,930 INFO DAGScheduler: Submitting 60 missing tasks from ResultStage 0 (GraphLoader.edgeListFile - edges (hdfs://10.8.0.1:9000/SparkBench/PageRank/Input) MapPartitionsRDD[3] at mapPartitionsWithIndex at GraphLoader.scala:75)
        # 16/09/11 12:41:13,537 INFO TaskSetManager: Finished task 10.0 in stage 1.0 (TID 78) in 1604 ms on 131.175.135.183 (1/60)
        app_id = ""
        with open(bench) as applog:
            stage_id = -1 if HDFS else 0
            for line in applog:
                line = line.split(" ")
                # 16/09/11 12:39:18,070 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20160911123918-0002
                if len(line) > 3 and line[3] == "TaskSetManager:" and line[4] == "Finished":
                    try:
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                            dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
                    except KeyError:
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"] = []
                        app_info[app_id][int(float(line[9]))]["tasktimestamps"].append(
                            dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
                if len(line) > 3 and line[3] == "StandaloneSchedulerBackend:" and line[4] == "Connected":
                    app_info[line[-1].rstrip()] = {}
                    app_id = line[-1].rstrip()
                    dict_to_plot[app_id] = {}
                    dict_to_plot[app_id]["dealineTimeStages"] = []
                    dict_to_plot[app_id]["startTimeStages"] = []
                    dict_to_plot[app_id]["finishTimeStages"] = []
                elif len(line) > 12 and line[3] == "ControllerJob:":
                    if line[5] == "INIT":
                        if stage_id != int(line[12].replace(",", "")) or int(line[12].replace(",", "")) == 0:
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
                        app_info[app_id][int(line[10])] = {}
                        app_info[app_id][int(line[10])]["tasks"] = int(line[5])
                        app_info[app_id][int(line[10])]["start"] = dt.strptime(line[1], STRPTIME_FORMAT).replace(
                            year=2016)
                    elif line[-4] == "finished":
                        if app_id != "":
                            stage_id = int(line[5])
                            app_info[app_id][stage_id]["end"] = dt.strptime(line[1], STRPTIME_FORMAT).replace(
                                year=2016)
                            if len(dict_to_plot[app_id]["startTimeStages"]) > len(
                                    dict_to_plot[app_id]["finishTimeStages"]):
                                dict_to_plot[app_id]["finishTimeStages"].append(app_info[app_id][stage_id]["end"])
                                print("END: " + str(app_info[app_id][stage_id]["end"]))

                elif len(line) > 10 and line[5] == "added:" and line[4] == "Executor":
                    None
                    # 16/08/31 14:28:52 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20160831142852-0000/0 on worker-20160831142832-ec2-52-43-162-151.us-west-2.compute.amazonaws.com-9999 (ec2-52-43-162-151.us-west-2.compute.amazonaws.com:9999) with 8 cores
                    # COREHTVM = int(l[-2])

    for app in app_info.keys():
        if len(app_info[app]) > 0:
            timestamps = []
            times = []
            deadlineapp = 0
            for sid in sorted(app_info[app].keys()):
                try:
                    deadlineapp = app_info[app][DELETE_HDFS]["start"] + timedelta(milliseconds=DEADLINE)
                    for timestamp in app_info[app][sid]["tasktimestamps"]:
                        timestamps.append(timestamp)
                        if len(times) == 0:
                            times.append(1)
                        else:
                            times.append(times[-1] + 1)
                except KeyError:
                    None

            fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)
            # PLOT NORMALIZED TASK PROGRESS
            normalized = [(z - min(times)) / (max(times) - min(times)) for z in times]
            ax1.plot(timestamps, normalized, ".k-")
            ymin, ymax = ax1.get_ylim()
            ax1.axvline(deadlineapp)
            ax1.text(deadlineapp, ymax, 'DEADLINE APP', rotation=90)
            app_alpha_deadline = deadlineapp - timedelta(milliseconds=((1 - ALPHA) * DEADLINE))
            ax1.axvline(app_alpha_deadline)
            ax1.text(app_alpha_deadline, ymax, 'ALPHA DEADLINE', rotation=90)
            ax1.set_xlabel('time')
            ax1.set_ylabel('app progress')
            errors = []
            for sid in sorted(app_info[app].keys()):
                # color = colors_stage.popitem()[1]
                int_dead = 0
                try:
                    ax1.axvline(app_info[app][sid]["deadline"], color="r", linestyle='--')
                    ax1.text(app_info[app][sid]["deadline"], ymin + 0.15 * ymax, 'DEAD SID ' + str(sid), rotation=90)
                    int_dead = app_info[app][sid]["deadline"].timestamp()
                    # if sid != 0:
                    ax1.axvline(app_info[app][sid]["start"], color="b")
                    ax1.text(app_info[app][sid]["start"], ymax - 0.02 * ymax, 'START SID ' + str(sid), rotation=90, )
                    ax1.axvline(app_info[app][sid]["end"], color="r")
                    ax1.text(app_info[app][sid]["end"], ymax - 0.25 * ymax, 'END SID ' + str(sid), rotation=90)
                    # if int_dead != 0 and sid != DELETE_HDFS:
                    end = app_info[app][sid]["end"].timestamp()
                    duration = app_alpha_deadline.timestamp() - app_info[app][DELETE_HDFS]["start"].timestamp()
                    error = round(round(((abs(int_dead - end)) / duration), 3) * 100, 3)
                    errors.append(error)
                    ax1.text(app_info[app][sid]["end"], ymax - random.uniform(0.4, 0.5) * ymax,
                             "E " + str(error) + "%", rotation=90)
                except KeyError:
                    None

            try:
                end = app_info[app][sorted(app_info[app].keys())[-1]]["end"].timestamp()
            except KeyError:
                None
            int_dead = app_alpha_deadline.timestamp()
            duration = int_dead - app_info[app][DELETE_HDFS]["start"].timestamp()
            error = round(round(((int_dead - end) / duration), 3) * 100, 3)
            ax1.text(app_alpha_deadline, ymax - 0.5 * ymax, "ERROR = " + str(error) + "%")

            np_errors = np.array(errors)
            print("DEADLINE_ERROR " + str(abs(error)))
            if len(np_errors) > 0:
                print("MEAN ERROR: " + str(np.mean(np_errors)))
                print("DEVSTD ERROR: " + str(np.std(np_errors)))
                print("MEDIAN ERROR: " + str(np.median(np_errors)))
                print("MAX ERROR: " + str(max(np_errors)))
                print("MIN ERROR: " + str(min(np_errors)))

            with open(folder + "ERROR.txt", "w") as error_f:
                error_f.write("DEADLINE_ERROR " + str(abs(error)) + "\n")
                if len(np_errors) > 0:
                    error_f.write("MEAN_ERROR " + str(np.mean(np_errors)) + "\n")
                    error_f.write("DEVSTD_ERROR: " + str(np.std(np_errors)) + "\n")
                    error_f.write("MEDIAN_ERROR: " + str(np.median(np_errors)) + "\n")
                    error_f.write("MAX_ERROR: " + str(max(np_errors)) + "\n")
                    error_f.write("MIN_ERROR: " + str(min(np_errors)) + "\n")

            labels = ax1.get_xticklabels()
            plt.setp(labels, rotation=90)
            locator = mpdate.SecondLocator(interval=SECONDLOCATOR)
            plt.gca().xaxis.set_major_locator(locator)
            plt.gca().xaxis.set_major_formatter(mpdate.DateFormatter(STRPTIME_FORMAT))
            plt.gcf().autofmt_xdate()
            if TITLE:
                plt.title(app + " " + str(SCALE_FACTOR) + " " + str(DEADLINE) + " " + str(TSAMPLE) + " " + str(
                    ALPHA) + " " + str(K))
            plt.savefig(folder + app + ".png", bbox_inches='tight', dpi=300)
            plt.close()

    worker_logs = glob.glob(folder + "*worker*.out")
    cpu_logs = glob.glob(folder + "sar*.log")

    # Check len log
    print(len(worker_logs), len(cpu_logs))

    if len(worker_logs) == len(cpu_logs):
        for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
            print(worker_log)
            print(cpu_log)
            # 16/08/31 14:29:43 INFO Worker: Scaled executorId 2  of appId app-20160831142852-0000 to  8 Core
            # 16/09/11 17:37:18,062 INFO Worker: Created ControllerExecutor: 0 , 1 , 16000 , 30 , 6
            with open(worker_log) as wlog:
                app_id = ""
                dict_to_plot[worker_log] = {}
                dict_to_plot[worker_log]["cpu_real"] = []
                dict_to_plot[worker_log]["time_cpu"] = []
                for line in wlog:
                    line = line.split(" ")
                    if len(line) > 3:
                        if line[4] == "Created" and app_id != "":
                            dict_to_plot[app_id][worker_log]["cpu"].append(float(line[-1].replace("\n", "")))
                            dict_to_plot[app_id][worker_log]["sp_real"].append(0.0)
                            dict_to_plot[app_id][worker_log]["time"].append(
                                dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
                            dict_to_plot[app_id][worker_log]["sp"].append(0.0)
                        if line[4] == "Scaled":
                            # print(l)
                            if app_id == "" or app_id != line[10]:
                                next_app_id = line[10]
                                try:
                                    dict_to_plot[next_app_id][worker_log] = {}
                                    dict_to_plot[next_app_id][worker_log]["cpu"] = []
                                    dict_to_plot[next_app_id][worker_log]["time"] = []
                                    dict_to_plot[next_app_id][worker_log]["sp_real"] = []
                                    dict_to_plot[next_app_id][worker_log]["sp"] = []
                                    app_id = next_app_id
                                except KeyError:
                                    print(next_app_id + " NOT FOUND BEFORE IN BENCHMARK LOGS")
                        if app_id != "":
                            if line[4] == "CoreToAllocate:":
                                # print(l)
                                dict_to_plot[app_id][worker_log]["cpu"].append(float(line[-1].replace("\n", "")))
                            if line[4] == "Real:":
                                dict_to_plot[app_id][worker_log]["sp_real"].append(float(line[-1].replace("\n", "")))
                            if line[4] == "SP":
                                dict_to_plot[app_id][worker_log]["time"].append(
                                    dt.strptime(line[1], STRPTIME_FORMAT).replace(year=2016))
                                # print(l[-1].replace("\n", ""))
                                progress = float(line[-1].replace("\n", ""))
                                # print(sp)
                                if progress < 0.0:
                                    dict_to_plot[app_id][worker_log]["sp"].append(abs(progress) / 100)
                                else:
                                    dict_to_plot[app_id][worker_log]["sp"].append(progress)

            with open(cpu_log) as cpulog:
                for line in cpulog:
                    line = line.split("    ")
                    if not ("Linux" in line[0].split(" ") or "\n" in line[0].split(" ")) and line[1] != " CPU" and line[
                        0] != "Average:":
                        dict_to_plot[worker_log]["time_cpu"].append(
                            dt.strptime(line[0], '%I:%M:%S %p').replace(year=2016))
                        cpuint = float('{0:.2f}'.format((float(line[2]) * COREHTVM) / 100))
                        dict_to_plot[worker_log]["cpu_real"].append(cpuint)
    else:
        print("ERROR: SAR != WORKER LOGS")

    for app_id in sorted(dict_to_plot.keys()):
        if len(app_id) <= len("app-20160831142852-0000"):
            cpu_time = 0
            cpu_time_max = 0
            for worker in dict_to_plot[app_id].keys():
                if worker not in ["startTimeStages", "dealineTimeStages", "finishTimeStages"]:
                    cpu_time += (TSAMPLE / 1000) * sum(dict_to_plot[app_id][worker]["cpu"])
                    for cpu, time in zip(dict_to_plot[app_id][worker]["cpu"], dict_to_plot[app_id][worker]["time"]):
                        try:
                            index = dict_to_plot[worker]["time_cpu"].index(time)
                        except ValueError:
                            index = dict_to_plot[worker]["time_cpu"].index(
                                min(dict_to_plot[worker]["time_cpu"],
                                    key=lambda x: x - time if x > time else timedelta.max))
                        cpu_time_max += (TSAMPLE / 1000) * max(cpu, dict_to_plot[worker]["cpu_real"][
                            index + int(TSAMPLE / 1000)])

            if cpu_time == 0:
                cpu_time = ((app_info[app_id][max(list(app_info[app_id].keys()))]["end"].timestamp() -
                             app_info[app_id][0]["start"].timestamp())) * MAXEXECUTOR * COREVM
                cpu_time_max = cpu_time
            cpu_time_max = math.floor(cpu_time_max)
            print("CPU_TIME: " + str(cpu_time))
            print("CPU TIME MAX: " + str(cpu_time_max))
            print("SID " + str(app_info[app_id].keys()))
            print("CHECK NON CONTROLLED STAGE FOR CPU_TIME")

    with open(folder + "CPU_TIME.txt", "w") as cpu_time_f:
        if dict_to_plot:
            cpu_time_f.write("CPU_TIME " + str(cpu_time) + "\n")
            cpu_time_f.write("CPU_TIME_MAX " + str(cpu_time_max) + "\n")

    # print(plotDICT)
    for app_id in sorted(dict_to_plot.keys()):
        if len(app_id) <= len("app-20160831142852-0000"):
            for worker in dict_to_plot[app_id].keys():
                colors_stage = list(colors.cnames.values())
                if worker not in ["startTimeStages", "dealineTimeStages", "finishTimeStages"] and len(
                        dict_to_plot[app_id][worker]["sp"]) > 0:
                    # print(appID, worker)
                    fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)
                    # pos = np.where(np.abs(np.diff(plotDICT[appID][worker]["time"])) >= timedelta(
                    #         milliseconds=TSAMPLE*2))[0] + 1
                    # print(pos)
                    # if pos != []:
                    #     for p in pos:
                    #         plotDICT[appID][worker]["time"] = np.insert(plotDICT[appID][worker]["time"], p, plotDICT[appID][worker]["time"][p-1])
                    #         plotDICT[appID][worker]["sp"] = np.insert(plotDICT[appID][worker]["sp"], p, 0)
                    #         plotDICT[appID][worker]["sp_real"] = np.insert(plotDICT[appID][worker]["sp_real"], p, 0)
                    #         plotDICT[appID][worker]["cpu"] = np.insert(plotDICT[appID][worker]["cpu"], p, 0)
                    if len(dict_to_plot[app_id][worker]["sp"]) > 0:
                        sp_plt, = ax1.plot(dict_to_plot[app_id][worker]["time"], dict_to_plot[app_id][worker]["sp"],
                                           ".r-",
                                           label='PROGRESS')
                    if len(dict_to_plot[app_id][worker]["sp_real"]) > 0:
                        sp_real_plt, = ax1.plot(dict_to_plot[app_id][worker]["time"],
                                                dict_to_plot[app_id][worker]["sp_real"],
                                                ".k-",
                                                label='PROGRESS REAL')
                    ax1.set_xlabel('time')
                    ax1.set_ylabel('stage progress')

                    for starttime, finishtime in zip(dict_to_plot[app_id]["startTimeStages"],
                                                     dict_to_plot[app_id]["finishTimeStages"]):
                        color = colors_stage[random.randint(0, len(colors_stage) - 1)]
                        ax1.axvline(starttime, color=color, linestyle='--')
                        ax1.axvline(finishtime, color=color)
                    for deadline in dict_to_plot[app_id]["dealineTimeStages"]:
                        ax1.axvline(deadline, color="red", linestyle='--')

                    ax1.spines["top"].set_visible(False)
                    ax1.spines["right"].set_visible(False)

                    ax1.get_xaxis().tick_bottom()
                    ax1.get_yaxis().tick_left()

                    xlim = ax1.get_xlim()
                    factor = 0.1
                    new_xlim = (xlim[0] + xlim[1]) / 2 + np.array((-0.5, 0.5)) * (xlim[1] - xlim[0]) * (1 + factor)
                    ax1.set_xlim(new_xlim)

                    ylim = ax1.get_ylim()
                    factor = 0.1
                    new_ylim = (ylim[0] + ylim[1]) / 2 + np.array((-0.5, 0.5)) * (ylim[1] - ylim[0]) * (1 + factor)
                    ax1.set_ylim(new_ylim)

                    ax2 = ax1.twinx()
                    cpu_plt, = ax2.plot(dict_to_plot[app_id][worker]["time"], dict_to_plot[app_id][worker]["cpu"],
                                        ".b-",
                                        label='CPU')
                    if len(dict_to_plot[worker]["cpu_real"]) > 0:
                        index_init = dict_to_plot[worker]["time_cpu"].index(
                            min(dict_to_plot[worker]["time_cpu"],
                                key=lambda x: x - dict_to_plot[app_id][worker]["time"][0] if x >
                                                                                             dict_to_plot[app_id][
                                                                                                 worker][
                                                                                                 "time"][
                                                                                                 0] else timedelta.max))
                        index_end = dict_to_plot[worker]["time_cpu"].index(
                            min(dict_to_plot[worker]["time_cpu"],
                                key=lambda x: x - dict_to_plot[app_id][worker]["time"][-1] if x > dict_to_plot[app_id][
                                    worker][
                                    "time"][-1] else timedelta.max))
                        print(index_init, index_end)
                        cpu_real, = ax2.plot(dict_to_plot[worker]["time_cpu"][index_init:index_end + 1],
                                             dict_to_plot[worker]["cpu_real"][index_init:index_end + 1], ".g-",
                                             label='CPU REAL')
                        # plt.legend(handles=[sp_plt, sp_real_plt, cpu_plt, cpu_real], bbox_to_anchor=(1, 0.2), prop={'size': 12})
                        plt.legend(handles=[sp_plt, sp_real_plt, cpu_plt, cpu_real], loc='best', prop={'size': 12})
                    else:
                        plt.legend(handles=[sp_plt, sp_real_plt, cpu_plt], bbox_to_anchor=(1, 0.15), prop={'size': 12})

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
                    factor = 0.1
                    new_ylim = (ylim[0] + ylim[1]) / 2 + np.array((-0.5, 0.5)) * (ylim[1] - ylim[0]) * (1 + factor)
                    ax2.set_ylim(new_ylim)
                    locator = mpdate.SecondLocator(interval=SECONDLOCATOR)
                    plt.gca().xaxis.set_major_locator(locator)
                    plt.gca().xaxis.set_major_formatter(mpdate.DateFormatter(STRPTIME_FORMAT))
                    plt.gcf().autofmt_xdate()
                    # print(worker)
                    # for starttime, finishtime in zip(plotDICT[appID]["startTimeStages"],
                    #                                  plotDICT[appID]["finishTimeStages"]):
                    #
                    #     b_d = starttime
                    #
                    #     def func(x):
                    #         delta = x - b_d if x > b_d else timedelta.max
                    #         return delta
                    #
                    #     indexStart = plotDICT[appID][worker]["time"].index(
                    #         min(plotDICT[appID][worker]["time"], key=func))
                    #
                    #     if starttime < plotDICT[appID][worker]["time"][indexStart] and plotDICT[appID][worker]["time"][indexStart] - starttime <= timedelta(seconds=10):
                    #         print("START", starttime, plotDICT[appID][worker]["time"][indexStart])
                    #         b_d = finishtime
                    #         indexFinish = plotDICT[appID][worker]["time"].index(
                    #             min(plotDICT[appID][worker]["time"], key=func))
                    #         print("END", finishtime, plotDICT[appID][worker]["time"][indexFinish -1])
                    #         if plotDICT[appID][worker]["time"][indexFinish - 1] - finishtime <= timedelta(seconds=10):
                    #             print("END OK", finishtime, plotDICT[appID][worker]["time"][indexFinish -1])
                    #             b_d = starttime
                    #             indexInit = plotDICT[worker]["time_cpu"].index(min(plotDICT[worker]["time_cpu"], key=func))
                    #             b_d = finishtime
                    #             indexEnd = plotDICT[worker]["time_cpu"].index(min(plotDICT[worker]["time_cpu"], key=func))
                    #             if indexFinish == 0:
                    #                 indexFinish = len(plotDICT[appID][worker]["cpu"])
                    #             mean_cpu = np.mean(plotDICT[appID][worker]["cpu"][indexStart:indexFinish -1])
                    #             mean_cpu_real = np.mean(plotDICT[worker]["cpu_real"][indexInit:indexEnd])
                    #
                    #             xmin = float(indexStart) / len(plotDICT[appID][worker]["time"])
                    #             xmax = float(indexFinish - 1) / len(plotDICT[appID][worker]["time"])
                    #             xmin = xmin + 0.1
                    #             xmax = xmax + 0.1
                    #             if xmax == 0.0 or xmax >= 0.9:
                    #                 xmax = 0.9
                    #             print(mean_cpu, mean_cpu_real, xmin, xmax)
                    #             ax2.axhline(y=mean_cpu, xmin=xmin, xmax=xmax, c="blue", linewidth=2)
                    #             ax2.axhline(y=mean_cpu_real, xmin=xmin, xmax=xmax, c="green", linewidth=2)
                    if TITLE:
                        plt.title(
                            app_id + " " + str(SCALE_FACTOR) + " " + str(DEADLINE) + " " + str(TSAMPLE) + " " + str(
                                ALPHA) + " " + str(K))
                    plt.savefig(worker + "." + app_id + '.png', bbox_inches='tight', dpi=300)
                    plt.close()
        else:
            worker = app_id

            fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)

            ax1.set_xlabel('time')
            ax1.set_ylabel('cpu')

            ax1.spines["top"].set_visible(False)
            ax1.spines["right"].set_visible(False)

            ax1.get_xaxis().tick_bottom()
            ax1.get_yaxis().tick_left()

            ax2 = ax1.twinx()

            if len(dict_to_plot[worker]["cpu_real"]) > 0:
                cpu_real, = ax2.plot(dict_to_plot[worker]["time_cpu"],
                                     dict_to_plot[worker]["cpu_real"], ".g-",
                                     label='CPU REAL')
                plt.legend(handles=[cpu_real], bbox_to_anchor=(1.1, -0.025))

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
            factor = 0.1
            new_ylim = (ylim[0] + ylim[1]) / 2 + np.array((-0.5, 0.5)) * (ylim[1] - ylim[0]) * (1 + factor)
            ax2.set_ylim(new_ylim)
            # plt.title(appID + " " + str(SCALE_FACTOR) + " " + str(DEADLINE) + " " + str(TSAMPLE) + " " + str(
            #   ALPHA) + " " + str(K))
            locator = mpdate.SecondLocator(interval=SECONDLOCATOR)
            plt.gca().xaxis.set_major_locator(locator)

            plt.gcf().autofmt_xdate()
            # plt.savefig(worker + '.png', bbox_inches='tight', dpi=300)
            plt.close()
