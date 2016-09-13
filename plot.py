import matplotlib.pyplot as plt
import matplotlib.colors as colors
import glob
from datetime import datetime
from datetime import timedelta
import numpy as np

from config import *

# STRPTIME_FORMAT = '%H:%M:%S,%f'
STRPTIME_FORMAT = '%H:%M:%S'
SECONDLOCATOR = 10

def plot(folder):
    # print("COREVM = " + str(COREVM))
    # COREVM = 12
    # COREHTVM = 12
    # DEADLINE = 82800
    # SCALE_FACTOR = 0.6
    benchLog = glob.glob(folder + "*.err") + glob.glob(folder + "*.dat")
    plotDICT = {}
    appIDinfo = {}
    for bench in sorted(benchLog):
        # 16/08/30 21:45:51 INFO ControllerJob: SEND INIT TO EXECUTOR CONTROLLER EID 0, SID 2, TASK 150, DL 81322, C 12
        # 16/08/30 21:46:13 INFO DAGScheduler: ResultStage 2 (count at KVDataTest.scala:151) finished in 22.195 s
        # 16/08/31 14:30:28 INFO ControllerJob: SEND NEEDED CORE TO MASTER spark://ec2-52-42-181-165.us-west-2.compute.amazonaws.com:7077, 0, Vector(4, 4, 4, 4), app-20160831142852-0000
        # 16/09/11 12:39:19,930 INFO DAGScheduler: Submitting 60 missing tasks from ResultStage 0 (GraphLoader.edgeListFile - edges (hdfs://10.8.0.1:9000/SparkBench/PageRank/Input) MapPartitionsRDD[3] at mapPartitionsWithIndex at GraphLoader.scala:75)
        # 16/09/11 12:41:13,537 INFO TaskSetManager: Finished task 10.0 in stage 1.0 (TID 78) in 1604 ms on 131.175.135.183 (1/60)
        appID = ""
        with open(bench) as applog:
            SID = 0
            for line in applog:
                l = line.split(" ")
                # 16/09/11 12:39:18,070 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20160911123918-0002
                if len(l) > 3 and l[3] == "TaskSetManager:" and l[4] == "Finished":
                    try:
                        appIDinfo[appID][int(float(l[9]))]["tasktimestamps"].append(
                            datetime.strptime(l[1], STRPTIME_FORMAT).replace(year=2016))
                    except KeyError:
                        appIDinfo[appID][int(float(l[9]))]["tasktimestamps"] = []
                        appIDinfo[appID][int(float(l[9]))]["tasktimestamps"].append(
                            datetime.strptime(l[1], STRPTIME_FORMAT).replace(year=2016))
                if len(l) > 3 and l[3] == "StandaloneSchedulerBackend:" and l[4] == "Connected":
                    appIDinfo[l[-1].rstrip()] = {}
                    appID = l[-1].rstrip()
                    plotDICT[appID] = {}
                    plotDICT[appID]["dealineTimeStages"] = []
                    plotDICT[appID]["startTimeStages"] = []
                    plotDICT[appID]["finishTimeStages"] = []
                elif len(l) > 3 and l[3] == "ControllerJob:":
                    if l[5] == "INIT" and SID != int(l[12].replace(",", "")):
                        SID = int(l[12].replace(",", ""))
                        appIDinfo[appID][SID]["start"] = datetime.strptime(l[1], STRPTIME_FORMAT).replace(year=2016)
                        plotDICT[appID]["startTimeStages"].append(appIDinfo[appID][SID]["start"])
                        print("START: " + str(datetime.strptime(l[1], STRPTIME_FORMAT).replace(year=2016)))
                        print(l[16].replace(",", ""))
                        appIDinfo[appID][SID]["deadline"] = plotDICT[appID]["startTimeStages"][-1] + timedelta(
                            milliseconds=float(l[16].replace(",", "")))
                        plotDICT[appID]["dealineTimeStages"].append(appIDinfo[appID][SID]["deadline"])
                    if l[5] == "NEEDED" and l[4] == "SEND":
                        nextAppID = l[-1].replace("\n", "")
                        if appID != nextAppID:
                            appID = nextAppID
                            plotDICT[appID] = {}
                            plotDICT[appID]["dealineTimeStages"] = []
                            plotDICT[appID]["startTimeStages"] = []
                            plotDICT[appID]["finishTimeStages"] = []
                elif len(l) > 3 and l[3] == "DAGScheduler:":
                    if l[4] == "Submitting" and l[6] == "missing":
                        appIDinfo[appID][int(l[10])] = {}
                        appIDinfo[appID][int(l[10])]["tasks"] = int(l[5])
                        appIDinfo[appID][int(l[10])]["start"] = datetime.strptime(l[1], STRPTIME_FORMAT).replace(
                            year=2016)
                    elif l[-4] == "finished":
                        if appID != "":
                            SID = int(l[5])
                            appIDinfo[appID][SID]["end"] = datetime.strptime(l[1], STRPTIME_FORMAT).replace(
                                year=2016)
                            if len(plotDICT[appID]["startTimeStages"]) > len(plotDICT[appID]["finishTimeStages"]):
                                plotDICT[appID]["finishTimeStages"].append(appIDinfo[appID][SID]["end"])
                                print("END: " + str(appIDinfo[appID][SID]["end"]))

                elif len(l) > 10 and l[5] == "added:" and l[4] == "Executor":
                    # 16/08/31 14:28:52 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20160831142852-0000/0 on worker-20160831142832-ec2-52-43-162-151.us-west-2.compute.amazonaws.com-9999 (ec2-52-43-162-151.us-west-2.compute.amazonaws.com:9999) with 8 cores
                    COREHTVM = int(l[-2])

                    # print(plotDICT.keys())
    print(appIDinfo)


    for app in appIDinfo.keys():
        if len(appIDinfo[app]) > 0:
            x = []
            y = []
            colors_stage = colors.cnames
            deadlineapp = 0
            for sid in sorted(appIDinfo[app].keys()):
                if sid == 1:
                    deadlineapp = appIDinfo[app][sid]["start"] + timedelta(milliseconds=DEADLINE)
                for timestamp in appIDinfo[app][sid]["tasktimestamps"]:
                    x.append(timestamp)
                    if len(y) == 0:
                        y.append(1)
                    else:
                        y.append(y[-1] + 1)

            fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)

            taskprogress, = ax1.plot(x, y, ".k-")
            ymin, ymax = ax1.get_ylim()
            ax1.axvline(deadlineapp)
            ax1.text(deadlineapp, ymax, 'DEADLINE APP', rotation=90)
            deadlineAlphaApp = deadlineapp - timedelta(milliseconds=((1 - ALPHA) * DEADLINE))
            ax1.axvline(deadlineAlphaApp)
            ax1.text(deadlineAlphaApp, ymax, 'ALPHA DEADLINE', rotation=90)
            for sid in sorted(appIDinfo[app].keys()):
                color = colors_stage.popitem()[1]
                try:
                    ax1.axvline(appIDinfo[app][sid]["deadline"], color="r", linestyle='--')
                    ax1.text(appIDinfo[app][sid]["deadline"], ymin + 0.15 * ymax, 'DEAD SID ' + str(sid), rotation=90)
                except KeyError:
                    None
                if sid != 0:
                    ax1.axvline(appIDinfo[app][sid]["start"], color="b")
                    ax1.text(appIDinfo[app][sid]["start"], ymax - 0.02 * ymax, 'START SID ' + str(sid), rotation=90, )
                ax1.axvline(appIDinfo[app][sid]["end"], color="r")
                ax1.text(appIDinfo[app][sid]["end"], ymax - 0.2 * ymax, 'END SID ' + str(sid), rotation=90)

            # import matplotlib.dates as mdate
            # print(int(len(x) / 10 ))
            # locator = mdate.SecondLocator(interval=10)
            # plt.gca().xaxis.set_major_locator(locator)
            #
            # plt.gcf().autofmt_xdate()
            labels = ax1.get_xticklabels()
            plt.setp(labels, rotation=90)
            import matplotlib.dates as mdate
            locator = mdate.SecondLocator(interval=SECONDLOCATOR)
            plt.gca().xaxis.set_major_locator(locator)

            plt.gcf().autofmt_xdate()
            plt.title(app + " " + str(SCALE_FACTOR) + " " + str(DEADLINE) + " " + str(TSAMPLE) + " " + str(
                ALPHA) + " " + str(K))
            plt.savefig(folder + app + ".png", bbox_inches='tight', dpi=300)
            plt.close()

    workerLog = glob.glob(folder + "*worker*.out")
    sarLog = glob.glob(folder + "sar*.log")

    # Check len log
    print(len(workerLog), len(sarLog))

    if len(workerLog) == len(sarLog):
        for w, s in zip(sorted(workerLog), sorted(sarLog)):
            print(w)
            print(s)
            # 16/08/31 14:29:43 INFO Worker: Scaled executorId 2  of appId app-20160831142852-0000 to  8 Core
            # 16/09/11 17:37:18,062 INFO Worker: Created ControllerExecutor: 0 , 1 , 16000 , 30 , 6
            with open(w) as wlog:
                appID = ""
                plotDICT[w] = {}
                plotDICT[w]["cpu_real"] = []
                plotDICT[w]["time_cpu"] = []
                for line in wlog:
                    l = line.split(" ")
                    if len(l) > 3:
                        if l[4] == "Created" and appID != "":
                            plotDICT[appID][w]["cpu"].append(int(l[-1].replace("\n", "")))
                            plotDICT[appID][w]["sp_real"].append(0.0)
                            plotDICT[appID][w]["time"].append(
                                    datetime.strptime(l[1], STRPTIME_FORMAT).replace(year=2016))
                            plotDICT[appID][w]["sp"].append(0.0)
                        if l[4] == "Scaled":
                            # print(l)
                            if appID == "" or appID != l[10]:
                                nextAppID = l[10]
                                try:
                                    plotDICT[nextAppID][w] = {}
                                    plotDICT[nextAppID][w]["cpu"] = []
                                    plotDICT[nextAppID][w]["time"] = []
                                    plotDICT[nextAppID][w]["sp_real"] = []
                                    plotDICT[nextAppID][w]["sp"] = []
                                    appID = nextAppID
                                except KeyError:
                                    None
                                    # print(nextAppID + " NOT FOUND BEFORE IN BENCHMARK LOGS")
                        if appID != "":
                            if l[4] == "CoreToAllocate:":
                                # print(l)
                                plotDICT[appID][w]["cpu"].append(int(l[-1].replace("\n", "")))
                            if l[4] == "Real:":
                                plotDICT[appID][w]["sp_real"].append(l[-1].replace("\n", ""))
                            if l[4] == "SP":
                                plotDICT[appID][w]["time"].append(
                                    datetime.strptime(l[1], STRPTIME_FORMAT).replace(year=2016))
                                plotDICT[appID][w]["sp"].append(l[-1].replace("\n", ""))

            with open(s) as cpulog:
                for line in cpulog:
                    l = line.split("    ")
                    if not ("Linux" in l[0].split(" ") or "\n" in l[0].split(" ")) and l[1] != " CPU" and l[
                        0] != "Average:":
                        plotDICT[w]["time_cpu"].append(
                            datetime.strptime(l[0], '%I:%M:%S %p').replace(year=2016))
                        cpuint = '{0:.2f}'.format((float(l[2]) * COREHTVM) / 100)
                        plotDICT[w]["cpu_real"].append(cpuint)
    else:
        print("ERROR: SAR != WORKER LOGS")

    # print(plotDICT)
    for appID in sorted(plotDICT.keys()):
        if len(appID) <= len("app-20160831142852-0000"):
            for worker in plotDICT[appID].keys():
                colors_stage = {'m', 'y', 'k', 'c', 'g'}
                colors2_stage = colors_stage.copy()
                if worker not in ["startTimeStages", "dealineTimeStages", "finishTimeStages"] and len(
                        plotDICT[appID][worker]["sp"]) > 0:
                    # print(appID, worker)
                    fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)

                    sp_plt, = ax1.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["sp"], ".r-",
                                       label='SP')
                    sp_real_plt, = ax1.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["sp_real"], ".k-",
                                            label='SP REAL')
                    ax1.set_xlabel('time')
                    ax1.set_ylabel('sp')

                    for starttime, finishtime in zip(plotDICT[appID]["startTimeStages"],
                                                     plotDICT[appID]["finishTimeStages"]):
                        try:
                            c = colors_stage.pop()
                        except KeyError:
                            c = colors2_stage.pop()
                        ax1.axvline(starttime, color=c, linestyle='--')
                        ax1.axvline(finishtime, color=c)
                    for deadline in plotDICT[appID]["dealineTimeStages"]:
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
                    cpu_plt, = ax2.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["cpu"], ".b-",
                                        label='CPU')
                    if len(plotDICT[worker]["cpu_real"]) > 0:
                        b_d = plotDICT[appID][worker]["time"][0]

                        def func(x):
                            delta = x - b_d if x > b_d else timedelta.max
                            return delta

                        indexInit = plotDICT[worker]["time_cpu"].index(min(plotDICT[worker]["time_cpu"], key=func))
                        b_d = plotDICT[appID][worker]["time"][-1]
                        indexEnd = plotDICT[worker]["time_cpu"].index(min(plotDICT[worker]["time_cpu"], key=func))
                        print(indexInit, indexEnd)
                        cpu_real, = ax2.plot(plotDICT[worker]["time_cpu"][indexInit:indexEnd + 1],
                                             plotDICT[worker]["cpu_real"][indexInit:indexEnd + 1], ".g-",
                                             label='CPU REAL')
                        plt.legend(handles=[sp_plt, sp_real_plt, cpu_plt, cpu_real], bbox_to_anchor=(1.1, -0.025))
                    else:
                        plt.legend(handles=[sp_plt, sp_real_plt, cpu_plt], bbox_to_anchor=(1.1, -0.025))

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
                    plt.title(appID + " " + str(SCALE_FACTOR) + " " + str(DEADLINE) + " " + str(TSAMPLE) + " " + str(
                        ALPHA)+ " " + str(K))
                    import matplotlib.dates as mdate
                    locator = mdate.SecondLocator(interval=SECONDLOCATOR)
                    plt.gca().xaxis.set_major_locator(locator)

                    plt.gcf().autofmt_xdate()
                    plt.savefig(worker + "." + appID + '.png', bbox_inches='tight', dpi=300)
                    plt.close()
