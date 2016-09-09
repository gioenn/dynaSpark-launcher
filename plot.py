import matplotlib.pyplot as plt
import glob
from datetime import datetime
from datetime import timedelta
import numpy as np

from config import *


def plot(folder):
    # print("COREVM = " + str(COREVM))
    benchLog = glob.glob(folder + "*.err")
    plotDICT = {}
    for bench in sorted(benchLog):
        # 16/08/30 21:45:51 INFO ControllerJob: SEND INIT TO EXECUTOR CONTROLLER EID 0, SID 2, TASK 150, DL 81322, C 12
        # 16/08/30 21:46:13 INFO DAGScheduler: ResultStage 2 (count at KVDataTest.scala:151) finished in 22.195 s
        # 16/08/31 14:30:28 INFO ControllerJob: SEND NEEDED CORE TO MASTER spark://ec2-52-42-181-165.us-west-2.compute.amazonaws.com:7077, 0, Vector(4, 4, 4, 4), app-20160831142852-0000
        appID = ""
        with open(bench) as applog:
            SID = 0
            for line in applog:
                l = line.split(" ")
                if len(l) > 3 and l[3] == "ControllerJob:":
                    if l[5] == "INIT" and SID != int(l[12].replace(",", "")):
                        SID = int(l[12].replace(",", ""))
                        plotDICT[appID]["startTimeStages"].append(
                            datetime.strptime(l[1], '%H:%M:%S').replace(year=2016))
                        print("START: " + str(datetime.strptime(l[1], '%H:%M:%S').replace(year=2016)))
                        print(l[16].replace(",", ""))
                        plotDICT[appID]["dealineTimeStages"].append(plotDICT[appID]["startTimeStages"][-1] + timedelta(
                            milliseconds=float(l[16].replace(",", ""))))
                    if l[5] == "NEEDED" and l[4] == "SEND":
                        nextAppID = l[-1].replace("\n", "")
                        if appID != nextAppID:
                            appID = nextAppID
                            print(appID)
                            plotDICT[appID] = {}
                            plotDICT[appID]["dealineTimeStages"] = []
                            plotDICT[appID]["startTimeStages"] = []
                            plotDICT[appID]["finishTimeStages"] = []
                elif len(l) > 3 and l[3] == "DAGScheduler:":
                    if l[-4] == "finished":
                        if appID != "":
                            if len(plotDICT[appID]["startTimeStages"]) > len(plotDICT[appID]["finishTimeStages"]):
                                plotDICT[appID]["finishTimeStages"].append(
                                datetime.strptime(l[1], '%H:%M:%S').replace(
                                    year=2016))
                                print("END: " + str(datetime.strptime(l[1], '%H:%M:%S').replace(year=2016)))

                elif len(l) > 10 and l[5] == "added:" and l[4] == "Executor":
                    # 16/08/31 14:28:52 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20160831142852-0000/0 on worker-20160831142832-ec2-52-43-162-151.us-west-2.compute.amazonaws.com-9999 (ec2-52-43-162-151.us-west-2.compute.amazonaws.com:9999) with 8 cores
                    COREHTVM = int(l[-2])

                    # print(plotDICT.keys())
    workerLog = glob.glob(folder + "*worker*.out")
    sarLog = glob.glob(folder + "sar*.log")

    # Check len log
    print(len(workerLog), len(sarLog))

    if len(workerLog) == len(sarLog):
        for w, s in zip(sorted(workerLog), sorted(sarLog)):
            print(w)
            print(s)
            # 16/08/31 14:29:43 INFO Worker: Scaled executorId 2  of appId app-20160831142852-0000 to  8 Core
            with open(w) as wlog:
                appID = ""
                plotDICT[w] = {}
                plotDICT[w]["cpu_real"] = []
                plotDICT[w]["time_cpu"] = []
                for line in wlog:
                    l = line.split(" ")
                    if len(l) > 3:
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
                                plotDICT[appID][w]["time"].append(datetime.strptime(l[1], '%H:%M:%S').replace(year=2016))
                                plotDICT[appID][w]["sp"].append(l[-1].replace("\n", ""))

            with open(s) as cpulog:
                for line in cpulog:
                    l = line.split("    ")
                    if not ("Linux" in l[0].split(" ") or "\n" in l[0].split(" ")) and l[1] != " CPU" and l[
                        0] != "Average:":
                        plotDICT[w]["time_cpu"].append(
                            datetime.strptime(l[0], '%I:%M:%S %p').replace(year=2016))
                        cpuint = '{0:.2f}'.format(float(l[2]) * COREHTVM / 100)
                        plotDICT[w]["cpu_real"].append(cpuint)
    else:
        print("ERROR: SAR != WORKER LOGS")

    # print(plotDICT)
    for appID in sorted(plotDICT.keys()):
        if len(appID) <= len("app-20160831142852-0000"):
            for worker in plotDICT[appID].keys():
                colors_stage = {'m', 'y', 'k', 'c', 'g'}
                if worker not in ["startTimeStages", "dealineTimeStages", "finishTimeStages"] and len(
                        plotDICT[appID][worker]["sp"]) > 0:
                    # print(appID, worker)
                    fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)

                    sp_plt, = ax1.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["sp"], ".r-", label='SP')
                    sp_real_plt, = ax1.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["sp_real"],  ".k-",
                                            label='SP REAL')
                    ax1.set_xlabel('time')
                    ax1.set_ylabel('sp')

                    for starttime, finishtime in zip(plotDICT[appID]["startTimeStages"],
                                                     plotDICT[appID]["finishTimeStages"]):
                        c = colors_stage.pop()
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
                    cpu_plt, = ax2.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["cpu"],  ".b-", label='CPU')
                    if len(plotDICT[worker]["cpu_real"]) > 0:
                        indexInit = plotDICT[worker]["time_cpu"].index(plotDICT[appID][worker]["time"][0])
                        indexEnd = plotDICT[worker]["time_cpu"].index(plotDICT[appID][worker]["time"][-1])
                        cpu_real, = ax2.plot(plotDICT[worker]["time_cpu"][indexInit:indexEnd + 1],
                                             plotDICT[worker]["cpu_real"][indexInit:indexEnd + 1],  ".g-", label='CPU REAL')
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
                        SCALE_FACTOR))
                    plt.savefig(worker + "." + appID + '.png', bbox_inches='tight', dpi=300)
                    plt.close()
