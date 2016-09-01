import matplotlib.pyplot as plt
import glob
from datetime import datetime
from datetime import timedelta
import numpy as np

from config import *


def plot(folder):
    print("COREVM = " + str(COREVM))
    benchLog = glob.glob(folder + "*.err")
    plotDICT = {}
    for bench in benchLog:
        # 16/08/30 21:45:51 INFO ControllerJob: SEND INIT TO EXECUTOR CONTROLLER EID 0, SID 2, TASK 150, DL 81322, C 12
        # 16/08/30 21:46:13 INFO DAGScheduler: ResultStage 2 (count at KVDataTest.scala:151) finished in 22.195 s
        # 16/08/31 14:30:28 INFO ControllerJob: SEND NEEDED CORE TO MASTER spark://ec2-52-42-181-165.us-west-2.compute.amazonaws.com:7077, 0, Vector(4, 4, 4, 4), app-20160831142852-0000
        appID = ""
        with open(bench) as applog:
            for line in applog:
                l = line.split(" ")
                if len(l) > 3 and l[3] == "ControllerJob:":
                    if l[5] == "INIT":
                        plotDICT[appID]["startTime"] = datetime.strptime(l[1], '%H:%M:%S').replace(year=2016)
                        print(l[16].replace(",", ""))
                        plotDICT[appID]["dealineTime"] = plotDICT[appID]["startTime"] + timedelta(
                            milliseconds=float(l[16].replace(",", "")))
                    if l[5] == "NEEDED":
                        appID = l[-1].replace("\n", "")
                        print(appID)
                        plotDICT[appID] = {}
                elif len(l) > 3 and l[3] == "DAGScheduler:":
                    if l[4] == "ResultStage" and l[-4] == "finished":
                        if appID != "": plotDICT[appID]["finishTime"] = datetime.strptime(l[1], '%H:%M:%S').replace(
                            year=2016)

    print(plotDICT.keys())
    workerLog = glob.glob(folder + "*worker*.out")
    sarLog = glob.glob(folder + "sar*.log")

    # Check len log
    print(len(workerLog), len(sarLog))

    if len(workerLog) == len(sarLog):
        for w, s in zip(workerLog, sarLog):
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
                            print(l)
                            if appID == "" or appID != l[10] and l[10] != "app-20160831230103-0005":
                                appID = l[10]
                                plotDICT[appID][w] = {}
                                plotDICT[appID][w]["cpu"] = []
                                plotDICT[appID][w]["time"] = []
                                plotDICT[appID][w]["sp_real"] = []
                                plotDICT[appID][w]["sp"] = []
                        if l[4] == "CoreToAllocate:":
                            print(l)
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

    print(plotDICT)
    for appID in plotDICT.keys():
        if len(appID) <= len("app-20160831142852-0000"):
            for worker in plotDICT[appID].keys():
                if worker not in ["startTime", "dealineTime", "finishTime"] and len(plotDICT[appID][worker]["sp"]) > 0:
                    print(appID, worker)
                    fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)

                    sp_plt, = ax1.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["sp"], label='SP',
                                       color="red")
                    sp_real_plt, = ax1.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["sp_real"],
                                            label='SP REAL', color="black")
                    ax1.set_xlabel('time')
                    ax1.set_ylabel('sp')

                    ax1.axvline(plotDICT[appID]["startTime"], color="k", linestyle='--')
                    ax1.axvline(plotDICT[appID]["dealineTime"], color="red", linestyle='--')
                    ax1.axvline(plotDICT[appID]["finishTime"], color="k", linestyle='--')

                    ax1.spines["top"].set_visible(False)
                    ax1.spines["right"].set_visible(False)

                    ax1.get_xaxis().tick_bottom()
                    ax1.get_yaxis().tick_left()

                    xlim = ax1.get_xlim()
                    factor = 0.1
                    new_xlim = (xlim[0] + xlim[1]) / 2 + np.array((-0.5, 0.5)) * (xlim[1] - xlim[0]) * (1 + factor)
                    ax1.set_xlim(new_xlim)

                    ax2 = ax1.twinx()
                    cpu_plt, = ax2.plot(plotDICT[appID][worker]["time"], plotDICT[appID][worker]["cpu"], label='CPU')
                    if len(plotDICT[worker]["cpu_real"]) > 0:
                        indexInit = plotDICT[worker]["time_cpu"].index(plotDICT[appID][worker]["time"][0])
                        indexEnd = plotDICT[worker]["time_cpu"].index(plotDICT[appID][worker]["time"][-1])
                        cpu_real, = ax2.plot(plotDICT[worker]["time_cpu"][indexInit:indexEnd + 1],
                                             plotDICT[worker]["cpu_real"][indexInit:indexEnd + 1], label='CPU REAL')
                        plt.legend(handles=[sp_plt, sp_real_plt, cpu_plt, cpu_real])
                    else:
                        plt.legend(handles=[sp_plt, sp_real_plt, cpu_plt])
                    ax2.set_ylabel('cpu')
                    ax2.set_ylim(0, COREVM)
                    labels = ax1.get_xticklabels()
                    plt.setp(labels, rotation=90, fontsize=10)
                    xlim = ax2.get_xlim()
                    # example of how to zoomout by a factor of 0.1
                    factor = 0.1
                    new_xlim = (xlim[0] + xlim[1]) / 2 + np.array((-0.5, 0.5)) * (xlim[1] - xlim[0]) * (1 + factor)
                    ax2.set_xlim(new_xlim)

                    plt.savefig(worker + "." + appID + '.png', bbox_inches='tight', dpi=300)
