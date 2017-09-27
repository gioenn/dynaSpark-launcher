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
import os
import shutil
from datetime import datetime as dt, timedelta
from pathlib import Path
from sys import platform

import matplotlib
import matplotlib.pyplot as plt
plt.switch_backend('agg')      # Added to avoid RuntimeError('Invalid DISPLAY variable') when run on linux server
import matplotlib.ticker as plt_ticker

from log import load_app_data, load_worker_data
from util.utils import timing

Y_TICK_AGG = 40
Y_TICK_SORT = 40
Y_LABEL = 'Core [#]'
Y1_LABEL = 'App Progress [%]'
Y2_LABEL = 'Stage Progress [%]'
X_LABEL = 'Time [s]'
TITLE = False
PLOT_SID_STAGE = 0
PLOT_LABEL = True
LABEL_SIZE = 20
TQ_MICRO = 20
TQ_KMEANS = 9
PDF = 0

PLOT_PARAMETERS = {
    'axes.labelsize': LABEL_SIZE,  # fontsize for x and y labels (was 10)
    'axes.titlesize': 8,
    'font.size': LABEL_SIZE,  # was 10
    'legend.fontsize': 8,  # was 10
    'xtick.labelsize': LABEL_SIZE,
    'ytick.labelsize': LABEL_SIZE,
}

matplotlib.rcParams.update(PLOT_PARAMETERS)

m_script_content = []                   # Matlab script holder
charts = {}                             # JSON dict Charts holder
charts['Charts'] = []                   # JSON Charts
#json_charts_folder = ""                 # JSON Charts
json_charts_filename = "charts.json"    # JSON Charts

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
            from config import CONFIG_DICT
            return CONFIG_DICT
        else:
            return config
    else:
        from config import CONFIG_DICT
        return CONFIG_DICT


def text_plotter_x(x_data, axis, labels):
    """
    Plot all the label on the axis and adjust y to avoid label overlapping

    :param x_data:  the list of x point
    :param axis:  the axis on which plot the label text
    :param labels: the list of label to plot
    :return: dict of Labels, corresponding Y Points and Label Font Size - introduced with JSON Charts
    """
    # label_dict = { "Label" : [], "LabelX" : [], "LabelY" : [], "LabelSize" : LABEL_SIZE - 8} # JSON Charts
    label_dict = {} # JSON Charts
    for i, (x_point, label) in enumerate(zip(x_data, labels)):
        if len(x_data) > 3:
            y_point = 102 if (i % 2) == 1 else 110
        else:
            y_point = 102
        axis.text(x_point, y_point, label, rotation=0, horizontalalignment='center',
                  size=LABEL_SIZE)
        y_point = 108 if y_point == 110 else 102 # Matlab - avoid label crop with default view settings
        m_script_content.append("text("+str(x_point)+", "+str(y_point)+", '"+label+"','FontSize', "+str(LABEL_SIZE - 8)+\
                                        " ,'Rotation', 0, 'HorizontalAlignment', 'center')") # Matlab - reduce fontsize to avoid label crop
        label_dict[label] = {}          # JSON Charts
        label_dict[label]["X"] = x_point   # JSON Charts
        label_dict[label]["Y"] = y_point   # JSON Charts
    return label_dict

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
    m_script_name = [] # Matlab script filename holder
    # m_script_content = [] # Matlab script file holder
    chart = {}                                          # JSON Charts
    with open('chart_templates.json', 'r') as file:     # JSON Charts
        chart = json.load(file)['Chart']                # JSON Charts
    m_script_content.append("fig=figure;")              # Matlab
    m_script_content.append("hax=axes;")                # Matlab
    m_script_content.append("set(hax,'YLim',[0 100]);") # Matlab
    chart["YAxis"]["L"]["Limits"]["Lower"] = 0          # JSON Charts
    chart["YAxis"]["L"]["Limits"]["Upper"] = 100        # JSON Charts
    m_script_content.append("hold on;")                 # Matlab
    m_script_content.append("Grey=[0.5 0.5 0.5];")      # Matlab
    m_script_content.append("CornflowerBlue=[0.3906    0.5820    0.9258];") # Matlab
    m_script_content.append("LightSteelBlue=[0.6875    0.7656    0.8672];") # Matlab
    xy1_dat_name = [] # Matlab x1-y1 axis data filename holder
    xy1_dat_content = [] # Matlab x1-y1 axis data holder
    xy2_dat_name = [] # Matlab x2-y2 axis data filename holder
    xy2_dat_content = [] # Matlab x2-y2 axis data holder
    if config["HDFS"]:
        sorted_sid.remove(0)
    for sid in sorted_sid:
        try:
            start_ts = app_info[app_id][sid]["start"].timestamp() - first_ts_worker
            #start_ts_acc += start_ts                                            # JSON Charts
            # ax1.axvline(start_ts, ymin=0.0, ymax=1.1, color="green", zorder=100)
            # try:
            #     label_to_plot[tq_ts].append("S" + str(sid))
            # except KeyError:
            #     label_to_plot[tq_ts] = []
            #     label_to_plot[tq_ts].append("S" + str(sid))
            # ax1.text(start_ts, 105, "S" + str(sid), style="italic", weight="bold",
            # horizontalalignment='center')
            end_ts = app_info[app_id][sid]["end"].timestamp() - first_ts_worker
            #end_ts_acc += end_ts                                                # JSON Charts
            ax1.axvline(end_ts, color="green")
            # m_script_content.append("hold on;") # Matlab
            m_script_content.append("vl_end=line(["+str(end_ts)+" "+str(end_ts)+"], get(hax,'YLim'), 'color', 'green');") # Matlab
            label_to_plot.append("E" + str(sid))
            #end_label_acc.append("E" + str(sid))                                    # JSON Charts
            ts_to_plot.append(end_ts)
            chart["Stages"][sid] = {}                                               # JSON Charts
            chart["Stages"][sid]["StartTimestamp"] = start_ts                       # JSON Charts
            chart["Stages"][sid]["EndTimestamp"] = end_ts                           # JSON Charts
            chart["Stages"][sid]["EndLabel"] = "E" + str(sid)                       # JSON Charts
            chart["Stages"][sid]["StartLabel"] = "S" + str(sid)                     # JSON Charts
            chart["Stages"][sid]["VLines"] = {}                                     # JSON Charts
            chart["Stages"][sid]["VLines"]["S" + str(sid)] = {}                     # JSON Charts
            chart["Stages"][sid]["VLines"]["S" + str(sid)]["X"] = start_ts            # JSON Charts
            chart["Stages"][sid]["VLines"]["S" + str(sid)]["Type"] = "Start"          # JSON Charts
            chart["Stages"][sid]["VLines"]["S" + str(sid)]["Color"] = "blue"       # JSON Charts
            chart["Stages"][sid]["VLines"]["S" + str(sid)]["Line"] = "-"            # JSON Charts
            chart["Stages"][sid]["Labels"] = {}                                     # JSON Charts
            chart["Stages"][sid]["Labels"]["S" + str(sid)] = {}                     # JSON Charts
            chart["Stages"][sid]["Labels"]["S" + str(sid)]["X"] = start_ts            # JSON Charts
            chart["Stages"][sid]["Labels"]["S" + str(sid)]["Y"] = 105             # JSON Charts
            chart["Stages"][sid]["VLines"]["E" + str(sid)] = {}                     # JSON Charts
            chart["Stages"][sid]["VLines"]["E" + str(sid)]["X"] = end_ts            # JSON Charts
            chart["Stages"][sid]["VLines"]["E" + str(sid)]["Type"] = "End"          # JSON Charts
            chart["Stages"][sid]["VLines"]["E" + str(sid)]["Color"] = "green"       # JSON Charts
            chart["Stages"][sid]["VLines"]["E" + str(sid)]["Line"] = "-"            # JSON Charts
            chart["Stages"][sid]["Labels"]["E" + str(sid)] = {}                     # JSON Charts
            chart["Stages"][sid]["Labels"]["E" + str(sid)]["X"] = end_ts            # JSON Charts
            chart["Stages"][sid]["Labels"]["E" + str(sid)]["Y"] = 102             # JSON Charts
            chart["VLines"]["S" + str(sid)] = chart["Stages"][sid]["VLines"]["S" + str(sid)]     # JSON Charts
            chart["VLines"]["E" + str(sid)] = chart["Stages"][sid]["VLines"]["E" + str(sid)]     # JSON Charts
            chart["Labels"]["S" + str(sid)] = {}                                                 # JSON Charts
            chart["Labels"]["S" + str(sid)] = chart["Stages"][sid]["Labels"]["S" + str(sid)]     # JSON Charts
            chart["Labels"]["E" + str(sid)] = {}                                                 # JSON Charts
            chart["Labels"]["E" + str(sid)] = chart["Stages"][sid]["Labels"]["E" + str(sid)]     # JSON Charts
            # chart["VLines"].append({"Label" : "E" + str(sid), "X" : end_ts, "Color" : "green",  "Line" : "-"})       # JSON Charts
            # m_script_content.append("text(["+str(end_ts)+" "+str(end_ts)+"], get(hax,'YLim'), "+"\'E" + str(sid)+"\');") # Matlab
            # ax1.text(end_ts, -0.035, "E"+str(sid), style="italic", weight="bold",
            # horizontalalignment='center')
            dead_ts = app_info[app_id][sid]["deadline"].timestamp() - first_ts_worker
            #dead_ts_acc += dead_ts
            #dead_label_acc.append("D" + str(sid))                                   # JSON Charts
            ax1.axvline(dead_ts, color="red", linestyle='--')
            m_script_content.append("vl_dead=line(["+str(dead_ts)+" "+str(dead_ts)+"], get(hax,'YLim'), 'color', 'red', 'LineStyle', '--');") # Matlabchart["Stages"][sid]["StartTimestamp"] = start_ts                       # JSON Charts
            chart["Stages"][sid]["DeadlineTimestamp"] = dead_ts                     # JSON Charts
            chart["Stages"][sid]["DeadlineLabel"] = "D" + str(sid)                  # JSON Charts
            chart["Stages"][sid]["VLines"]["D" + str(sid)] = {}                     # JSON Charts
            chart["Stages"][sid]["VLines"]["D" + str(sid)]["X"] = dead_ts            # JSON Charts
            chart["Stages"][sid]["VLines"]["D" + str(sid)]["Type"] = "Deadline"     # JSON Charts
            chart["Stages"][sid]["VLines"]["D" + str(sid)]["Color"] = "red"         # JSON Charts
            chart["Stages"][sid]["VLines"]["D" + str(sid)]["Line"] = "--"           # JSON Charts
            chart["Stages"][sid]["Labels"]["D" + str(sid)] = {}                     # JSON Charts
            chart["Stages"][sid]["Labels"]["D" + str(sid)]["X"] = dead_ts           # JSON Charts
            chart["Stages"][sid]["Labels"]["D" + str(sid)]["Y"] = 108               # JSON Charts 
            chart["VLines"]["D" + str(sid)] = chart["Stages"][sid]["VLines"]["D" + str(sid)]     # JSON Charts
            chart["Labels"]["D" + str(sid)] = {}                                                 # JSON Charts
            chart["Labels"]["D" + str(sid)] = chart["Stages"][sid]["Labels"]["D" + str(sid)]     # JSON Charts           
            # chart["VLines"].append({"X" : dead_ts, "Color" : "red",  "Line" : "--"})       # JSON Charts
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
            sp_real = [y * 100 for y in worker_dict[app_id][sid]["sp_real"]]
            set_points_progress = [x * 100 for x in worker_dict[app_id][sid]["sp"]]
            if set_points_progress[-1] < 100.0:
                time_sp.append(dead_ts)
                set_points_progress.append(100.0)
            if sp_real[-1] < 100.0:
                next_time = time_sp_real[-1] + (int(config["Control"]["TSample"]) / 1000)
                if next_time <= end_ts:
                    time_sp_real.append(next_time)
                    sp_real.append(100.0)
                else:
                    time_sp_real.append(end_ts)
                    sp_real.append(100.0)
            ax1.plot(time_sp, set_points_progress, "gray", label='PROGRESS', linewidth=2)
            #time_sp_acc += time_sp                                              # JSON Charts
            #set_points_progress_acc += set_points_progress                      # JSON Charts
            time_sp_csv = " ".join(map(str, time_sp))                           #Matlab
            set_points_progress_csv = " ".join(map(str, set_points_progress))   #Matlab
            m_script_content.append("plot(["+time_sp_csv+"], ["+set_points_progress_csv+"], 'color', Grey, 'LineWidth', 2);") #Matlab - no line label added
            # time_sp_json = ",".join(map(str, time_sp))        #Matlab
            # set_points_progress_json = ",".join(map(str, set_points_progress))       #Matlab
            chart["Stages"][sid]["Timestamps"] =  time_sp                       # JSON Charts
            chart["Stages"][sid]["Progress"] =  set_points_progress             # JSON Charts
            ax1.plot(time_sp_real, sp_real, "black", label='PROGRESS REAL', linewidth=2)
            #time_sp_real_acc += time_sp_real                                    # JSON Charts
            #sp_real_acc += sp_real                                              # JSON Charts
            time_sp_real_csv = " ".join(map(str, time_sp_real)) #Matlab
            sp_real_csv = " ".join(map(str, sp_real)) #Matlab
            m_script_content.append("plot(["+time_sp_real_csv+"], ["+sp_real_csv+"], 'color', 'black', 'LineWidth', 2);") #Matlab - no line label added
            # time_sp_real_json = ",".join(map(str, time_sp_real))   #Matlab
            # sp_real_json = ",".join(map(str, sp_real))             #Matlab
            chart["Stages"][sid]["TimestampsReal"] = time_sp_real               # JSON Charts
            chart["Stages"][sid]["ProgressReal"] = sp_real                      # JSON Charts
            chart["Timestamps"] += time_sp                                      # JSON Charts
            chart["TimestampsReal"] += time_sp_real                             # JSON Charts
            chart["Progress"] += set_points_progress                            # JSON Charts
            chart["ProgressReal"] += sp_real                                    # JSON Charts
            chart["StartTimestamps"] += [start_ts]                                # JSON Charts
            chart["EndTimestamps"] += [end_ts]                                    # JSON Charts
            chart["DeadlineTimestamps"] += [dead_ts]                              # JSON Charts
            chart["StartLabels"] += ["S" + str(sid)]                                 # JSON Charts
            chart["EndLabels"] += ["E" + str(sid)]                                # JSON Charts
            chart["DeadlineLabels"] += ["D" + str(sid)]                               # JSON Charts 
        except KeyError as error:
            print("SID " + str(sid) + " not executed by " + worker_log)           

    if PLOT_LABEL:
        text_plotter_x(ts_to_plot, ax1, label_to_plot)
        #chart["Labels"] = text_plotter_x(ts_to_plot, ax1, label_to_plot)
    ax1.set_xlabel(X_LABEL)
    m_script_content.append("xlabel('"+X_LABEL+"');") #Matlab
    chart["XAxis"]["B"]["Label"] = X_LABEL          # JSON Charts
    ax1.set_ylabel(Y2_LABEL)
    m_script_content.append("yyaxis left") #Matlab
    m_script_content.append("ylabel('"+Y2_LABEL+"');") #Matlab
    chart["YAxis"]["L"]["Label"] = Y2_LABEL          # JSON Charts
    ax1.spines["top"].set_visible(False)
     #Matlab - apparently no spines by default 
    ax1.spines["right"].set_visible(False)
     #Matlab - apparently no spines by default 
    ax1.get_xaxis().tick_bottom()
    m_script_content.append("XTicksVisible = true;") #Matlab
    ax1.get_yaxis().tick_left()
    m_script_content.append("YTicksVisible = true;") #Matlab
    xlim = ax1.get_xlim()
    ax1.set_xlim(0.0, xlim[1])
    #Matlab - should be already set. however set it to override default anyway
    m_script_content.append("xl=get(hax,'XLim');") #Matlab
    m_script_content.append("xmin=xl(1,1);") #Matlab
    m_script_content.append("xmax=xl(1,2);") #Matlab
    m_script_content.append("set(hax,'XLim',[0 xmax]);") #Matlab
    chart["XAxis"]["B"]["Limits"]["Lower"] = 0          # JSON Charts
    chart["XAxis"]["B"]["Limits"]["Upper"] = xlim[1]    # JSON Charts
    ax1.set_ylim(0.0, 100.0)
    #Matlab - should be already set. however set it to override default anyway
    m_script_content.append("set(hax,'YLim',[0 100]);") # Matlab
    chart["YAxis"]["L"]["Limits"]["Lower"] = 0          # JSON Charts
    chart["YAxis"]["L"]["Limits"]["Upper"] = 100        # JSON Charts
    folder_split = worker_log.split("/")
    name = folder_split[-3].lower() + "-worker-" + folder_split[-2].replace("%", "") + "-" + \
           folder_split[-1].split("-")[-1].replace(".out", "")
    if "agg" in name:
        ax1.xaxis.set_major_locator(plt_ticker.MultipleLocator(base=Y_TICK_AGG))
        m_script_content.append("XTick = 0:"+Y_TICK_AGG+":xmax;") #Matlab
        m_script_content.append("xticks(XTick);") #Matlab
        chart["XAxis"]["B"]["TickBase"] = Y_TICK_AGG          # JSON Charts
    elif "sort" in name:
        ax1.xaxis.set_major_locator(plt_ticker.MultipleLocator(base=Y_TICK_SORT))
        m_script_content.append("XTick = 0:"+Y_TICK_SORT+":xmax;") #Matlab
        m_script_content.append("xticks(XTick);") #Matlab
        chart["XAxis"]["B"]["TickBase"] = Y_TICK_SORT        # JSON Charts
    ax1.yaxis.set_major_locator(plt_ticker.MultipleLocator(base=10.0))
    m_script_content.append("YTick = 0:10:100;") #Matlab
    m_script_content.append("yticks(YTick);") #Matlab
    chart["YAxis"]["L"]["TickBase"] = 10.0        # JSON Charts
    ax2 = ax1.twinx()
    m_script_content.append("yyaxis right;") #Matlab
    for sid in sorted_sid:
        try:
            times_stage = []                # JSON Charts
            cpus_stage = []                 # JSON Charts
            time = [t.timestamp() - first_ts_worker for t in worker_dict[app_id][sid]["time"]]
            times += time
            times_stage += time             # JSON Charts
            cpus += worker_dict[app_id][sid]["cpu"]
            cpus_stage += worker_dict[app_id][sid]["cpu"]                 # JSON Charts
            start_ts = app_info[app_id][sid]["start"].timestamp() - first_ts_worker
            times.insert(0, start_ts)
            times_stage.insert(0, start_ts) # JSON Charts
            cpus.insert(0, 0.01)
            cpus_stage.insert(0, 0.01)      # JSON Charts
            end_ts = app_info[app_id][sid]["end"].timestamp() - first_ts_worker
            dead_ts = app_info[app_id][sid]["deadline"].timestamp() - first_ts_worker
            next_time = time[-1] + (int(config["Control"]["TSample"]) / 1000)
            index_next = min(sorted(app_info[app_id]).index(sid) + 1, len(app_info[app_id]) - 1)
            next_start_ts = app_info[app_id][sorted(app_info[app_id])[index_next]][
                                "start"].timestamp() - first_ts_worker
            if end_ts < dead_ts and end_ts < next_time:
                times.append(end_ts - 0.01)
                times_stage.append(end_ts - 0.01)       # JSON Charts
                cpus.append(cpus[-1])
                cpus_stage.append(cpus[-1])             # JSON Charts
                if end_ts + 0.01 < next_start_ts:
                    times.append(end_ts + 0.01)
                    times_stage.append(end_ts + 0.01)   # JSON Charts
                else:
                    times.append(end_ts)
                    times_stage.append(end_ts)          # JSON Charts
                cpus.append(0.01)
                cpus_stage.append(0.01)                 # JSON Charts
            elif next_time <= end_ts:
                times.append(next_time - 0.02)
                times_stage.append(next_time - 0.02)    # JSON Charts
                cpus.append(cpus[-1])
                cpus_stage.append(cpus[-1])             # JSON Charts
                times.append(next_time - 0.01)
                times_stage.append(next_time - 0.01)    # JSON Charts
                cpus.append(0.01)
                cpus_stage.append(0.01)                 # JSON Charts
                if end_ts + 0.01 < next_start_ts:
                    times.append(end_ts + 0.01)
                    times_stage.append(end_ts + 0.01)   # JSON Charts
                    cpus.append(0.01)
                    cpus_stage.append(0.01)             # JSON Charts
                else:
                    times.append(end_ts)
                    times_stage.append(end_ts)          # JSON Charts
                    cpus.append(0.01)
                    cpus_stage.append(0.01)             # JSON Charts
                    # index_next = min(sorted(app_info[app_id]).index(sid) + 1, len(app_info[app_id]) - 1)
                    # times.append(
                    #     app_info[app_id][sorted(app_info[app_id])[index_next]][
                    #         "start"].timestamp() - first_ts_worker)
                    # cpus.append(0.0)
                # start_index = worker_dict["time_cpu"].index(worker_dict[app_id][sid]["time"][0])
                # end_index = worker_dict["time_cpu"].index(worker_dict[app_id][sid]["time"][-1])
                #
                # time_cpu = [t.timestamp() - first_ts_worker for t in worker_dict["time_cpu"]
                # [start_index:end_index]]
                # ax2.plot(time_cpu,
                #          worker_dict["cpu_real"][start_index:end_index], ".g-",
                #          label='CPU REAL')
            times_stage, cpus_stage = (list(t) for t in zip(*sorted(zip(times_stage, cpus_stage)))) # JSON Charts
            #times_acc += times_stage                                # JSON Charts
            #cpus_acc += cpus_stage                                  # JSON Charts
            chart["Stages"][sid]["TimestampsCpu"] = times_stage      # JSON Charts
            chart["Stages"][sid]["Cpu"] = cpus_stage                 # JSON Charts
            
        except KeyError:
            print("SID " + str(sid) + " not executed by " + worker_log)

    times, cpus = (list(t) for t in zip(*sorted(zip(times, cpus))))
    # [print(t, c) for t, c in zip(times, cpus)]
    ax2.plot(times, cpus, ".b-", label='CPU')
    times_csv = " ".join(map(str, times)) #Matlab
    cpus_csv = " ".join(map(str, cpus))   #Matlab
    m_script_content.append("cpuline=plot(["+times_csv+"], ["+cpus_csv+"], '.-', 'color', 'blue', 'LineWidth', 2, 'MarkerEdgeColor', 'blue', 'MarkerFaceColor', 'blue','MarkerSize', 12);") #Matlab
    chart["TimestampsCpu"] += times                         # JSON Charts
    chart["Cpu"] += cpus                                    # JSON Charts
    # times_json = ",".join(map(str, times)) #Matlab
    # cpus_json = ",".join(map(str, cpus))   #Matlab
    
    ax2.fill_between(times, 0.0, cpus, facecolor="b", alpha=0.2)
    m_script_content.append("xd=get(cpuline,'xdata');") #Matlab
    m_script_content.append("yd=get(cpuline,'ydata');") #Matlab
    m_script_content.append("a=area(xd, yd);") #Matlab
    m_script_content.append("set(a, 'FaceAlpha',0.5,'FaceColor', LightSteelBlue);") #Matlab
        
    # handles_ax1, labels_ax1 = ax1.get_legend_handles_labels()
    # handles_ax2, labels_ax2 = ax2.get_legend_handles_labels()
    # handles = handles_ax1[:2] + handles_ax2[:2]
    # labels = labels_ax1[:2] + labels_ax2[:2]
    # plt.legend(handles, labels, loc='best', prop={'size': 6})
    ax2.set_ylabel(Y_LABEL)
    m_script_content.append("set(hax.YAxis, 'color', 'black');")        #Matlab
    m_script_content.append("ylabel('"+Y_LABEL+"', 'color', 'black');") #Matlab
    chart["YAxis"]["R"]["Label"] = Y_LABEL                              # JSON Charts
    ax2.set_ylim(0, config["Control"]["CoreVM"])
    ymc = config["Control"]["CoreVM"]                                   #Matlab - # of VM cores
    m_script_content.append("set(hax,'YLim',[0 "+str(ymc)+"]);")        #Matlab
    chart["YAxis"]["R"]["Limits"]["Lower"] = 0          # JSON Charts
    chart["YAxis"]["R"]["Limits"]["Upper"] = ymc        # JSON Charts
    xlim = ax2.get_xlim()
    ax2.set_xlim(0.0, xlim[1])
    m_script_content.append("xl=get(hax,'XLim');") #Matlab
    m_script_content.append("xmin=xl(1,1);") #Matlab
    m_script_content.append("xmax=xl(1,2);") #Matlab
    m_script_content.append("set(hax,'XLim',[0 xmax]);") #Matlab
    chart["XAxis"]["B"]["Limits"]["Lower"] = 0          # JSON Charts
    chart["XAxis"]["B"]["Limits"]["Upper"] = xlim[1]    # JSON Charts
    # labels = ax1.get_xticklabels()
    # plt.setp(labels, rotation=90, fontsize=10)

    ax2.yaxis.set_major_locator(plt_ticker.MultipleLocator(base=1.0))
    m_script_content.append("YTick = 0:1.0:"+str(ymc)+";")  #Matlab
    m_script_content.append("yticks(YTick);")               #Matlab
    chart["YAxis"]["R"]["TickBase"] = 1.0                   # JSON Charts
    # locator = mpdate.SecondLocator(interval=SECONDLOCATOR)
    # plt.gca().xaxis.set_major_locator(locator)
    # plt.gca().xaxis.set_major_formatter(mpdate.DateFormatter(STRPTIME_FORMAT))
    # plt.gcf().autofmt_xdate()
    if TITLE:
        title = app_id + " " + str(config["Deadline"]) + " " + str(config["Control"]["TSample"]) + \
                " " + str(config["Control"]["Alpha"]) + " " + str(config["Control"]["K"])
        plt.title(title)
        m_script_content.append("title(hax, '"+title+"');") #Matlab
        chart["Title"] = title                              # JSON Charts
    ax1.set_zorder(ax2.get_zorder() + 1)
    # Layer = 'top|bottom'  #Matlab - limited availability to patch vs. axis z-order?
    ax1.patch.set_visible(False)
    # tbd equivalent Matlab code #Matlab 
    folder_split = worker_log.split("/")
    #name = folder_split[-3].lower() + "-worker-" + folder_split[-2].replace("%", "") + "-" + \
    #       folder_split[-1].split("-")[-1].replace(".out", "")
    delimiter = "/"
    if platform == "win32":
        name = folder_split[-3].lower() + "-worker-" + folder_split[-2].replace("%", "") + "-" + \
                folder_split[-1].split("-")[-1].replace(".out", "")
        delimiter = "\\"
    else:
        name = folder_split[-4].lower() + "-worker-" + folder_split[-3].replace("%", "") + "-" + \
                folder_split[-1].split("-")[-1].replace(".out", "")
    folder = "/".join(worker_log.split(delimiter)[:-1])
    labels = ax1.get_xticklabels()
    plt.setp(labels, rotation=45)
    m_script_content.append("set(hax.XAxis, 'TickLabelRotation', 45)")  #Matlab
    chart["Name"] = name                                                # JSON Charts
    charts['Charts'].append(chart)                                      # JSON Charts
    
    print(folder)
    if PDF:
        plt.savefig(folder + "/" + name + ".pdf", bbox_inches='tight', dpi=300)
        suffix="_pdf" #Matlab
    else:
        plt.savefig(folder + "/" + name + '.png', bbox_inches='tight', dpi=300)
        suffix="_png" #Matlab
    plt.close()
    m_script_name = folder + "/" + name.replace("-", "_") + suffix + ".m"  #Matlab doesn't like dashes in filenames
    #json_charts_folder = folder + "/" 
    with open(m_script_name, 'w') as file_handler:
        for line in m_script_content:
            file_handler.write("{}\n".format(line))
            # print("{}\n".format(line))
    m_script_content[:] = []

def plot_app_overview(app_id, app_dict, folder, config):
    """
    Plot only the application overview without the cpu data from the workers

    :param app_id: the id of the application
    :param app_dict: the data dictionary of the application
    :param folder: the folder where to save the output image
    :param config: the config dict of the application
    :return: nothing; save the figure in the output folder
    """
    m_script_name = [] # Matlab script filename holder
    # m_script_content = [] # Matlab script file holder
    m_script_content.append("fig=figure;") # Matlab
    m_script_content.append("hax=axes;") # Matlab
    # m_script_content.append("set(hax,'YLim',[0 100]);") # Matlab
    m_script_content.append("hold on;") # Matlab
    xy1_dat_name = [] # Matlab x1-y1 axis data filename holder
    xy1_dat_content = [] # Matlab x1-y1 axis data holder
    xy2_dat_name = [] # Matlab x2-y2 axis data filename holder
    xy2_dat_content = [] # Matlab x2-y2 axis data holder
    
    print("Plotting APP Overview")
    if len(app_dict) > 0:
        timestamps = []
        times = []
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
            except KeyError as error:
                print(error)

        fig, ax1 = plt.subplots(figsize=(16, 9), dpi=300)
        # PLOT NORMALIZED TASK PROGRESS
        min_times = min(times)
        max_times = max(times)
        normalized = [(z - min_times) / (max_times - min_times) * 100 for z in times]
        ax1.plot(timestamps, normalized, ".k-")
        timestamps_csv = " ".join(map(str, timestamps)) #Matlab
        normalized_csv = " ".join(map(str, normalized)) #Matlab
        m_script_content.append("plot(["+timestamps_csv+"], ["+normalized_csv+"], 'color', 'black');") #Matlab
        ymin, ymax = ax1.get_ylim()
        m_script_content.append("yl=get(hax,'YLim');") #Matlab
        m_script_content.append("ymin=yl(1,1);") #Matlab
        m_script_content.append("ymax=yl(1,2);") #Matlab
        # PLOT DEADLINE
        app_dead_ts = app_deadline.timestamp() - first_ts
        ax1.axvline(app_dead_ts)
        m_script_content.append("vl_dead=line(["+str(app_dead_ts)+" "+str(app_dead_ts)+"], get(hax,'YLim'), 'color', 'black');") # Matlab
        ax1.text(app_dead_ts, ymax + 0.5, 'D', weight="bold", horizontalalignment='center')
        m_script_content.append("text("+str(app_dead_ts)+", ymax + 0.5, 'D','FontSize', "+str(LABEL_SIZE - 8)+\
                                         " ,'FontWeight', 'bold','Rotation', 0, 'HorizontalAlignment', 'center')") # Matlab - reduce fontsize to avoid label crop
        # PLOT ALPHA DEADLINE
        app_alpha_deadline = app_deadline - timedelta(
            milliseconds=((1 - float(config["Control"]["Alpha"])) * float(config["Deadline"])))
        app_alpha_deadline_ts = app_alpha_deadline.timestamp() - first_ts
        ax1.axvline(app_alpha_deadline_ts)
        m_script_content.append("vl_dead=line(["+str(app_alpha_deadline_ts)+" "+str(app_alpha_deadline_ts)+"], get(hax,'YLim'), 'color', 'black', 'LineStyle', '--');") # Matlab
        ax1.text(app_alpha_deadline_ts, ymax + 0.5, 'AD', weight="bold",
                 horizontalalignment='center')
        m_script_content.append("text("+str(app_alpha_deadline_ts)+", ymax + 0.5, 'AD','FontSize', "+str(LABEL_SIZE - 8)+\
                                         " ,'FontWeight', 'bold','Rotation', 0, 'HorizontalAlignment', 'center')") # Matlab - reduce fontsize to avoid label crop
        ax1.set_xlabel(X_LABEL)
        m_script_content.append("xlabel('"+X_LABEL+"');") #Matlab
        ax1.set_ylabel(Y1_LABEL)
        m_script_content.append("yyaxis left") #Matlab
        m_script_content.append("ylabel('"+Y1_LABEL+"');") #Matlab

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
                ax1.axvline(end_ts, color="green")
                m_script_content.append("vl_dead=line(["+str(end_ts)+" "+str(end_ts)+"], get(hax,'YLim'), 'color', 'green');") # Matlab
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
                m_script_content.append("vl_dead=line(["+str(dead_ts)+" "+str(dead_ts)+"], get(hax,'YLim'), 'color', 'red', 'LineStyle', '--');") # Matlab
            except KeyError as error:
                print(error)

        if TITLE:
            plt.title(app_id + " " + str(config["Deadline"]) + " " +
                      str(config["Control"]["TSample"]) + " " +
                      str(config["Control"]["Alpha"]) + " " + str(config["Control"]["K"]))
            title = app_id + " " + str(config["Deadline"]) + " " + \
                    str(config["Control"]["TSample"]) + " " + \
                    str(config["Control"]["Alpha"]) + " " + str(config["Control"]["K"])
            m_script_content.append("title(hax, '"+title+"');") #Matlab
        folder_split = folder.split("/")
        name = folder_split[-4].lower() + "-overview-" + folder_split[-3].replace("%", "")
        labels = ax1.get_xticklabels()
        plt.setp(labels, rotation=45)
        m_script_content.append("set(hax.XAxis, 'TickLabelRotation', 45)") #Matlab
        if PDF:
            plt.savefig(folder + name + ".png", bbox_inches='tight', dpi=300)
            suffix="_pdf" #Matlab
        else:
            plt.savefig(folder + name + ".pdf", bbox_inches='tight', dpi=300)
            suffix="_png" #Matlab
        plt.close()
        m_script_name = folder + "/" + name.replace("-", "_") + suffix + ".m"  #Matlab doesn't like dashes in filenames
        with open(m_script_name, 'w') as file_handler:
            for line in m_script_content:
                file_handler.write("{}\n".format(line))
                # print("{}\n".format(line))
        m_script_content[:] = []


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
    
    m_script_name = [] # Matlab script filename holder
    # m_script_content = [] # Matlab script file holder
    chart = {}                                          # JSON Charts
    with open('chart_templates.json', 'r') as file:     # JSON Charts
        chart = json.load(file)['Chart']          # JSON Charts
    m_script_content.append("fig=figure;") # Matlab
    m_script_content.append("hax=axes;") # Matlab
    m_script_content.append("set(hax,'YLim',[0 100]);") # Matlab
    m_script_content.append("CornflowerBlue=[0.3906    0.5820    0.9258];") # Matlab
    m_script_content.append("LightSteelBlue=[0.6875    0.7656    0.8672];") # Matlab
    m_script_content.append("hold on;") # Matlab
    xy1_dat_name = [] # Matlab x1-y1 axis data filename holder
    xy1_dat_content = [] # Matlab x1-y1 axis data holder
    xy2_dat_name = [] # Matlab x2-y2 axis data filename holder
    xy2_dat_content = [] # Matlab x2-y2 axis data holder
    chart["YAxis"]["L"]["Limits"]["Lower"] = 0          # JSON Charts
    chart["YAxis"]["L"]["Limits"]["Upper"] = 100        # JSON Charts
    
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
            except KeyError as error:
                print(error)

        fig, ax1 = plt.subplots(figsize=(16, 5), dpi=300)
        # PLOT NORMALIZED TASK PROGRESS
        min_times = min(times)
        max_times = max(times)
        normalized = [(z - min_times) / (max_times - min_times) * 100 for z in times]
        timestamps_csv = " ".join(map(str, timestamps)) #Matlab
        normalized_csv = " ".join(map(str, normalized)) #Matlab
        ax1.plot(timestamps, normalized, "black", linewidth=3, zorder=10)
        #Matlab - tbd code equivalent to zorder
        m_script_content.append("plot(["+timestamps_csv+"], ["+normalized_csv+"],'color', 'black', 'LineWidth', 3);") #Matlab
        # timestamps_json = ",".join(map(str, timestamps)) #Matlab
        # normalized_json = ",".join(map(str, normalized)) #Matlab
        chart["TimestampsReal"] += timestamps                # JSON Charts
        chart["ProgressReal"] += normalized                          # JSON Charts
        ymin, ymax = ax1.get_ylim()
        ax1.set_ylim(0.0, 100.0)
        m_script_content.append("yl=get(hax,'YLim');") #Matlab
        m_script_content.append("ymin=yl(1,1);") #Matlab
        m_script_content.append("ymax=yl(1,2);") #Matlab
        m_script_content.append("set(hax,'YLim',[0 100]);") # Matlab
        chart["YAxis"]["L"]["Limits"]["Lower"] = 0          # JSON Charts
        chart["YAxis"]["L"]["Limits"]["Upper"] = 100        # JSON Charts
        xmin, xmax = ax1.get_xlim()
        ax1.set_xlim(0.0, xmax)
        m_script_content.append("xl=get(hax,'XLim');") #Matlab
        m_script_content.append("xmin=xl(1,1);") #Matlab
        m_script_content.append("xmax=xl(1,2);") #Matlab
        m_script_content.append("set(hax,'XLim',[0 xmax]);") # Matlab
        chart["XAxis"]["B"]["Limits"]["Lower"] = 0          # JSON Charts
        chart["XAxis"]["B"]["Limits"]["Upper"] = xmax       # JSON Charts
        # PLOT DEADLINE
        label_to_plot = {}
        original_ts = {}
        # app_dead_ts = app_deadline.timestamp() - first_ts
        #ax1.axvline(app_dead_ts, color="red", linewidth=3)
        if "agg" in name or "sort" in name:
            time_quantum = TQ_MICRO
        else:
            time_quantum = TQ_KMEANS
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
        tq_ts = math.floor(app_alpha_deadline_ts / time_quantum) * time_quantum
        original_ts[tq_ts] = app_alpha_deadline_ts
        ax1.axvline(app_alpha_deadline_ts, color="red", linewidth=3, zorder=10)
        m_script_content.append("vl_dead=line(["+str(app_alpha_deadline_ts)+" "+str(app_alpha_deadline_ts)+"], get(hax,'YLim'), 'color', 'red', 'LineWidth', 3);") # Matlab
        chart["VLines"]["D"] = {}                                               # JSON Charts
        chart["VLines"]["D"]["X"] = app_alpha_deadline_ts                       # JSON Charts
        chart["VLines"]["D"]["Type"] = "Deadline"                               # JSON Charts
        chart["VLines"]["D"]["Color"] = "red"                                   # JSON Charts
        chart["VLines"]["D"]["Line"] = "-"                                     # JSON Charts
        # chart["VLines"].append({"X" : app_alpha_deadline_ts, "Color" : "red",  "Line" : "-"})       # JSON Charts
        chart["Labels"]["D"] = {}                                               # JSON Charts
        chart["Labels"]["D"]["X"] = app_alpha_deadline_ts                       # JSON Charts
        chart["Labels"]["D"]["Y"] = 102                                       # JSON Charts
        try:
            label_to_plot[tq_ts].append("D")
        except KeyError:
            label_to_plot[tq_ts] = []
            label_to_plot[tq_ts].append("D")
        # ax1.text(app_alpha_deadline_ts, ymax + 0.5, 'AD', weight="bold",
        # horizontalalignment='center')
        ax1.set_xlabel(X_LABEL)
        m_script_content.append("xlabel('"+X_LABEL+"');")   #Matlab
        chart["XAxis"]["B"]["Label"] = X_LABEL              # JSON Charts
        ax1.set_ylabel(Y1_LABEL)
        m_script_content.append("yyaxis left") #Matlab
        m_script_content.append("ylabel('"+Y1_LABEL+"');") #Matlab
        chart["YAxis"]["L"]["Label"] = Y1_LABEL            # JSON Charts

        # PLOT VERTICAL LINES DEAD, START, END
        for sid in sorted_sid:
            try:
                # int_dead = app_info[app_id][sid]["deadline"].timestamp()
                # dead_ts = int_dead - first_ts
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
                # start_ts = app_info[app_id][sid]["start"].timestamp() - first_ts
                # ax1.axvline(start_ts, color="black")
                end = app_info[app_id][sid]["end"].timestamp()
                end_ts = end - first_ts
                tq_ts = math.floor(end_ts / time_quantum) * time_quantum
                original_ts[tq_ts] = end_ts
                try:
                    if "D" not in label_to_plot[tq_ts]:
                        label_to_plot[tq_ts].insert(0, "E" + str(sid))
                    else:
                        label_to_plot[tq_ts].append("E" + str(sid))
                except KeyError:
                    label_to_plot[tq_ts] = []
                    label_to_plot[tq_ts].append("E" + str(sid))
                ax1.axvline(end_ts, color="green")
                m_script_content.append("vl_dead=line(["+str(end_ts)+" "+str(end_ts)+"], get(hax,'YLim'), 'color', 'green');") # Matlab
                chart["Stages"][sid] = {}                                               # JSON Charts
                chart["Stages"][sid]["VLines"] = {}                                     # JSON Charts
                chart["Stages"][sid]["VLines"]["E" + str(sid)] = {}                     # JSON Charts
                chart["Stages"][sid]["VLines"]["E" + str(sid)]["X"] = end_ts            # JSON Charts
                chart["Stages"][sid]["VLines"]["E" + str(sid)]["Type"] = "End"          # JSON Charts
                chart["Stages"][sid]["VLines"]["E" + str(sid)]["Color"] = "green"       # JSON Charts
                chart["Stages"][sid]["VLines"]["E" + str(sid)]["Line"] = "-"            # JSON Charts
                # chart["VLines"].append({"X" : end_ts, "Color" : "green",  "Line" : "-"})       # JSON Charts
                chart["Stages"][sid]["Labels"] = {}                                     # JSON Charts
                chart["Stages"][sid]["Labels"]["E" + str(sid)] = {}                     # JSON Charts
                chart["Stages"][sid]["Labels"]["E" + str(sid)]["X"] = end_ts            # JSON Charts
                chart["Stages"][sid]["Labels"]["E" + str(sid)]["Y"] = 102               # JSON Charts
                chart["VLines"]["E" + str(sid)] = {}
                chart["VLines"]["E" + str(sid)] = chart["Stages"][sid]["VLines"]["E" + str(sid)]   # JSON Charts
                chart["Labels"]["E" + str(sid)] = {}
                chart["Labels"]["E" + str(sid)] = chart["Stages"][sid]["Labels"]["E" + str(sid)]        # JSON Charts
            except KeyError as error:
                print(error)

        if PLOT_LABEL:
            # Get the corrected text positions, then write the text.
            sorted_label_ts = [original_ts[ts] for ts in sorted(label_to_plot)]

            # print(text_positions)
            text_plotter_x(sorted_label_ts, ax1,
                           ["/".join(reversed(label_to_plot[ts])) for ts in sorted(label_to_plot)])
            #chart["Labels"] = text_plotter_x(sorted_label_ts, ax1,
            #               ["/".join(reversed(label_to_plot[ts])) for ts in sorted(label_to_plot)])

        # PLOT AGGREGATED CPU
        ax2 = ax1.twinx()
        m_script_content.append("yyaxis right;") #Matlab
        m_script_content.append("set(hax.YAxis, 'color', 'black');") #Matlab
        ax2.set_ylabel(Y_LABEL)
        # m_script_content.append("yyaxis right") #Matlab
        m_script_content.append("ylabel('"+Y_LABEL+"', 'color', 'black');") #Matlab
        chart["YAxis"]["R"]["Label"] = Y_LABEL            # JSON Charts
        ts_cpu = []
        cpus = []
        sid_len = {}
        for worker_log in workers_dict:
            worker_dict = workers_dict[worker_log]
            for sid in sorted(worker_dict[app_id]):
                next_time = worker_dict[app_id][sid]["time"][-1] + timedelta(
                    seconds=(int(config["Control"]["TSample"]) / 1000))
                if app_info[app_id][sid]["end"] < next_time and \
                                worker_dict[app_id][sid]["time"][-1].replace(microsecond=0) != \
                                app_info[app_id][sid]["end"].replace(microsecond=0) and \
                                app_info[app_id][sid]["end"] < app_info[app_id][sid]["deadline"]:
                    worker_dict[app_id][sid]["time"].append(app_info[app_id][sid]["end"])
                    worker_dict[app_id][sid]["cpu"].append(worker_dict[app_id][sid]["cpu"][-1])
                elif next_time < app_info[app_id][sid]["end"]:
                    worker_dict[app_id][sid]["time"].append(next_time)
                    worker_dict[app_id][sid]["cpu"].append(worker_dict[app_id][sid]["cpu"][-1])
                try:
                    sid_len[sid] = max(sid_len[sid], len(worker_dict[app_id][sid]["cpu"]))
                except KeyError:
                    sid_len[sid] = len(worker_dict[app_id][sid]["cpu"])
        for sid in sorted(sid_len):
            if sid > sorted(sid_len.keys())[0]:
                # print(sid, sorted(sid_len)[0])
                sid_len[sid] += sid_len[list(sorted(sid_len))[list(sorted(sid_len)).index(sid) - 1]]
        sid_len_keys = list(sid_len.keys())
        max_cpu = (len(list(workers_dict.keys())) * config["Control"]["CoreVM"])
        # first_ts = app_info[app_id][PLOT_SID_STAGE]["start"].replace(microsecond=0).timestamp()
        for worker_log in workers_dict.keys():
            print(worker_log)
            worker_dict = workers_dict[worker_log]
            first_sid = sorted(worker_dict[app_id])[0]
            for sid in sorted(worker_dict[app_id]):
                s_index = sid_len[
                    sid_len_keys[sid_len_keys.index(sid) - 1]] if sid != first_sid else 0

                for time_cpu, cpu in \
                        zip(worker_dict[app_id][sid]["time"], worker_dict[app_id][sid]["cpu"]):
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
                        # print(ts_cpu[s_index] != (time_cpu_ts - first_ts), abs(ts_cpu[s_index] - (time_cpu_ts - first_ts)) >= \
                        #           (config["Control"]["TSample"] / 1000))
                        if (sid == 1 and app_id == "app-20160930162120-0000") or (
                                sid in [0, 1, 2] and app_id == "app-20160930170244-0000") \
                                or (app_id == "app-20161001190429-0000"):
                            # eval = abs(ts_cpu[s_index] - (time_cpu_ts - first_ts)) >= (config["Control"]["TSample"] / 1000)
                            eval = False
                        else:
                            eval = True
                        # print(ts_cpu[s_index] != (time_cpu_ts - first_ts), eval)
                        # eval = True
                        if ts_cpu[s_index] != (time_cpu_ts - first_ts) and eval:
                            # print(ts_cpu)
                            # print([tixm.replace(microsecond=0).timestamp() -
                            # first_ts for tixm in worker_dict[app_id][sid]["time"]])
                            # print("QUI", sid, (time_cpu_ts - first_ts),  ts_cpu[s_index])
                            index = ts_cpu.index(time_cpu_ts - first_ts)
                            cpus[index] += cpu
                        else:
                            # if (cpus[s_index] + cpu) > max_cpu:
                            #     print("+0 ERR")
                            #     if (cpus[s_index + 1] + cpu) > max_cpu:
                            #         print("+1 ERR")
                            #         if (cpus[s_index + 2] + cpu) > max_cpu:
                            #             print("+2 ERR")
                            #         cpus[s_index + 2] += cpu
                            #     else:
                            #         cpus[s_index + 1] += cpu
                            # else:
                            cpus[s_index] += cpu
                            if ts_cpu[s_index] == 0.0:
                                # print(time_cpu_ts - first_ts)
                                ts_cpu[s_index] = time_cpu_ts - first_ts
                    except (IndexError, ValueError):
                        ts_cpu.append(time_cpu_ts - first_ts)
                        cpus.append(cpu)
                    s_index += 1
                padding = sid_len[sid] - len(worker_dict[app_id][sid]["cpu"])
                if len(ts_cpu) < sid_len[sid] and padding > 0:
                    for i in range(padding):
                        next_ts = ts_cpu[-1] + (config["Control"]["TSample"] / 1000)
                        if next_ts <= (end_ts - first_ts):
                            ts_cpu.append(next_ts)
                            cpus.append(0.0)
        for sid in sorted_sid:
            if sid != sorted_sid[-1]:
                end = app_info[app_id][sid]["end"].replace(microsecond=100).timestamp() - first_ts
                next_sid = sorted_sid[sorted_sid.index(sid) + 1]
                next_start = app_info[app_id][next_sid]["start"].replace(
                    microsecond=0).timestamp() - first_ts
                if (next_start - end) > (config["Control"]["TSample"] / 1000):
                    print("ERR ", sid)
                    ts_cpu.append(end + 0.01)
                    cpus.append(0.0)
                    ts_cpu.append(next_start - 0.01)
                    cpus.append(0.0)
        ts_cpu, cpus = (list(t) for t in zip(*sorted(zip(ts_cpu, cpus))))
        # for sid in sorted_sid:
        #     end = app_info[app_id][sid]["end"].replace(microsecond=100).timestamp() - first_ts
        #     try:
        #         index_ts = ts_cpu.index(end)
        #     except ValueError:
        #         index_ts = min(range(len(ts_cpu)), key=lambda i: abs(ts_cpu[i] - end))
        #     if cpus[index_ts] == 0.0:
        #         cpus[index_ts] = cpus[index_ts - 1]
        ts_cpu.append(end_ts)
        cpus.append(cpus[-1] * 2 / 3)
        ax2.plot(ts_cpu, cpus, zorder=0)
        ts_cpu_csv = " ".join(map(str, ts_cpu)) #Matlab
        cpus_csv = " ".join(map(str, cpus)) #Matlab
        m_script_content.append("cpuline=plot(["+ts_cpu_csv+"], ["+cpus_csv+"], '.-', 'color', 'blue', 'LineWidth', 2, 'MarkerEdgeColor', 'blue', 'MarkerFaceColor', 'blue','MarkerSize', 12);") #Matlab
        # ts_cpu_json = ",".join(map(str, ts_cpu))                        # JSON Charts
        # cpus_json = ",".join(map(str, cpus))                            # JSON Charts
        chart["TimestampsCpu"].append(ts_cpu)                           # JSON Charts
        chart["Cpu"].append(cpus)                                       # JSON Charts
        m_script_content.append("xd=get(cpuline,'xdata');") #Matlab
        m_script_content.append("yd=get(cpuline,'ydata');") #Matlab
        m_script_content.append("a=area(xd, yd);") #Matlab
        m_script_content.append("set(a, 'FaceAlpha',0.5,'FaceColor', LightSteelBlue);") #Matlab
        xmin, xmax = ax2.get_xlim()
        ax2.set_xlim(0.0, xmax)
        m_script_content.append("xl=get(hax,'XLim');") #Matlab
        m_script_content.append("xmin=xl(1,1);") #Matlab
        m_script_content.append("xmax=xl(1,2);") #Matlab
        m_script_content.append("set(hax,'XLim',[0 xmax]);") # Matlab
        chart["XAxis"]["B"]["Limits"]["Lower"] = 0          # JSON Charts
        chart["XAxis"]["B"]["Limits"]["Upper"] = xmax       # JSON Charts
        ymin, ymax = ax2.get_ylim()
        print(ymax)
        ax2.set_ylim(0.0, len(list(workers_dict.keys())) * config["Control"]["CoreVM"])
        ym = len(list(workers_dict.keys())) * config["Control"]["CoreVM"] #Matlab - ym is total (max) # of available cores
        m_script_content.append("yl=get(hax,'YLim');") #Matlab
        m_script_content.append("ymin=yl(1,1);") #Matlab
        m_script_content.append("ymax=yl(1,2);") #Matlab
        m_script_content.append("set(hax,'YLim',[0 "+str(ym)+"]);") # Matlab
        chart["YAxis"]["R"]["Limits"]["Lower"] = 0          # JSON Charts
        chart["YAxis"]["R"]["Limits"]["Upper"] = ym         # JSON Charts

        ax2.fill_between(ts_cpu, 0.0, cpus, facecolor="b", alpha=0.2)
        # Matlab area fill tbd
        yaxis_multiplier = float(config["Control"]["CoreVM"])
        if len(workers_dict) > 11:
            yaxis_multiplier = float(config["Control"]["CoreVM"]) * 2
        ax2.yaxis.set_major_locator(plt_ticker.MultipleLocator(base=yaxis_multiplier))
        m_script_content.append("yyaxis right;") #Matlab
        m_script_content.append("YTick = 0:"+str(yaxis_multiplier)+":" +str(ym)+ ";") #Matlab
        m_script_content.append("yticks(YTick);") #Matlab
        chart["YAxis"]["R"]["TickBase"] = yaxis_multiplier                   # JSON Charts
        ax1.yaxis.set_major_locator(plt_ticker.MultipleLocator(base=10.0))
        m_script_content.append("yyaxis left;")         #Matlab
        m_script_content.append("YTick = 0:10.0:100;")  #Matlab
        m_script_content.append("yticks(YTick);")       #Matlab
        chart["YAxis"]["L"]["TickBase"] = 10.0          # JSON Charts

        if "agg" in name:
            ax1.xaxis.set_major_locator(plt_ticker.MultipleLocator(base=Y_TICK_AGG))
            m_script_content.append("XTick = 0:"+Y_TICK_AGG+":100;") #Matlab
            chart["XAxis"]["B"]["TickBase"] = Y_TICK_AGG             # JSON Charts
            
        elif "sort" in name:
            ax1.xaxis.set_major_locator(plt_ticker.MultipleLocator(base=Y_TICK_SORT))
            m_script_content.append("XTick = 0:"+Y_TICK_SORT+":100;") #Matlab
            chart["XAxis"]["B"]["TickBase"] = Y_TICK_SORT             # JSON Charts
        if TITLE:
            plt.title(app_id + " " + str(config["Deadline"]) + " " + \
                      str(config["Control"]["TSample"]) + " " + str(config["Control"]["Alpha"]) + \
                      " " + str(config["Control"]["K"]))
            title = app_id + " " + str(config["Deadline"]) + " " + \
                      str(config["Control"]["TSample"]) + " " + str(config["Control"]["Alpha"]) + \
                      " " + str(config["Control"]["K"])
            m_script_content.append("title(hax, '"+title+"');") #Matlab
            chart["Title"] = title                              # JSON Charts

        ax1.set_zorder(ax2.get_zorder() + 1)
        # Layer = 'top|bottom'  #Matlab - limited availability to patch vs. axis z-order?
        ax1.patch.set_visible(False)
        # tbd equivalent Matlab code #Matlab
        labels = ax1.get_xticklabels()
        plt.setp(labels, rotation=45)
        m_script_content.append("set(hax.XAxis, 'TickLabelRotation', 45)") #Matlab
        chart["Name"] = name                                                # JSON Charts
        charts['Charts'].append(chart)                                      # JSON Charts
        # latexify(columns=1)
        # format_axes(ax1)
        # format_axes(ax2)
        if PDF:
            plt.savefig(folder + name + ".pdf", bbox_inches='tight', dpi=300)
            suffix="_pdf" #Matlab
        else:
            plt.savefig(folder + name + ".png", bbox_inches='tight', dpi=300)
            suffix="_png" #Matlab
        plt.close()
        m_script_name = folder + name.replace("-", "_") + suffix + ".m"  #Matlab doesn't like dashes in filenames
        with open(m_script_name, 'w') as file_handler:  #Matlab
            for line in m_script_content:
                file_handler.write("{}\n".format(line))
                # print("{}\n".format(line))
        m_script_content[:] = []        
    return cpus, ts_cpu


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
            print("Stage ID {0} not executed by {1}".format(PLOT_SID_STAGE, worker_log))
    return first_ts_worker


def plot_mean_comparision(folders):
    """

    :param folders:
    :return:
    """
    import numpy as np
    
    m_script_name = [] # Matlab script filename holder
    # m_script_content = [] # Matlab script file holder
    m_script_content.append("fig=figure;") # Matlab
    m_script_content.append("hax=axes;") # Matlab
    m_script_content.append("set(hax,'YLim',[0 100]);") # Matlab
    m_script_content.append("hold on;") # Matlab
    xy1_dat_name = [] # Matlab x1-y1 axis data filename holder
    xy1_dat_content = [] # Matlab x1-y1 axis data holder
    xy2_dat_name = [] # Matlab x2-y2 axis data filename holder
    xy2_dat_content = [] # Matlab x2-y2 axis data holder    

    x_multi = []
    for folder in folders:
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
            app_info = load_app_data(app_log)

        worker_logs = glob.glob(folder + "*worker*.out")
        cpu_logs = glob.glob(folder + "sar*.log")

        if len(worker_logs) == len(cpu_logs):
            workers_dict = {}
            for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
                worker_dict = load_worker_data(worker_log, cpu_log, config)
                workers_dict[worker_log] = worker_dict

            for app_id in app_info:
                cpus, ts_cpu = plot_overview_cpu(app_id, app_info, workers_dict, config, folder)
                if "/0%" in folder and len(x_multi) > 0:
                    break
                    # old_set = x_multi.pop(len(x_multi) - 1)
                    # x_multi.append(np.mean(np.array([old_set, cpus]), axis=0, dtype=np.float64))
                elif "20%" in folder and len(x_multi) > 1:
                    break
                    # old_set = x_multi.pop(len(x_multi) - 1)
                    # print(old_set)
                    # print(cpus)
                    # if len(old_set) > len(cpus)
                    #     print(np.array([old_set, cpus]))
                    #     print(np.mean(np.array([old_set, cpus]), axis=0, dtype=np.float64))
                    # x_multi.append()
                elif "40%" in folder and len(x_multi) > 2:
                    break
                    # old_set = x_multi.pop(len(x_multi) - 1)
                    # x_multi.append(np.mean(np.array([old_set, cpus]), axis=0, dtype=np.float64))
                else:
                    if config["Control"]["TSample"] == 5000:
                        print("FOLDER", folder)
                        print("XMULTI", x_multi)
                        print("XMULTI LEN", len(x_multi))
                        x_multi.append(cpus)

        else:
            print("ERROR: SAR != WORKER LOGS")
    max_len = 0
    for cpu in x_multi:
        max_len = max(len(cpu), max_len)
    for cpu in x_multi:
        if len(cpu) < max_len:
            print(max_len - len(cpu))
            for i in range(max_len - len(cpu)):
                x_multi[x_multi.index(cpu)].append(0)
    print(len(x_multi[0]))
    zero = np.mean(np.array(x_multi[0]).reshape(-1, 4), axis=1)
    twenty = np.mean(np.array(x_multi[1]).reshape(-1, 4), axis=1)
    fourty = np.mean(np.array(x_multi[2]).reshape(-1, 4), axis=1)
    ind = np.arange(len(zero))
    width = 0.35  # the width of the bars
    ind_csv = " ".join(map(str, ind)) #Matlab
    zero_csv = " ".join(map(str, zero)) #Matlab
    twenty_csv = " ".join(map(str, twenty)) #Matlab
    fourty_csv = " ".join(map(str, fourty)) #Matlab
    fig, ax1 = plt.subplots(figsize=(16, 5), dpi=300)
    zero_bar = ax1.bar(ind, zero, width, color='r', label="0%") 
    m_script_content.append("bar(["+ind_csv+"], ["+zero_csv+"], "+str(width)+",'red');") #Matlab
    twenty_bar = ax1.bar(ind + width, twenty, width, color='b', label="20%")
    m_script_content.append("bar(["+ind_csv+"], ["+twenty_csv+"], "+str(width)+",'blue');") #Matlab
    fourty_bar = ax1.bar(ind + width + width, fourty, width, color='green', label="40%")
    m_script_content.append("bar(["+ind_csv+"], ["+fourty_csv+"], "+str(width)+",'green');") #Matlab
    ax1.set_ylabel('Core [avg]')
    m_script_content.append("yyaxis left") #Matlab
    m_script_content.append("ylabel('Core [avg]');") #Matlab
    ax1.set_xlabel('TimeSlot [10s]')
    m_script_content.append("yyaxis right") #Matlab
    m_script_content.append("ylabel('TimeSlot [10s]');") #Matlab
    ax1.set_xticks(ind + width + width)
    m_script_content.append("XTick = 0:"+str(ind + width + width)+":1000;") #Matlab
    labels = ax1.get_xticklabels()
    plt.setp(labels, rotation=45)
    m_script_content.append("set(hax.XAxis, 'TickLabelRotation', 45)") #Matlab
    plt.legend()
    m_script_content.append("lgd=legend()") #Matlab
    plt.tight_layout()
    m_script_content.append("axis tight") #Matlab - apparently this is the closest Matlab way to do it
    fig.savefig("hist.png")
    m_script_name = "hist.m"  #Matlab
    with open(m_script_name, 'w') as file_handler:
            for line in m_script_content:
                file_handler.write("{}\n".format(line))
                # print("{}\n".format(line))
    m_script_content[:] = []
                

@timing
def plot(folder):
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
        app_info = load_app_data(app_log)

        for app_id in app_info:
            plot_app_overview(app_id, app_info[app_id], folder, config)

    worker_logs = glob.glob(folder + "*worker*.out")
    cpu_logs = glob.glob(folder + "sar*.log")

    if len(worker_logs) == len(cpu_logs):
        workers_dict = {}
        for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
            worker_dict = load_worker_data(worker_log, cpu_log, config)
            workers_dict[worker_log] = worker_dict

        first_ts_worker = -1.0
        for worker_log, cpu_log in zip(sorted(worker_logs), sorted(cpu_logs)):
            for app_id in app_info:
                if first_ts_worker == -1.0:
                    first_ts_worker = find_first_ts_worker(app_id, workers_dict)
                    if first_ts_worker == -1.0:
                        print("ERROR FIRST TS WORKER")
                        exit(1)
                plot_worker(app_id, app_info, worker_log, workers_dict[worker_log], config,
                            first_ts_worker)
                print_cpu(app_id, app_info, worker_log, workers_dict[worker_log], config, folder, first_ts_worker)
        for app_id in app_info:
            plot_overview_cpu(app_id, app_info, workers_dict, config, folder)
    else:
        print("ERROR: SAR != WORKER LOGS")
 
    with open(folder + json_charts_filename, 'w') as file:          # JSON Charts
        json.dump(charts, file, indent = 4)                         # JSON Charts
        
    source = ('./Matlab/')                                          # JSON Charts
    for filename in os.listdir(source):                             # JSON Charts
        if filename.endswith('.m'):                                 # JSON Charts
            shutil.copy(source+filename, folder+filename)           # JSON Charts
    
    # moved to run.py and process_on_server.py
    # for filename in os.listdir('./output_json/'):                   # Profiling on server
    #     shutil.copy('./output_json/' + filename, folder + filename)     # Profiling on server
               
    rename_files(folder)

def print_cpu(app_id, app_info, worker_log, worker_dict, config, folder, first_ts_worker):
    sorted_sid = sorted(app_info[app_id])
    if config["HDFS"]:
        sorted_sid.remove(0)
    filename = worker_log.split(".")[-2] + ".cpu_profile.txt"
    with open(folder + filename, 'w') as file:
        for sid in sorted_sid:
            if sid in worker_dict[app_id]:
                cpu = worker_dict[app_id][sid]["cpu"]
                time = [t.timestamp() - first_ts_worker for t in worker_dict[app_id][sid]["time"]]
                time = ["{:f}".format(a) for a in time]
                text = str(list(zip(time, cpu))).replace("'", "")
                file.write("SID " + str(sid) + " " + text + "\n")

def rename_files(folder):
    for filename in os.listdir(folder):
        if filename.endswith(".err"):
            os.rename(folder + filename, folder + "app.err")
        elif filename.endswith(".dat") and filename.find("run") > -1:
            os.rename(folder + filename, folder + "app.dat")