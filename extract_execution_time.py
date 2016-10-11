import json

from config import *

exec_time_dict = {}
with open("./results\spark_perf_output__2016-09-18_14-51-11_logs/app-20160918145118-0000") as app_log:
    for line in app_log:
        data = json.loads(line)
        if data["Event"] == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
            #print(data["Task Metrics"])
            try:
                exec_time_dict[data["Stage ID"]]['Time'] += data["Task Metrics"]['Executor Run Time'] + data["Task Metrics"]["Shuffle Write Metrics"]['Shuffle Write Time'] +  data["Task Metrics"]["Shuffle Read Metrics"]['Fetch Wait Time'] + data["Task Metrics"]['Result Serialization Time'] + data["Task Metrics"]['Executor Deserialize Time'] + data["Task Metrics"]['JVM GC Time']
                exec_time_dict[data["Stage ID"]]['Records Read'] += data["Task Metrics"]['Input Metrics']['Records Read']

            except KeyError:
                # print(data)
                exec_time_dict[data["Stage ID"]] = {}
                exec_time_dict[data["Stage ID"]]['Time'] = data["Task Metrics"]['Executor Run Time'] + data["Task Metrics"]["Shuffle Write Metrics"]['Shuffle Write Time'] +  data["Task Metrics"]["Shuffle Read Metrics"]['Fetch Wait Time'] + data["Task Metrics"]['Result Serialization Time'] + data["Task Metrics"]['Executor Deserialize Time'] + data["Task Metrics"]['JVM GC Time']
                exec_time_dict[data["Stage ID"]]['Records Read'] = data["Task Metrics"]['Input Metrics']['Records Read']

print(exec_time_dict)

for host in exec_time_dict.keys():
    nominalrate = exec_time_dict[host]['Records Read'] / ((exec_time_dict[host]['Time'] / 1000.0) * COREVM)
    print(nominalrate)