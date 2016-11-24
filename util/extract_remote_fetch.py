import json

remote_fetch_dict = {}
with open("./results\spark_perf_output__2016-09-19_12-03-51_logs/app-20160919120358-0000") as app_log:
    for line in app_log:
        data = json.loads(line)
        if data["Event"] == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
            #print(data)
            try:
                remote_fetch_dict[data["Task Info"]["Host"]]["Fetch Wait Time"] += data["Task Metrics"]["Shuffle Read Metrics"]["Fetch Wait Time"]
                remote_fetch_dict[data["Task Info"]["Host"]]["Remote Bytes Read"] += data["Task Metrics"]["Shuffle Read Metrics"]["Remote Bytes Read"]
                remote_fetch_dict[data["Task Info"]["Host"]]["Remote Blocks Fetched"] += data["Task Metrics"]["Shuffle Read Metrics"]["Remote Blocks Fetched"]

            except KeyError:
                # print(data)
                remote_fetch_dict[data["Task Info"]["Host"]] = {}
                remote_fetch_dict[data["Task Info"]["Host"]]["Fetch Wait Time"] = data["Task Metrics"]["Shuffle Read Metrics"]["Fetch Wait Time"]
                remote_fetch_dict[data["Task Info"]["Host"]]["Remote Bytes Read"] = data["Task Metrics"]["Shuffle Read Metrics"]["Remote Bytes Read"]
                remote_fetch_dict[data["Task Info"]["Host"]]["Remote Blocks Fetched"] = data["Task Metrics"]["Shuffle Read Metrics"]["Remote Blocks Fetched"]
print(remote_fetch_dict)