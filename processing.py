import bz2
import glob
import json
import re
from collections import OrderedDict

import numpy as np


def main():
    for log in glob.glob("./input_logs/app-*"):
        app_name = ""
        # Build stage dictionary
        stage_dict = OrderedDict()
        if ".bz" in log:
            file_open = bz2.BZ2File(log, "r")
        else:
            file_open = open(log)

        with file_open as logfile:
            print(log)
            for line in logfile:
                if ".bz" in log:
                    line = line.decode("utf-8")
                data = json.loads(line)
                try:
                    if data["Event"] == "SparkListenerApplicationStart":
                        app_name = data["App Name"]
                    elif data["Event"] == "SparkListenerStageSubmitted":
                        # print(data)
                        stage = data["Stage Info"]
                        stage_id = stage["Stage ID"]
                        if stage_id not in stage_dict.keys():
                            stage_dict[stage_id] = {}
                            if stage_id == 0:
                                stage_dict[0]["totalduration"] = 0
                            stage_dict[stage_id]["name"] = stage['Stage Name']
                            stage_dict[stage_id]["genstage"] = False
                            stage_dict[stage_id]["parentsIds"] = stage["Parent IDs"]
                            stage_dict[stage_id]["nominalrate"] = 0.0
                            stage_dict[stage_id]["weight"] = 0
                            stage_dict[stage_id]["RDDIds"] = {
                                x["RDD ID"]: {"name": x["Name"], "callsite": x["Callsite"]} for x in
                                stage["RDD Info"]}
                            stage_dict[stage_id]["skipped"] = False
                            stage_dict[stage_id]["cachedRDDs"] = []
                            stage_dict[stage_id]["numtask"] = 0
                            stage_dict[stage_id]["recordsread"] = 0.0
                            stage_dict[stage_id]["shufflerecordsread"] = 0.0
                            stage_dict[stage_id]["recordswrite"] = 0.0
                            stage_dict[stage_id]["shufflerecordswrite"] = 0.0
                            for rdd_info in stage["RDD Info"]:
                                storage_level = rdd_info["Storage Level"]
                                if storage_level["Use Disk"] or storage_level["Use Memory"] or \
                                        storage_level["Deserialized"]:
                                    stage_dict[stage_id]["cachedRDDs"].append(rdd_info["RDD ID"])
                    elif data["Event"] == "SparkListenerStageCompleted":
                        # print(data)
                        stage_id = data["Stage Info"]["Stage ID"]
                        stage_dict[stage_id]["numtask"] = data["Stage Info"]['Number of Tasks']
                        for acc in data["Stage Info"]["Accumulables"]:
                            if acc["Name"] == "internal.metrics.executorRunTime":
                                stage_dict[stage_id]["duration"] = int(acc["Value"])
                                stage_dict[0]["totalduration"] += int(acc["Value"])
                            if acc["Name"] == "internal.metrics.input.recordsRead":
                                stage_dict[stage_id]["recordsread"] = acc["Value"]
                            if acc["Name"] == "internal.metrics.shuffle.read.recordsRead":
                                stage_dict[stage_id]["shufflerecordsread"] = acc["Value"]
                            if acc["Name"] == "internal.metrics.output.recordsWrite":
                                stage_dict[stage_id]["recordswrite"] = acc["Value"]
                            if acc["Name"] == "internal.metrics.shuffle.write.recordsWritten":
                                stage_dict[stage_id]["shufflerecordswrite"] = acc["Value"]
                except KeyError:
                    print(data)

        skipped = []
        if ".bz" in log:
            file_open = bz2.BZ2File(log, "r")
        else:
            file_open = open(log)
        with file_open as logfile:
            for line in logfile:
                if ".bz" in log:
                    line = line.decode("utf-8")
                data = json.loads(line)
                try:
                    if data["Event"] == "SparkListenerJobStart":
                        for stage in data["Stage Infos"]:
                            stage_id = stage["Stage ID"]
                            # print(stage)
                            if stage["Stage ID"] not in stage_dict.keys():
                                stage_dict[stage_id] = {}
                                stage_dict[stage_id]["name"] = stage['Stage Name']
                                stage_dict[stage_id]["genstage"] = False
                                stage_dict[stage_id]["parentsIds"] = stage["Parent IDs"]
                                stage_dict[stage_id]["nominalrate"] = 0.0
                                stage_dict[stage_id]["weight"] = 0
                                stage_dict[stage_id]["RDDIds"] = {
                                    x["RDD ID"]: {"name": x["Name"], "callsite": x["Callsite"]} for
                                    x in
                                    stage["RDD Info"]}
                                stage_dict[stage_id]["skipped"] = True
                                stage_dict[stage_id]["cachedRDDs"] = []
                                stage_dict[stage_id]["numtask"] = 0
                                stage_dict[stage_id]["recordsread"] = 0.0
                                stage_dict[stage_id]["shufflerecordsread"] = 0.0
                                stage_dict[stage_id]["recordswrite"] = 0.0
                                stage_dict[stage_id]["shufflerecordswrite"] = 0.0
                                for rdd_info in stage["RDD Info"]:
                                    storage_level = rdd_info["Storage Level"]
                                    if storage_level["Use Disk"] or storage_level["Use Memory"] or \
                                            storage_level["Deserialized"]:
                                        stage_dict[stage_id]["cachedRDDs"].append(
                                            rdd_info["RDD ID"])
                                skipped.append(stage_id)
                except KeyError:
                    None

        # Replace skipped stage id in parents ids based on RDD IDs
        for skipped_id in skipped:
            for stage_id1 in stage_dict.keys():
                if stage_id1 != skipped_id and stage_dict[skipped_id]["RDDIds"] == \
                        stage_dict[stage_id1]["RDDIds"]:
                    for stage_id2 in stage_dict.keys():
                        if skipped_id in stage_dict[stage_id2]["parentsIds"]:
                            stage_dict[stage_id2]["parentsIds"].remove(skipped_id)
                            stage_dict[stage_id2]["parentsIds"].append(stage_id1)

        for stage in stage_dict.keys():
            if len(stage_dict[stage]["parentsIds"]) == 0:
                try:
                    cached = list(stage_dict[stage]["cachedRDDs"])
                except KeyError:
                    None
                for i in range(0, stage):
                    try:
                        for rdd in cached:
                            if rdd in stage_dict[i]["cachedRDDs"]:
                                stage_dict[stage]["parentsIds"].append(i)
                                cached.remove(rdd)
                    except KeyError:
                        None

        print(stage_dict)

        # REPEATER = re.compile(r"(.+?)\1+$")
        # def repeated(s):
        #     match = REPEATER.match(s)
        #     return match.group(1) if match else None
        #
        # # Find iterations
        # lenparent = []
        # for key in stageDict.keys():
        #     lenparent.append(str(len(stageDict[key]['Parent IDs'])))
        # i = 0
        # stage_repeated = None
        # while stage_repeated == None and i < len(lenparent):
        #     stage_repeated = repeated("".join(lenparent[i:]))
        #     i += 1
        # print(i, stage_repeated)

        # def setWeight(key):
        #     for parentid in stageDict[key]['parentsIds']:
        #         w1 = stageDict[key]["weight"] + 1
        #         w2 = stageDict[parentid]["weight"]
        #         stageDict[parentid]["weight"] = max(w1, w2)
        #         setWeight(parentid)
        #
        # # Set weights
        # for key in reversed(stageDict.keys()):
        #     setWeight(key)

        stage_to_do = len(list(stage_dict.keys())) - len(skipped)
        for stage_id in sorted(stage_dict.keys()):
            parent_output = 0
            parent_input = 0
            if stage_id not in skipped:
                stage_dict[stage_id]["weight"] = stage_to_do
                stage_to_do -= 1
                for parent_id in stage_dict[stage_id]["parentsIds"]:
                    parent_output += stage_dict[parent_id]["recordswrite"]
                    parent_output += stage_dict[parent_id]["shufflerecordswrite"]
                    parent_input += stage_dict[parent_id]["recordsread"]
                    parent_input += stage_dict[parent_id]["shufflerecordsread"]
                if parent_output != 0:
                    stage_dict[stage_id]["nominalrate"] = parent_output / (
                        stage_dict[stage_id]["duration"] / 1000.0)
                elif parent_input != 0:
                    stage_dict[stage_id]["nominalrate"] = parent_input / (
                        stage_dict[stage_id]["duration"] / 1000.0)
                else:
                    stage_input = stage_dict[stage_id]["recordsread"] + stage_dict[stage_id][
                        "shufflerecordsread"]
                    if stage_input != 0 and stage_input != stage_dict[stage_id]["numtask"]:
                        stage_dict[stage_id]["nominalrate"] = stage_input / (
                            stage_dict[stage_id]["duration"] / 1000.0)
                    else:
                        stage_output = stage_dict[stage_id]["recordswrite"] + stage_dict[stage_id][
                            "shufflerecordswrite"]
                        stage_dict[stage_id]["nominalrate"] = stage_input / (
                            stage_dict[stage_id]["duration"] / 1000.0)
                if stage_dict[stage_id]["nominalrate"] == 0.0:
                    stage_dict[stage_id]["genstage"] = True

        totalduration = stage_dict[0]["totalduration"]
        for key in stage_dict.keys():
            if key not in skipped:
                old_weight = stage_dict[key]["weight"]
                stage_dict[key]["weight"] = np.mean(
                    [old_weight, totalduration / stage_dict[key]["duration"]])
                totalduration -= stage_dict[key]["duration"]

        # Create json output
        with open("./output_json/" + re.sub("[^a-zA-Z0-9.-]", "_", app_name)+"_"+log.split("-")[1]+ ".json",
                  "w") as jsonoutput:
            json.dump(stage_dict, jsonoutput, indent=4, sort_keys=True)


if __name__ == "__main__":
    main()
