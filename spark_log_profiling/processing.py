import bz2
import glob
import json
import re
import os
import errno
from collections import OrderedDict
import sys

import numpy as np

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(ROOT_DIR, 'input_logs')
OUTPUT_DIR = os.path.join(ROOT_DIR, 'output_json')

def make_sure_path_exists(path):
    """"Check if the provided path exists. If it does not exist, create it."""
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def gather_records_rw(stages):
    not_skipped = {k: v for k, v in stages.items() if v['skipped'] == False}
    dataset = sorted(map(lambda args: (args[1].update({'id': int(args[0])}) or args[1]),
                         not_skipped.items()), key=lambda v: v['id'], reverse=False)

    # total_duration = sum(map(lambda x: x['duration'], dataset))/1000
    # total_data = sum(map(lambda x: max(x['recordsread'], x['recordswrite'], x['shufflerecordsread'], x['shufflerecordswrite']), dataset))
    reads = {}
    writes = {}
    numtask = dataset[0]['numtask']
    for i in range(0, len(dataset)):
        stage = dataset[i]
        stage_id = stage['id']

        reads[stage_id] = 0
        if len(stage['parentsIds']) == 0:
                reads[stage_id] = stage['recordsread']
        else:
            for parent_id in stage['parentsIds']:
                reads[stage_id] += writes[parent_id]

        if stage['shufflerecordswrite'] > 0 and stage['shufflerecordswrite'] % numtask > 0:
            writes[stage_id] = stage['shufflerecordswrite']
        elif stage['recordswrite'] > 0 and stage['recordswrite'] % numtask > 0:
            writes[stage_id] = stage['recordswrite']
        else:
            writes[stage_id] = reads[stage_id]
    #print(reads)
    #print(writes)
    # add actual reads, writes and io_factor writes to stages_dict
    for k, v in reads.items():
        stages[k]['actual_records_read'] = v
        stages[k]['actual_records_write'] = writes[k]
        stages[k]['t_record_ta_executor'] = float(stages[k]['duration']) / float(v) if v > 0 else 0
        stages[k]['io_factor'] = float(writes[k]) / float(v) if v > 0 else 0


def main(input_dir=INPUT_DIR, json_out_dir=OUTPUT_DIR, reprocess=False):
    processed_dir = os.path.join(ROOT_DIR, 'processed_logs')
    if reprocess:
        input_dir = processed_dir
    make_sure_path_exists(input_dir)
    make_sure_path_exists(processed_dir)
    print("Start log profiling: \ninput_dir:\t{}\nprocessed_dir:\t{}\noutput_dir:\t{}".format(input_dir,
                                                                                           processed_dir,
                                                                                           json_out_dir))
    for log in glob.glob(os.path.join(input_dir, 'app-*')):
        app_name = ""
        # Build stage dictionary
        stage_dict = OrderedDict()
        if ".bz" in log:
            file_open = bz2.BZ2File(log, "r")
        else:
            file_open = open(log)

        with file_open as logfile:
            #print(log)
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
                            stage_dict[stage_id]["bytesread"] = 0.0
                            stage_dict[stage_id]["shufflebytesread"] = 0.0
                            stage_dict[stage_id]["byteswrite"] = 0.0
                            stage_dict[stage_id]["shufflebyteswrite"] = 0.0
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
                            if acc["Name"] == "internal.metrics.input.bytesRead":
                                stage_dict[stage_id]["bytesread"] = acc["Value"]
                            if acc["Name"] == "internal.metrics.shuffle.read.localBytesRead":
                                stage_dict[stage_id]["shufflebytesread"] = acc["Value"]
                            if acc["Name"] == "internal.metrics.output.bytesWrite":
                                stage_dict[stage_id]["byteswrite"] = acc["Value"]
                            if acc["Name"] == "internal.metrics.shuffle.write.bytesWritten":
                                stage_dict[stage_id]["shufflebyteswrite"] = acc["Value"]    
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
                                stage_dict[stage_id]["bytesread"] = 0.0
                                stage_dict[stage_id]["shufflebytesread"] = 0.0
                                stage_dict[stage_id]["byteswrite"] = 0.0
                                stage_dict[stage_id]["shufflebyteswrite"] = 0.0
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
        if stage_dict:
            gather_records_rw(stage_dict)
            #print(stage_dict)

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
                parent_output_bytes = 0
                parent_input_bytes = 0
                if stage_id not in skipped:
                    stage_dict[stage_id]["weight"] = stage_to_do
                    stage_to_do -= 1
                    for parent_id in stage_dict[stage_id]["parentsIds"]:
                        parent_output += stage_dict[parent_id]["recordswrite"]
                        parent_output += stage_dict[parent_id]["shufflerecordswrite"]
                        parent_input += stage_dict[parent_id]["recordsread"]
                        parent_input += stage_dict[parent_id]["shufflerecordsread"]
                        parent_output_bytes += stage_dict[parent_id]["byteswrite"]
                        parent_output_bytes += stage_dict[parent_id]["shufflebyteswrite"]
                        parent_input_bytes += stage_dict[parent_id]["bytesread"]
                        parent_input_bytes += stage_dict[parent_id]["shufflebytesread"]
                    if parent_output != 0:
                        stage_dict[stage_id]["nominalrate"] = parent_output / (
                            stage_dict[stage_id]["duration"] / 1000.0)
                        stage_dict[stage_id]["nominalrate_bytes"] = parent_input_bytes / (
                            stage_dict[stage_id]["duration"] / 1000.0)
                    elif parent_input != 0:
                        stage_dict[stage_id]["nominalrate"] = parent_input / (
                            stage_dict[stage_id]["duration"] / 1000.0)
                        stage_dict[stage_id]["nominalrate_bytes"] = parent_input_bytes / (
                            stage_dict[stage_id]["duration"] / 1000.0)
                    else:
                        stage_input = stage_dict[stage_id]["recordsread"] + stage_dict[stage_id][
                            "shufflerecordsread"]
                        stage_input_bytes = stage_dict[stage_id]["bytesread"] + stage_dict[stage_id][
                            "shufflebytesread"]
                        if stage_input != 0 and stage_input != stage_dict[stage_id]["numtask"]:
                            stage_dict[stage_id]["nominalrate"] = stage_input / (
                                stage_dict[stage_id]["duration"] / 1000.0)
                            stage_dict[stage_id]["nominalrate_bytes"] = stage_input_bytes / (
                                stage_dict[stage_id]["duration"] / 1000.0)
                        else:
                            stage_output = stage_dict[stage_id]["recordswrite"] + stage_dict[stage_id][
                                "shufflerecordswrite"]
                            stage_output_bytes = stage_dict[stage_id]["byteswrite"] + stage_dict[stage_id][
                                "shufflebyteswrite"]
                            stage_dict[stage_id]["nominalrate"] = stage_input / (
                                stage_dict[stage_id]["duration"] / 1000.0)
                            stage_dict[stage_id]["nominalrate_bytes"] = stage_input_bytes / (
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

            # create output dir
            log_name = os.path.basename(log)

            output_dir = os.path.join(OUTPUT_DIR,
                                      re.sub("[^a-zA-Z0-9.-]",
                                             "_", app_name) +
                                      "_"+log_name.split("-")[1]) if not json_out_dir else json_out_dir
            make_sure_path_exists(output_dir)
            # Create json output
            datagen_strings = ['datagen', 'scheduling-throughput']
            out_filename = 'app_datagen.json' if any(x in app_name.lower() for x in datagen_strings) \
                else re.sub("[^a-zA-Z0-9.-]", "_", app_name)+"_"+log_name.split("-")[1]+ ".json"
            #out_filename = 'app_datagen.json' if any(x in app_name.lower() for x in datagen_strings) else 'app.json'
            print('ROOT_DIR: {}\nAPP_NAME: {}\noutputdir: {}\noutfilename:{}'.format(ROOT_DIR,
                                                                                     app_name,
                                                                                     output_dir,
                                                                                     out_filename))
            with open(os.path.join(output_dir, out_filename), "w") as jsonoutput:
                json.dump(stage_dict, jsonoutput, indent=4, sort_keys=True)
            #os.rename(log, os.path.join(processed_dir, os.path.basename(log_name)))
            os.rename(log, os.path.join(processed_dir, log.split(os.path.sep)[-1]))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        in_dir = out_dir = os.path.abspath(sys.argv[1])
        main(json_out_dir=out_dir, input_dir=in_dir)
    else:
        main(reprocess=False)
