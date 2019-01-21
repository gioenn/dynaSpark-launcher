import glob
import json
import re
from collections import OrderedDict
import os

def join_dags(path):
    json_index = 0
    app_name = ""
    joint_json = OrderedDict()
    #print(glob.glob(os.path.join(path, "*[0-9].json")))
    #for json_file in [f for f in glob.glob(os.path.join(path, "*.json")) if not f.contains('çollection.json')]:
    #for json_file in set(glob.glob(os.path.join(path, "*.json"))) - set(glob.glob(os.path.join(path, "*_collection.json"))):
                      #if not '_çollection.json' in f]:
                      #if not str(f.split(os.sep)[-1].split("-")[-1].split('.')[0].split('_')[-1]) == 'çollection']:
    for json_file in glob.glob(os.path.join(path, "*[0-9].json")):
        app_name = json_file.split(os.sep)[-1].split("-")[0]
        json_index = json_file.split(".")[0].split("-")[-1]
        file_open = open(json_file)
        print("json_file: ", json_file)
        with file_open as jsonfile:
            data = json.load(jsonfile)
            joint_json[json_index] = data

    print("app_name: ", app_name)
    with open(os.path.join(path, app_name + ".json"), "w") as jsonoutput:
        json.dump(joint_json, jsonoutput, indent=4, sort_keys=True)


if __name__ == "__main__":
     # join_dags(sys.argv[1], sys.argv[2])
     join_dags("spark_log_profiling/avg_json")