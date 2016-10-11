# 16/10/03 12:31:54 INFO ControllerJob: NOMINAL RECORD/S STAGE ID 1 : 3671965.1777167222


import glob
import numpy as np
import matplotlib.pyplot as plt

def mean_nominalrate(fold):
    stageDict = {}
    count = 0
    print(fold)
    for folder in glob.glob(fold+"/*"):
        print(folder)
        for file in glob.glob(folder+"/*.dat"):
            with open(file) as f:
                print(file)
                count += 1
                for line in f:
                    l = line.split(" ")
                    if len(l) > 4 and l[4] == "NOMINAL" and l[5] == "RECORD/S":
                        try:
                            print(l [-3], float(l[-1]))
                            stageDict[l[-3]]["nominalrate"].append(float(l[-1]))
                        except KeyError:
                            stageDict[l[-3]] = {}
                            stageDict[l[-3]]["nominalrate"] = []
                            stageDict[l[-3]]["nominalrate"].append(float(l[-1]))


    print(stageDict)
    with open(fold + "/result.txt", "w") as out:
        for id in sorted(stageDict.keys()):
            out.write("SID " + str(id) + " MEAN " + str(np.mean(stageDict[id]["nominalrate"])) + "\n")
            out.write("SID " + str(id) + " STD " + str(np.std(stageDict[id]["nominalrate"]))+ "\n")
            out.write("SID " + str(id) + " MIN " + str(np.min(stageDict[id]["nominalrate"])) + "\n")
            out.write("SID " + str(id) + " MAX " + str(np.max(stageDict[id]["nominalrate"])) + "\n")
            out.write("\n")
            out.write("SID " + str(id) + " % " + str((np.std(stageDict[id]["nominalrate"]) / np.mean(stageDict[id]["nominalrate"]))*100) + "\n")
            out.write("\n")
            stageDict[id]["nominalrate"] = np.mean(stageDict[id]["nominalrate"])

    print(stageDict)

import os
for dirpath, dirnames, files in os.walk("./results/OK/PageRank/Profiling-NominalRate/"):
    for dir in dirnames:
        mean_nominalrate(dirpath + "/" + dir)
    break

nominalrate_dict = {}
mini_dict = {}
max_dict = {}
mean_dict = {}
for dirpath, dirnames, files in os.walk("./results/OK/PageRank/Profiling-NominalRate/"):
    for dir in dirnames:
        nominalrate_dict[dir] = {}
        mini_dict[dir] = {}
        max_dict[dir] = {}
        mean_dict[dir] = {}
        with open(dirpath +"/"+ dir + "/result.txt") as f:
            # SID 13 % 19.7066924684
            for line in f:
                l = line.split(" ")
                if len(l) > 1 and l[-2] == "%":
                    nominalrate_dict[dir][int(l[1])] = float(l[-1].replace("\n", ""))
                if len(l) > 1 and l[-2] == "MIN":
                    mini_dict[dir][int(l[1])] = float(l[-1].replace("\n", ""))
                if len(l) > 1 and l[-2] == "MAX":
                    max_dict[dir][int(l[1])] = float(l[-1].replace("\n", ""))
                if len(l) > 1 and l[-2] == "MEAN":
                    mean_dict[dir][int(l[1])] = float(l[-1].replace("\n", ""))
    break

print(nominalrate_dict)


for dirpath, dirnames, files in os.walk("./results/OK/PageRank/Profiling-NominalRate/"):
    for dir in dirnames:
        data = []
        for key in nominalrate_dict[dir].keys():
            data.append((key, nominalrate_dict[dir][key]))
        data.sort(key=lambda x: x[0])
        print(data)
        plt.plot([x[0] for x in data], [x[1] for x in data])
    plt.legend(dirnames)
    break
plt.ylabel('% std dev')
plt.xlabel("Stage ID")
plt.savefig("./results/OK/PageRank/Profiling-NominalRate/result.png")
plt.close()

for dirpath, dirnames, files in os.walk("./results/OK/PageRank/Profiling-NominalRate/"):
    legend = []
    for dir in dirnames:
        if dir == "6EXEC-NOHDFS" or dir == "12EXEC-NOHDFS":
            legend.append(dir)
            data = []
            for key in mini_dict[dir].keys():
                data.append((key, mini_dict[dir][key]))
            data.sort(key=lambda x: x[0])
            print(data)
    plt.legend(legend)
    break
plt.ylabel('MIN DIFF')
plt.xlabel("Stage ID")
plt.savefig("./results/OK/PageRank/Profiling-NominalRate/min.png")
plt.close()


A = "6EXEC-6DATANODE"
B = "12EXEC-6DATANODE"
print("\nMIN")
for key in mini_dict[A].keys():
    diff = mini_dict[A][key] - mini_dict[B][key]
    print(str(key) + " " + str(diff / min(mini_dict[A][key], mini_dict[B][key])* 100))

print("\nMEAN")
for key in mean_dict[A].keys():
    diff = mean_dict[A][key] - mean_dict[B][key]
    print(str(key) + " " + str(diff / min(mean_dict[A][key], mean_dict[B][key])* 100))

print("\nMAX")
for key in max_dict[A].keys():
    diff = max_dict[A][key] - max_dict[B][key]
    print(str(key) + " " + str(diff / min(max_dict[A][key], max_dict[B][key])* 100))