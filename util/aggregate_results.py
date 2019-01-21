import glob

import matplotlib.pyplot as plt

folder = "./results/OK/PageRank/"


def compute_cpu_time(folder):
    cpu_time_native = []
    cpu_time_0 = []
    cpu_time_20 = []
    cpu_time_40 = []

    for fold in glob.glob(folder + "/Native/*"):
        print(fold + "/CPU_TIME.txt")
        with open(fold + "/CPU_TIME.txt") as f:
            for line in f.readlines():
                cpu_time_native.append(float(line.split(" ")[-1]))

    for fold in glob.glob(folder + "/0%/*"):
        print(fold + "/CPU_TIME.txt")
        with open(fold + "/CPU_TIME.txt") as f:
            line = f.readline()
            print(line.split(" "))
            cpu_time_0.append(float(line.split(" ")[-1].replace("\n", "")))

    for fold in glob.glob(folder + "/20%/*"):
        print(fold + "/CPU_TIME.txt")
        with open(fold + "/CPU_TIME.txt") as f:
            line = f.readline()
            print(line.split(" "))
            cpu_time_20.append(float(line.split(" ")[-1].replace("\n", "")))

    for fold in glob.glob(folder + "/40%/*"):
        print(fold + "/CPU_TIME.txt")
        with open(fold + "/CPU_TIME.txt") as f:
            line = f.readline()
            print(line.split(" "))
            cpu_time_40.append(float(line.split(" ")[-1].replace("\n", "")))

    data = [cpu_time_native, cpu_time_0, cpu_time_20, cpu_time_40]
    plt.figure()
    plt.boxplot(data, showmeans=True,
                labels=["Native", "Base Deadline", "+20% Deadline", "+40% Deadline"])
    plt.title("CPU TIME")
    plt.savefig(folder + "/boxplot-cputime.png", bbox_inches='tight', dpi=300)
    plt.close()


def compute_error(folder):
    error_0 = []
    error_0_mean = []
    error_0_median = []
    error_0_max = []
    error_0_min = []
    error_20 = []
    error_20_mean = []
    error_20_median = []
    error_20_max = []
    error_20_min = []
    error_40 = []
    error_40_mean = []
    error_40_median = []
    error_40_max = []
    error_40_min = []

    for fold in glob.glob(folder + "/0%/*"):
        print(fold + "/ERROR.txt")
        with open(fold + "/ERROR.txt") as f:
            for line in f:
                l = line.split(" ")
                err = float(l[-1].replace("\n", ""))
                if l[0] == "MEDIAN_ERROR:":
                    error_0_median.append(err)
                if l[0] == "DEADLINE_ERROR":
                    error_0.append(err)
                if l[0] == "MEAN_ERROR":
                    error_0_mean.append(err)
                if l[0] == "MAX_ERROR:":
                    error_0_max.append(err)
                if l[0] == "MIN_ERROR:":
                    error_0_min.append(err)

    for fold in glob.glob(folder + "/20%/*"):
        print(fold + "/ERROR.txt")
        with open(fold + "/ERROR.txt") as f:
            for line in f:
                l = line.split(" ")
                err = float(l[-1].replace("\n", ""))
                if l[0] == "MEDIAN_ERROR:":
                    error_20_median.append(err)
                if l[0] == "DEADLINE_ERROR":
                    error_20.append(err)
                if l[0] == "MEAN_ERROR":
                    error_20_mean.append(err)
                if l[0] == "MAX_ERROR:":
                    error_20_max.append(err)
                if l[0] == "MIN_ERROR:":
                    error_20_min.append(err)

    for fold in glob.glob(folder + "/40%/*"):
        print(fold + "/ERROR.txt")
        with open(fold + "/ERROR.txt") as f:
            for line in f:
                l = line.split(" ")
                err = float(l[-1].replace("\n", ""))
                if l[0] == "MEDIAN_ERROR:":
                    error_40_median.append(err)
                if l[0] == "DEADLINE_ERROR":
                    error_40.append(err)
                if l[0] == "MEAN_ERROR":
                    error_40_mean.append(err)
                if l[0] == "MAX_ERROR:":
                    error_40_max.append(err)
                if l[0] == "MIN_ERROR:":
                    error_40_min.append(err)

    label = ["Base Deadline", "+20% Deadline", "+40% Deadline"]
    data = [error_0, error_20, error_40]
    plt.figure()
    plt.boxplot(data, showmeans=True, labels=label)
    plt.title("Error Deadline APP")
    plt.savefig(folder + "/boxplot-error-app-deadline.png", bbox_inches='tight', dpi=300)
    plt.close()
    data = [error_0_mean, error_20_mean, error_40_mean]
    plt.figure()
    plt.boxplot(data, showmeans=True, labels=label)
    plt.title("Mean Error DL Stages")
    plt.savefig(folder + "/boxplot-mean-error-dl-stage.png", bbox_inches='tight', dpi=300)
    plt.close()
    data = [error_0_median, error_20_median, error_40_median]
    plt.figure()
    plt.boxplot(data, showmeans=True, labels=label)
    plt.title("Median Error DL Stages")
    plt.savefig(folder + "/boxplot-median-error-dl-stage.png", bbox_inches='tight', dpi=300)
    plt.close()
    data = [error_0_max, error_20_max, error_40_max]
    plt.figure()
    plt.boxplot(data, showmeans=True, labels=label)
    plt.title("Max Error DL Stages")
    plt.savefig(folder + "/boxplot-max-error-dl-stage.png", bbox_inches='tight', dpi=300)
    plt.close()
    data = [error_0_min, error_20_min, error_40_min]
    plt.figure()
    plt.boxplot(data, showmeans=True, labels=label)
    plt.title("Min Error DL Stages")
    plt.savefig(folder + "/boxplot-min-error-dl-stage.png", bbox_inches='tight', dpi=300)
    plt.close()


compute_cpu_time(folder)
compute_error(folder)
