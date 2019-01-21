#
#
# parent = "./results/OK/"
# for folder in [parent + d for d in os.listdir(parent) if os.path.isdir(os.path.join(parent, d))]:
#     for fold in [folder + "/"+  d for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]:
#         if fold.split("/")[-1] == "Native":
#             for app in [fold + "/" + d for d in os.listdir(fold) if os.path.isdir(os.path.join(fold, d))]:
#                 import metrics
#                 metrics.compute_metrics(app)


import metrics

metrics.compute_metrics(
    "./results/OK/aggregate-by-key/Native/spark_perf_output__2016-09-15_16-06-47_logs")
