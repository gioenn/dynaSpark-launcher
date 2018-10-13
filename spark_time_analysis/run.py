import datetime
import json
import math
import os
import re
import sys
from functools import reduce
import configparser
import numpy as np
import glob

if __name__ == "__main__":
    import cfg
else:
    from . import cfg

PERCENTILES = [71, 77]


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")


def parse_date_time(d, t):
    """ Convert log time string format 17/05/09 08:17:37.669 into a Python datetime object
    Args:
        d (str): date in format 17/05/09
        t (str): time in format 08:17:37.669
    Returns:
        datetime: datetime object (ignore timezone)
    """
    milliseconds = int(t[9:12]) * 1000 if t[9:12] else 0
    return datetime.datetime(int(d[0:2]),
                             int(d[3:5]),
                             int(d[6:8]),
                             int(t[0:2]),
                             int(t[3:5]),
                             int(t[6:8]),
                             milliseconds)


def parse_spark_logline(logline):
    match = re.search(cfg.SPARK_LOG_PATTERN,  logline)
    if match is None:
       return {
            "timestamp": None,
            "loglevel": '',
            "classname": '',
            "description": ''
            }
    else:
        return {
            "timestamp":parse_date_time(match.group(1), match.group(2)),
            "loglevel": match.group(3),
            "classname": match.group(4),
            "description": match.group(5)
            }


def equals_fields(log, timestamp=None, loglevel=None, classname=None, description=None):
    return (timestamp is None or timestamp == log["timestamp"]) and \
           (loglevel is None or loglevel == log["loglevel"]) and \
           (classname is None or classname == log["classname"]) and \
           (description is None or description in log["description"])


def populate_stage_time_struct(msg, time_struct, job_info, first_ev):
    match_add_taskset = (re.search(cfg.get_log_fields(cfg.ADD_TASKSET_MSG)["descr"],
                                   msg["description"]),
                         "add_taskset")
    match_start_stage = (re.search(cfg.get_log_fields(cfg.START_STAGE_MSG)["descr"],
                                   msg["description"]),
                         "start_taskset")
    match_end_stage = (re.search(cfg.get_log_fields(cfg.END_STAGE_MSG)["descr"],
                                 msg["description"]),
                       "remove_taskset")
    match_end_task = (re.search(cfg.get_log_fields(cfg.END_TASK_MSG)["descr"], msg["description"]))

    match = list(filter(lambda x: x[0] is not None, [match_add_taskset,
                                                     match_start_stage,
                                                     match_end_stage]))
    if len(match) > 0:
        stage_id, label = match[0][0].group(1), match[0][1]
        print("{}\t\t{}-{}".format((msg["timestamp"] - first_ev).total_seconds() * 1000, stage_id, label))
        if stage_id not in time_struct:
            time_struct[stage_id] = {}
        time_struct[stage_id][label] = msg["timestamp"]
#    elif re.search(cfg.get_log_fields(cfg.END_JOB_MSG)["descr"], msg["description"]) is not None:
#        job_info["end_job"] = msg["timestamp"]
    elif match_end_task is not None:
        cur_task_duration = int(match_end_task.group(4))
        cur_stage_id = match_end_task.group(2)
        # First time a task of this stage is detected
        if "sum_of_task_durations_ta_master" not in time_struct[cur_stage_id]:
            time_struct[cur_stage_id]["sum_of_task_durations_ta_master"] = 0
            time_struct[cur_stage_id]["min_task_duration"] = \
                time_struct[cur_stage_id]["t_max_duration"] = cur_task_duration
            time_struct[cur_stage_id]["task_durations"] = []
        time_struct[cur_stage_id]["sum_of_task_durations_ta_master"] += cur_task_duration
        time_struct[cur_stage_id]["min_task_duration"] = min(cur_task_duration,
                                                             time_struct[cur_stage_id]
                                                                        ["min_task_duration"])
        time_struct[cur_stage_id]["t_max_duration"] = max(cur_task_duration,
                                                             time_struct[cur_stage_id]
                                                                        ["t_max_duration"])
        time_struct[cur_stage_id]["task_durations"].append(cur_task_duration)
        job_info["latest_event"] = msg["timestamp"]


def sum_all_stages_by_stats(stages, stat):
    return reduce(lambda x, y: x + y, [z[stat] for z in stages.values()])


def compute_t_task(stages_struct, num_records, num_task):
    """
    computes t_task for all the stages and modifies stages_struct to include it.
    :param stages_struct: data structure containing the
    :param num_records: total number of input records
    :param num_task: number of tasks for each stages (currently uniform)
    """
    reads = {}
    writes = {}
    stage_id_list = [int(x) for x in stages_struct.keys()]
    stage_id_list.sort()
    for i in stage_id_list:  # range(0, len(stages_struct)):
        stage = stages_struct[str(i)]
        stage_id = str(i)
        if len(stage['parent_ids']) == 0:
            if not num_records:
                num_records = stage['actual_records_read']
            reads[stage_id] = num_records
        else:
            reads[stage_id] = 0
            print(stage_id)
            for parent_id in stage['parent_ids']:
                reads[stage_id] += writes[str(parent_id)]
        writes[stage_id] = reads[stage_id] * stage['io_factor']

        if not num_task:
            num_task = stage['num_task']
        stage['t_task_ta_master'] = stage['t_record_ta_master'] * reads[stage_id] / (num_task * stage['s_GQ_ta_master'])
        stage['t_task_ta_executor'] = stage['t_record_ta_executor']*reads[stage_id]/(num_task*stage['s_GQ_ta_executor'])


def main(app_dir, app_name=None, num_records=None, num_tasks=None):
    """
    runs the main time analysis
    :param app_dir: directory where all the
    :param app_name: (optional) custom application name that will be included in the generated file names
    :return: job_time_struct, stage_time_struct, dictionaries containing all the timing information and statistics
    about the whole application and specific to the stages
    """
    stage_time_struct = {}
    job_time_struct = {}

    if app_name is None:
        app_name = app_dir.split(os.sep)[-1]

    # open spark log file
    dat_paths = glob.glob(os.path.join(app_dir, 'app.dat'))
    err_paths = glob.glob(os.path.join(app_dir, 'scala-sort-by-key.err'))
    log_path = dat_paths[0] if dat_paths else err_paths[0]
    print("opening {}...".format(log_path))
    with open(log_path) as log_file:
        spark_log_lines = map(lambda x: parse_spark_logline(x), log_file.readlines())

    # open ta_executor log file
    print("opening {}...".format(app_dir + os.sep + 'app.json'))
    with open(app_dir + os.sep + 'app.json') as stages_file:
        stages = json.load(stages_file)

    # open cfg_clusters.ini
    cfg_clusters = configparser.ConfigParser()
    cfg_clusters.read(os.path.join(app_dir, 'cfg_clusters.ini'))
    # open config.json
    print("opening {}...".format(app_dir + os.sep + 'config.json'))
    with open(app_dir + os.sep + 'config.json') as spark_config_file:
        spark_config = json.load(spark_config_file)
        try:
            max_executors = int(cfg_clusters['main']['max_executors'])
        except KeyError as e:
            print('key not found: {}, getting it from config.json'.format(e))
            max_executors = spark_config['Control']['MaxExecutor']
        job_time_struct["num_cores"] = num_cores = spark_config["Control"]["CoreVM"] * max_executors
        job_time_struct['benchmark_name'] = (spark_config['Benchmark']['Name']).lower().replace("-", "_")
        if spark_config['Benchmark']['Name'] == "PageRank":
            job_time_struct["num_v"] = var_par = spark_config['Benchmark']['Config']['numV']
        elif spark_config['Benchmark']['Name'] == "KMeans":
            job_time_struct["num_of_points"] = var_par = spark_config['Benchmark']['Config']['NUM_OF_POINTS']
        elif spark_config['Benchmark']['Name'] == "scala-sort-by-key":
            job_time_struct['benchmark_name'] = 'sort_by_key'
            job_time_struct["scale_factor"] = var_par = spark_config['Benchmark']['Config']['ScaleFactor']

    # get first event
    first_event = next(i for i in spark_log_lines if i["timestamp"] is not None)["timestamp"]
    print("FIRST EVENT", first_event)
    job_time_struct['latest_event'] = first_event

    # extract stage-specific times
    for i in spark_log_lines:
        populate_stage_time_struct(i, stage_time_struct, job_time_struct, first_event)

    # calculate stage-specific statistics
    for s, t in stage_time_struct.items():
        if s != "job_time_struct":

            # TODO: avoid replication of fields between profiling and time_analysis. Also, remove unnecessary measures
            t["id"] = s
            t["parent_ids"] = stages[s]["parentsIds"]
            t["actual_records_read"] = stages[s]["actual_records_read"]
            t["io_factor"] = stages[s]["io_factor"]
            t["num_task"] = stages[s]["numtask"]
            t["skipped"] = stages[s]["skipped"]
            t["t_record_ta_executor"] = stages[s]["t_record_ta_executor"]
            # calculate variance using a list comprehension
            t['t_mean'] = np.mean(t["task_durations"])
            t["t_variance"] = np.var(t["task_durations"])
            t["t_std_dev"] = np.std(t["task_durations"])
            for p in PERCENTILES:
                t["t_percentile"+str(p)] = np.percentile(t["task_durations"], p)
            t["t_2sigma"] = np.mean(t["task_durations"]) + 2 * np.std(t["task_durations"])
            # t.pop("task_durations")
            num_batches = math.ceil(stages[s]["numtask"] / num_cores)

            t["add_to_end_taskset"] = (t["remove_taskset"] - t["add_taskset"]).total_seconds() * 1000
            t["start_to_end_taskset"] = (t["remove_taskset"] - t["start_taskset"]).total_seconds() * 1000
            t["add_to_start_taskset_overhead"] = (t["start_taskset"] - t["add_taskset"]).total_seconds() * 1000
            t["sum_of_task_durations_ta_executor"] = stages[s]["duration"]

            t["t_avg_duration_ta_executor"] = stages[s]["duration"]/stages[s]["numtask"]
            t["t_avg_duration_ta_master"] = t["sum_of_task_durations_ta_master"] / stages[s]["numtask"]

            t["s_avg_duration_ta_executor"] = t["t_avg_duration_ta_executor"] * num_batches
            t["s_avg_duration_ta_master"] = t["t_avg_duration_ta_master"] * num_batches
            t["s_duration_w_slowest_task"] = t["t_max_duration"] * num_batches
            t["s_mean_plus_std_dev_stage_duration"] = (t["t_mean"] + t["t_std_dev"]) * num_batches
            for p in PERCENTILES:
                t["s_percentile"+str(p)] = t["t_percentile"+str(p)] * num_batches

            diff_sum_of_task_durations = float(t["sum_of_task_durations_ta_master"] - t["sum_of_task_durations_ta_executor"])
            diff_avg_task_duration = float(t["t_avg_duration_ta_master"] - t["t_avg_duration_ta_executor"])
            diff_avg_stage_duration = float(t["s_avg_duration_ta_master"] - t["s_avg_duration_ta_executor"])

            t["s_GQ_ta_master"] = t["sum_of_task_durations_ta_master"] / num_cores / t["add_to_end_taskset"]
            t["s_GQ_ta_executor"] = t["sum_of_task_durations_ta_executor"] / num_cores / t["add_to_end_taskset"]
            t["t_record_ta_master"] = t["sum_of_task_durations_ta_master"] / stages[s]["actual_records_read"]

            '''
            print(""" STAGE {} \t({}):
                  SUM_OF_TASK_DURATIONS:
                   - ta_executor:\t{}
                   - ta_master:\t{}
                   - diff:\t\t{}\t(+{:.2f} %)
                  AVG_TASK_DURATION:
                   - ta_executor:\t{}
                   - ta_master:\t{}
                   - diff:\t\t{}
                  AVG_STAGE_DURATION:
                   - ta_executor:\t{}
                   - ta_master:\t{}
                   - diff:\t\t{}
                  add_to_end_taskset:\t\t{}
                  start_to_end_taskset:\t\t{}
                  add_to_start_taskset_overhead:\t{}\t({:.2f} %)
                  """.format(s,
                        stages[s]["name"],
                        t["sum_of_task_durations_ta_executor"],
                        t["sum_of_task_durations_ta_master"],
                        diff_sum_of_task_durations,
                        diff_sum_of_task_durations/t["sum_of_task_durations_ta_executor"]*100,
                        t["t_avg_duration_ta_executor"],
                        t["t_avg_duration_ta_master"],
                        diff_avg_task_duration,
                        t["s_avg_duration_ta_executor"],
                        t["s_avg_duration_ta_master"],
                        diff_avg_stage_duration,
                        t["add_to_end_taskset"],
                        t["start_to_end_taskset"],
                        t["add_to_start_taskset_overhead"],
                        t["add_to_start_taskset_overhead"]/t["add_to_end_taskset"]*100))
            '''

    add_to_start_taskset_overhead = reduce(lambda x, y: x + y,
                                           [z["add_to_start_taskset_overhead"] for z in stage_time_struct.values()])
    total_start_to_end_taskset = reduce(lambda x, y: x + y,
                                        [z["start_to_end_taskset"] for z in stage_time_struct.values()])
    total_add_to_end_taskset = reduce(lambda x, y: x + y, [z["add_to_end_taskset"] for z in stage_time_struct.values()])
    total_ta_executor_stages = sum_all_stages_by_stats(stage_time_struct, "s_avg_duration_ta_executor")
    total_ta_master_stages = reduce(lambda x, y: x + y,
                                    [z["s_avg_duration_ta_master"] for z in stage_time_struct.values()])
    total_slowest_task_stages = reduce(lambda x, y: x + y,
                                       [z["s_duration_w_slowest_task"] for z in stage_time_struct.values()])
    total_mean_plus_stddev_stages = sum_all_stages_by_stats(stage_time_struct, "s_mean_plus_std_dev_stage_duration")
    total_monocore_ta_master_stages = sum_all_stages_by_stats(stage_time_struct, "sum_of_task_durations_ta_master")
    total_monocore_ta_executor_stages = sum_all_stages_by_stats(stage_time_struct, "sum_of_task_durations_ta_executor")



    job_time_struct["start_job"] = stage_time_struct['0']["start_taskset"]
    job_duration = (job_time_struct["latest_event"] - job_time_struct["start_job"]).total_seconds() * 1000
    job_time_struct["total_add_to_start_taskset_overhead"] = add_to_start_taskset_overhead
    job_time_struct["total_start_to_end_taskset"] = total_start_to_end_taskset
    job_time_struct["total_add_to_end_taskset"] = total_add_to_end_taskset
    job_time_struct["total_ta_executor_stages"] = total_ta_executor_stages
    job_time_struct["total_ta_master_stages"] = total_ta_master_stages
    job_time_struct["total_slowest_task_stages"] = total_slowest_task_stages
    job_time_struct["actual_job_duration"] = job_duration
    job_time_struct["total_mean_plus_stddev_stages"] = total_mean_plus_stddev_stages
    job_time_struct["total_monocore_ta_executor_stages"] = total_monocore_ta_executor_stages
    job_time_struct["total_monocore_ta_master_stages"] = total_monocore_ta_master_stages
    job_time_struct["total_overhead_monocore"] = total_monocore_ta_master_stages - total_monocore_ta_executor_stages

    for p in PERCENTILES:
        job_time_struct["total_percentile"+str(p)] = sum_all_stages_by_stats(stage_time_struct, "s_percentile"+str(p))

    job_time_struct["GQ_executor"] = (total_monocore_ta_executor_stages / job_duration) / num_cores
    job_time_struct["GQ_master"] = (total_monocore_ta_master_stages / job_duration) / num_cores

    compute_t_task(stage_time_struct, num_records, num_tasks)


    # end of analysis: only prints and file generation below
    '''
    print("TOTAL TASKS_ONLY:\t{}".format(total_start_to_end_taskset))
    print("TOTAL OVERHEAD:\t\t{}\t({:.2f} %)".format(add_to_start_taskset_overhead, add_to_start_taskset_overhead / total_add_to_end_taskset * 100))
    print("TOT WITH OVERHEAD:\t{}".format(total_add_to_end_taskset))
    print("JOB DURATION:\t\t{}".format(job_duration))
    print("TOTAL TA_EXECUTOR:\t\t{} - {}".format(stages['0']['totalduration']/num_cores, total_ta_executor_stages))
    '''

    SPARK_CONTEXT = {
        "app_name" : "{}_c{}_t{}_{}l_d{}_tc_{}_n_rounds_{}".format(app_name,
                                                                   num_cores,
                                                                   cfg.TIME_BOUND,
                                            "no_" if cfg.NO_LOOPS else "",
                                                                   job_duration,
                                            "parametric" if cfg.PARAMETRIC_TC else "by20",
                                            "by2"),
    #        "app_dir_acceleration_0_1000_c48_t40_no-l_d133000_tc_parametric_forall_nrounds_TEST",
        "verification_params" :
        {
            "plugin": cfg.PLUGIN,
            "time_bound" : cfg.TIME_BOUND,
            "parametric_tc": cfg.PARAMETRIC_TC,
            "no_loops" : cfg.NO_LOOPS
        },
        "tot_cores" : num_cores,
        "analysis_type" : "feasibility",
        "deadline" : job_duration,
        "max_time" : job_duration,
        "tolerance": cfg.TOLERANCE,
        "stages": stages
    }

    # save spark_context file
    out_path_context = app_dir+os.sep+app_name+'_context.json'
    print("dumping to {}".format(out_path_context))
    with open(out_path_context, 'w') as outfile:
        json.dump(SPARK_CONTEXT, outfile, indent=4, sort_keys=True)

    # save time analysis file
    out_path_time_structs = app_dir+os.sep+app_name+'_time_analysis.json'
    print("dumping to {}".format(out_path_time_structs))
    with open(out_path_time_structs, 'w') as outfile:
        json.dump({"stages": stage_time_struct, "job": job_time_struct},
                  outfile, indent=4, sort_keys=True, default=json_serial)


    print("{}, {}, {}, {}\n".format(
        var_par, job_duration, total_ta_executor_stages, total_ta_master_stages
    ))
    return job_time_struct, stage_time_struct


def modify_executors(app_dir, executors, app_name=None):
    if app_name is None:
        app_name = app_dir.split(os.sep)[-1]
    print("opening {}...".format(app_dir + os.sep + 'config.json'))
    with open(app_dir + os.sep + 'config.json', 'r') as spark_config_file:
        spark_config = json.load(spark_config_file)
        spark_config["Control"]["MaxExecutor"] = executors
    with open(app_dir + os.sep + 'config.json', 'w') as spark_config_file:
        json.dump(spark_config, spark_config_file, indent=4, sort_keys=True)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_app_dir = sys.argv[1].strip(os.sep)
        input_app_name = sys.argv[2] if len(sys.argv) > 2 else input_app_dir.split(os.sep)[-1]
    else:
        print('ERROR: You must provide at least one argument')
        sys.exit(0)
    main(app_dir=input_app_dir, app_name=input_app_name)
