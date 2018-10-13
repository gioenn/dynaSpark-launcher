import re

# 17/05/09 08:17:37.669 INFO SparkContext: Running Spark version 2.0.3-SNAPSHOT

# 17/05/23 08:35:25.379 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, 10.0.0.12, partition 0, ANY, 5860 bytes)
## START STAGE
# 17/05/23 08:36:00.792 INFO TaskSchedulerImpl: Adding task set 7.0 with 64 tasks
# 17/05/23 08:36:00.822 INFO TaskSetManager: Starting task 0.0 in stage 7.0 (TID 448, 10.0.0.12, partition 0, PROCESS_LOCAL, 6402 bytes)
## END STAGE
# 17/05/23 08:36:03.752 INFO TaskSchedulerImpl: Removed TaskSet 13.0, whose tasks have all completed, from pool


SPARK_LOG_PATTERN = '^(\d{2}/\d{2}/\d{2}) (\d{2}\:\d{2}:\d{2}(?:\.\d+)?) (\S+) (\S+): (.+)'
REDUCED_PATTERN = '(\S+) (\S+): (.+)'

START_JOB_MSG = "INFO TaskSetManager: Starting task 0.0 in stage 0.0"
END_JOB_MSG = "INFO SparkUI: Stopped Spark web"

ADD_TASKSET_MSG = "INFO TaskSchedulerImpl: Adding task set (\d+).\d* with (\d+) tasks"
START_STAGE_MSG = "INFO TaskSetManager: Starting task 0.0 in stage (\d+).0"
END_STAGE_MSG = "INFO TaskSchedulerImpl: Removed TaskSet (\d+)"

START_TASK_MSG = "INFO TaskSetManager: Starting task (\d+).0 in stage (\d+).0 \(TID (\d+)"
END_TASK_MSG = "INFO TaskSetManager: Finished task (\d+).0 in stage (\d+).0 \(TID (\d+)\) in (\d+) ms"

start_match = re.search(REDUCED_PATTERN, START_JOB_MSG)
START_LOG_LEVEL = start_match.group(1)
START_CLASS = start_match.group(2)
START_DESCR = start_match.group(3)

end_match = re.search(REDUCED_PATTERN, END_JOB_MSG)
END_LOG_LEVEL = end_match.group(1)
END_CLASS = end_match.group(2)
END_DESCR = end_match.group(3)


def get_log_fields(log_msg):
    match = re.search(REDUCED_PATTERN, log_msg)
    return {
        "log_level" : match.group(1),
        "class" : match.group(2),
        "descr" : match.group(3)
    }


# zot params
PLUGIN = "ae2sbvzot"
TIME_BOUND = 100
PARAMETRIC_TC = False
NO_LOOPS = True
ANALYSIS_TYPE = "feasibility"
TOLERANCE = 0.1