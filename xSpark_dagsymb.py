#from config import PROVIDER, NUM_INSTANCE, NUM_RUN, CLUSTER_ID, TERMINATE, RUN, REBOOT, CLUSTER_MAP, VAR_PAR_MAP, \
#                  PROCESS_ON_SERVER
#import config as c
from configure import config_instance as c
import libcloud.common.base
import argparse
import sys

from spark_log_profiling import processing as profiling
from spark_log_profiling import average_runs, join_jsons
from spark_time_analysis import run as run_ta
from colors import header, okblue, okgreen, warning, underline, bold, fail
#from join_jsons import join_dags
import util.utils as utils
from spark_log_profiling.average_runs import OUTPUT_DIR

from factory_methods import BenchInstanceFactory
import argcomplete
import json
import os
from pathlib import Path
libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True

def run_xspark(current_cluster, num_instance=c.NUM_INSTANCE, num_run=c.NUM_RUN, cluster_id=c.CLUSTER_ID, terminate=c.TERMINATE,
               run=c.RUN, reboot=c.REBOOT, assume_yes=False):
    """ Main function;
    * Launch NUMINSTANCE virtual machines
    * Run DagSymb Application
    * Download Log
    * Plot data from log
    """
    print(header('run_xspark(num_instance={}, num_run={}, cluster_id={},terminate={}, run={}, reboot={})'
          .format(num_instance, num_run, cluster_id, terminate, run, reboot)))
    ''' get cfg_file and initialize main settings'''
    with utils.open_cfg(mode='w') as cfg:
        if 'main' not in cfg:
            cfg['main'] = {}
        cfg.set('main', 'current_cluster', current_cluster)
        if 'tool_on_master' not in cfg['main']:
            cfg.set('main', 'tool_on_master', 'false')

    bench_instance = BenchInstanceFactory.get_bench_instance(c.PROVIDER, cluster_id)
    setup_ok = True

    if num_instance > 0:
        setup_ok = bench_instance.setup(num_instance, assume_yes)

    if reboot:
        bench_instance.reboot()

    if setup_ok and run:
        bench_instance.run(num_run)

    if terminate:
        bench_instance.terminate()

def deploy_profile(bench, cluster_id=c.CLUSTER_ID):
    """ Main function;
    * Uploads profile to xSpark 
    * configuration directory
    * on spark master
    """
    #moved to bench.upload_profile
    #cfg = utils.get_cfg()
    #profile_fname = cfg[bench][profile_name] + 'json'
    bench_instance = BenchInstanceFactory.get_bench_instance(c.PROVIDER, cluster_id)
    #bench_instance.upload_profile(profile_fname)
    bench_instance.upload_profile()

def deploy_meta_profile(meta_profile_name, cluster_id=c.CLUSTER_ID, overwrite=False ):
    """ Main function;
    * Uploads profile to xSpark 
    * configuration directory
    * on spark master
    """
    #moved to bench.upload_profile
    #cfg = utils.get_cfg()
    #profile_fname = cfg[bench][profile_name] + 'json'
    bench_instance = BenchInstanceFactory.get_bench_instance(c.PROVIDER, cluster_id)
    #bench_instance.upload_profile(profile_fname)
    bench_instance.upload_meta_profile(meta_profile_name, overwrite=True)

def setup_cluster(cluster, num_instances, assume_yes):
    # termporary structure to save run configuration
    # TODO: IMPROVE THIS
    run_on_setup = {
        'spark': 1,
        'hdfs' : 1,
        'generic': 0
    }
    cluster_id = c.CLUSTER_MAP[cluster]
    print(bold('Setup {} with {} instances...'.format(cluster_id, num_instances)))
    c.NUM_INSTANCE = num_instances
    run_xspark(current_cluster=cluster, num_instance=num_instances, cluster_id=cluster_id,
               run=run_on_setup[cluster], terminate=0, reboot=0, assume_yes=assume_yes)


def kill_cluster(cluster):
    cluster_id = c.CLUSTER_MAP[cluster]
    print(bold('Terminate {}...'.format(cluster_id)))
    run_xspark(current_cluster=cluster, num_instance=0, cluster_id=cluster_id, run=0, terminate=1, reboot=0)
    with utils.open_cfg(mode='w') as cfg:
        cfg[cluster] = {}


def run_log_profiling(local):
    out_dir = None
    in_dir = None
    cfg = utils.get_cfg()
    if not local:
        if 'out_folders' in cfg:
            for x in cfg['out_folders']:
                in_dir = out_dir = cfg['out_folders'][x]
                profiling.main(input_dir=in_dir, json_out_dir=out_dir)
    else:
        profiling.main(input_dir=in_dir, json_out_dir=out_dir)
        #profiling.main()

def run_time_analysis(input_dir):
    if not input_dir:
        cfg = utils.get_cfg()
        if 'out_folders' in cfg:
            for x in cfg['out_folders']:
                input_dir  = cfg['out_folders'][x]
                run_ta.main(input_dir)
    else:
        run_ta.main(input_dir)


def run_check_cluster(cluster):
    cluster_id = c.CLUSTER_MAP[cluster]
    bench_instance = BenchInstanceFactory.get_bench_instance(c.PROVIDER, cluster_id)
    print(bench_instance.retrieve_nodes())
    print([(x.name, x.state) for x in bench_instance.nodes])


def setup(args):
    cluster = args.cluster
    num_instances = args.num_instances
    assume_yes = args.assume_yes
    with utils.open_cfg(mode='w') as cfg:
        if cluster == 'all' or cluster == 'hdfs':
            for s in cfg.sections():
                cfg.remove_section(s)
        if 'main' not in cfg:
            cfg['main'] = {}
        cfg.set('main', 'setup', 'true')
        if args.app_dir:
            cfg.set('main', 'appdir', args.app_dir)
    if cluster == 'all':
        setup_cluster('hdfs', num_instances, assume_yes)
        setup_cluster('spark', num_instances, assume_yes)
    else:
        setup_cluster(cluster, num_instances, assume_yes)

def setup_application_agnostic(args):
    cluster = args.cluster
    num_instances = args.num_instances
    assume_yes = args.assume_yes
    if cluster == 'all' or cluster == 'hdfs':
        with utils.open_cfg(mode='w') as cfg:
            for s in cfg.sections():
                cfg.remove_section(s)
    if cluster == 'all':
        setup_cluster('hdfs', num_instances, assume_yes)
        setup_cluster('spark', num_instances, assume_yes)
    else:
        setup_cluster(cluster, num_instances, assume_yes)

def profile(args):
    cluster_id = c.CLUSTER_MAP['spark']
    num_run = args.num_runs
    reuse_dataset = args.reuse_dataset
    #exp_filepath = args.exp_file_path if args.exp_file_path else "experiment.json"
    exp_filepaths = args.exp_file_paths if args.exp_file_paths else ["experiment.json"]
    num_experiments = len(exp_filepaths)
    spark_seq = args.spark_seq if args.spark_seq else False
    index = 0
    for exp_filepath in exp_filepaths:
        exp_file = Path(exp_filepath)
        index += 1
        if exp_file.exists():
            experiment = json.load(open(exp_filepath))
            try:
                benchmark = experiment["BenchmarkName"]
                #benchmark = experiment["BenchmarkBench"][0]
            except KeyError as error:
                print("ERROR:  {} in experiment file: {}".format(error, exp_filepath))
                exit(1) 
        with utils.open_cfg(mode='w') as cfg:
            for s in cfg.sections():
                if s != 'hdfs':
                    cfg.remove_section(s)
            cfg['main'] = {}
            cfg['main']['tool_on_master'] = 'false'
            cfg['main']['experiment_file'] = exp_filepath
            cfg['main']['process_on_server'] = str(c.PROCESS_ON_SERVER)
            cfg['main']['iter_num'] = str(1) #vboxvm
            cfg['main']['num_experiments'] = str(num_experiments)
            cfg['main']['experiment_num'] = str(index)
            #cfg['main']['cluster_id'] = cluster_id
            cfg['profile'] = {}
            cfg['profile']['spark_seq'] = str(spark_seq)
            cfg[benchmark] = {}
            cfg[benchmark]['profile_name'] = '{}'.format(c.VAR_PAR_MAP[benchmark]['profile_name'])
            if reuse_dataset:
                cfg['main']['delete_hdfs'] = str(not reuse_dataset)
            
        print(bold('Profile experiment {} performing {} runs for benchmark {} on cluster {}'.format(exp_filepath, 
                                                                                                   num_run, benchmark,
                                                                                                   cluster_id,)))
        run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
                   cluster_id=cluster_id, run=1, terminate=0, reboot=0)
        if not c.PROCESS_ON_SERVER:
            average_runs.main(profile_name=utils.get_cfg()[benchmark]['profile_name'])
            deploy_profile(benchmark, cluster_id)
    # raise NotImplementedError()

def profile_symex(args):
    cluster_id = c.CLUSTER_MAP['spark']
    num_run = args.num_runs
    reuse_dataset = args.reuse_dataset
    #exp_filepath = args.exp_file_path if args.exp_file_path else "experiment.json"
    exp_filepaths = args.exp_file_paths if args.exp_file_paths else ["experiment.json"]
    num_experiments = len(exp_filepaths)
    spark_seq = args.spark_seq if args.spark_seq else False
    index = 0
    app_name = ''
    app_jar = ''
    app_class = ''
    guard_evaluator_class = ''
    num_partitions = ''
    app_args = {}
    meta_profile_name = ''
    for exp_filepath in exp_filepaths:
        exp_file = Path(exp_filepath)
        index += 1
        if exp_file.exists():
            experiment = json.load(open(exp_filepath))
            try:
                app_name = experiment["AppName"]
                app_jar = experiment["AppJar"]
                app_class = experiment["AppClass"]
                guard_evaluator_class = experiment["GuardEvaluatorClass"]
                num_partitions = experiment["NumPartitions"]
                app_args = experiment["AppConf"]
                data_multiplier = experiment["DataMultiplier"] if experiment["DataMultiplier"] else 1
                meta_profile_name = experiment["MetaProfileName"] if experiment["MetaProfileName"] else meta_profile_name
            except KeyError as error:
                print("ERROR:  {} in experiment file: {}".format(error, exp_filepath))
                exit(1) 
        with utils.open_cfg(mode='w') as cfg:
            for s in cfg.sections():
                if s != 'hdfs':
                    cfg.remove_section(s)
            cfg['main'] = {}
            cfg['main']['app_name'] = app_name
            cfg['main']['app_jar'] = app_jar
            cfg['main']['app_class'] = app_class
            cfg['main']['guard_evaluator_class'] = guard_evaluator_class
            cfg['main']['tool_on_master'] = 'false'
            cfg['main']['experiment_file'] = exp_filepath
            cfg['main']['process_on_server'] = str(c.PROCESS_ON_SERVER)
            cfg['experiment'] = {}
            cfg['experiment']['app_name'] = app_name
            cfg['experiment']['profile_name'] = app_name
            cfg['experiment']['meta_profile_name'] = meta_profile_name
            cfg['app_args'] = {}
            arg_string = ''
            not_to_scale_args = ["pastMonths", "inputFile", "outputFile", "delimiter", "parallelism", "minimumCompressionProgress", "progressCounter"]
            for key_app_arg in sorted(app_args.keys(), key=lambda k: int(k)):
                app_arg_name = '{}'.format(app_args[key_app_arg]["Name"])
                app_arg_val = '{}'.format(app_args[key_app_arg]["Value"]) 
                app_arg_value = app_arg_val if app_arg_name in not_to_scale_args else '{}'.format(int(app_arg_val) * int(data_multiplier)) 
                cfg['app_args']['arg'+key_app_arg+': ' + app_arg_name] = app_arg_value 
                arg_string += ' {}'.format(app_arg_value)
            #arg_string += ' {}'.format(str(num_partitions))
            cfg['main']['child_args_string'] = '{}'.format(arg_string)
            cfg['main']['num_partitions'] = str(num_partitions)
            cfg['main']['iter_num'] = str(1) #vboxvm
            cfg['main']['num_experiments'] = str(num_experiments)
            cfg['main']['experiment_num'] = str(index)
            #cfg['main']['cluster_id'] = cluster_id
            cfg['profile'] = {}
            cfg['profile']['spark_seq'] = str(spark_seq)
            cfg['profile']['profile_name'] = app_name
            cfg['profile']['metaprofile_name'] = meta_profile_name
            
            if reuse_dataset:
                cfg['main']['delete_hdfs'] = str(not reuse_dataset)
                
        print(bold('Profile experiment {} performing {} runs for application {} on cluster {}'.format(exp_filepath, 
                                                                                                   num_run, app_name,
                                                                                                   cluster_id,)))
        run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
                   cluster_id=cluster_id, run=1, terminate=0, reboot=0)
        if not c.PROCESS_ON_SERVER:
            average_runs.main(profile_name=utils.get_cfg()['experiment']['profile_name'])
            
    join_jsons.join_dags(OUTPUT_DIR)
    #join_jsons.join_dags("spark_log_profiling"+os.sep+"avg_json")
    
    deploy_meta_profile(meta_profile_name, cluster_id, True)
    
    #upload all the normal (non-meta) profiles
    for filename in os.listdir(OUTPUT_DIR):
        profilename = filename.split(os.sep)[-1].split(".")[0]
        profile_fname = filename.split(os.sep)[-1]
        if profilename != meta_profile_name and not "collection" in profilename and profile_fname.split(".")[-1] == "json":
           deploy_meta_profile(profilename, cluster_id) 
    
    # raise NotImplementedError()


def submit(args):
    cluster_id = c.CLUSTER_MAP['spark']
    num_run = args.num_runs
    reuse_dataset = args.reuse_dataset
    #exp_filepath = args.exp_file_path if args.exp_file_path else "experiment.json"
    exp_filepaths = args.exp_file_paths if args.exp_file_paths else ["experiment.json"]
    for exp_filepath in exp_filepaths:
        exp_file = Path(exp_filepath)
        if exp_file.exists():
            experiment = json.load(open(exp_filepath))
            try:
                benchmark = experiment["BenchmarkName"]
            except KeyError as error:
                print("ERROR:  {} in experiment file: {}".format(error, exp_filepath))
                exit(1) 
        with utils.open_cfg(mode='w') as cfg:
            for s in cfg.sections():
                if s != 'hdfs':
                    cfg.remove_section(s)
            cfg['main'] = {}
            cfg['main']['tool_on_master'] = 'false'
            cfg['main']['experiment_file'] = exp_filepath
            cfg['main']['process_on_server'] = str(c.PROCESS_ON_SERVER)
            cfg['main']['iter_num'] = str(1)
            cfg['submit'] = {}
            cfg[benchmark] = {}
            #cfg[benchmark]['profile_name'] = '{}'.format(c.VAR_PAR_MAP[benchmark]['profile_name'])
            if reuse_dataset:
                cfg['main']['delete_hdfs'] = str(not reuse_dataset)
        print(bold('Submit experiment {} performing {} runs for benchmark {} on cluster {}'.format(exp_filepath, 
                                                                                num_run, benchmark, 
                                                                                cluster_id,)))
        run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
                   cluster_id=cluster_id, run=1, terminate=0, reboot=0)
    #raise NotImplementedError()

def submit_symex(args):
    cluster_id = c.CLUSTER_MAP['spark']
    num_run = args.num_runs
    reuse_dataset = args.reuse_dataset
    exp_filepaths = args.exp_file_paths if args.exp_file_paths else ["experiment.json"]
    num_experiments = len(exp_filepaths)
    index = 0
    app_name = ''
    app_jar = ''
    app_class = ''
    guard_evaluator_class = ''
    num_partitions = ''
    app_args = {}
    meta_profile_name = ''
    for exp_filepath in exp_filepaths:
        exp_file = Path(exp_filepath)
        index += 1
        if exp_file.exists():
            experiment = json.load(open(exp_filepath))
            try:
                app_name = experiment["AppName"]
                app_jar = experiment["AppJar"]
                app_class = experiment["AppClass"]
                guard_evaluator_class = experiment["GuardEvaluatorClass"]
                num_partitions = experiment["NumPartitions"]
                app_args = experiment["AppConf"]
                data_multiplier = experiment["DataMultiplier"] if experiment["DataMultiplier"] else 1
                meta_profile_name = experiment["MetaProfileName"] if experiment["MetaProfileName"] else meta_profile_name
            except KeyError as error:
                print("ERROR:  {} in experiment file: {}".format(error, exp_filepath))
                exit(1) 
        with utils.open_cfg(mode='w') as cfg:
            for s in cfg.sections():
                if s != 'hdfs':
                    cfg.remove_section(s)
            cfg['main'] = {}
            cfg['main']['app_name'] = app_name
            cfg['main']['app_jar'] = app_jar
            cfg['main']['app_class'] = app_class
            cfg['main']['guard_evaluator_class'] = guard_evaluator_class
            cfg['main']['tool_on_master'] = 'false'
            cfg['main']['experiment_file'] = exp_filepath
            cfg['main']['process_on_server'] = str(c.PROCESS_ON_SERVER)
            cfg['experiment'] = {}
            cfg['experiment']['app_name'] = app_name
            cfg['experiment']['profile_name'] = app_name
            cfg['experiment']['meta_profile_name'] = meta_profile_name
            cfg['app_args'] = {}
            arg_string = ''
            not_to_scale_args = ["pastMonths", "inputFile", "outputFile", "delimiter", "parallelism", "minimumCompressionProgress", "progressCounter"]
            for key_app_arg in sorted(app_args.keys(), key=lambda k: int(k)):
                app_arg_name = '{}'.format(app_args[key_app_arg]["Name"])
                app_arg_val = '{}'.format(app_args[key_app_arg]["Value"]) 
                app_arg_value = app_arg_val if app_arg_name in not_to_scale_args else '{}'.format(int(app_arg_val) * int(data_multiplier)) 
                cfg['app_args']['arg'+key_app_arg+': ' + app_arg_name] = app_arg_value 
                arg_string += ' {}'.format(app_arg_value)
            #arg_string += ' {}'.format(str(num_partitions))
            cfg['main']['child_args_string'] = '{}'.format(arg_string)
            cfg['main']['num_partitions'] = str(num_partitions)
            cfg['main']['iter_num'] = str(1) #vboxvm
            cfg['main']['num_experiments'] = str(num_experiments)
            cfg['main']['experiment_num'] = str(index)
            
            if reuse_dataset:
                cfg['main']['delete_hdfs'] = str(not reuse_dataset)
            
        print(bold('Submit experiment {} performing {} runs for application {} on cluster {}'.format(exp_filepath, 
                                                                                num_run, app_name, 
                                                                                cluster_id,)))
        run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
                   cluster_id=cluster_id, run=1, terminate=0, reboot=0)
    #raise NotImplementedError()
    
def reboot_cluster(cluster):
    cluster_id = c.CLUSTER_MAP[cluster]
    print(bold('Reboot {}...'.format(cluster_id)))
    run_xspark(current_cluster=cluster, num_instance=0, cluster_id=cluster_id, run=0, terminate=0, reboot=1)


def reboot(args):
    cluster = args.cluster
    if cluster == 'all':
        reboot_cluster('hdfs')
        reboot_cluster('spark')
    else:
        reboot_cluster(cluster)




def terminate(args):
    cluster = args.cluster
    if cluster == 'all':
        kill_cluster('spark')
        kill_cluster('hdfs')
    else:
        kill_cluster(cluster)


def launch_exp(args):
    cluster_id = c.CLUSTER_MAP['spark']
    var_par = args.var_par
    bench = args.benchmark
    num_run = args.num_runs
    reuse_dataset = args.reuse_dataset
    max_executors = args.max_executors
    num_partitions = args.num_partitions
    for v in var_par:
        with utils.open_cfg(mode='w') as cfg:
            for s in cfg.sections():
                if s != 'hdfs':
                    cfg.remove_section(s)
            cfg['main'] = {}
            cfg['main']['profile'] = 'true' if args.profile else 'false'
            cfg['main']['time_analysis'] = 'true' if args.time_analysis else 'false'
            cfg['main']['tool_on_master'] = 'false'
            cfg['main']['benchmark'] = bench
            cfg[bench] = {}
            cfg[bench][c.VAR_PAR_MAP[bench]['var_name']] = '{}'.format(v)
            cfg[bench]['num_partitions'] = str(num_partitions)
            if reuse_dataset:
                cfg['main']['delete_hdfs'] = str(not reuse_dataset)
            if max_executors:
                cfg['main']['max_executors'] = str(max_executors)
        print(bold('Launch {} Experiments for benchmark {} on cluster {} with {}={}...'.format(num_run, bench,
                                                                                               cluster_id,
                                                                                               c.VAR_PAR_MAP[bench][
                                                                                                   'var_name'], v)))
        run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
                   cluster_id=cluster_id, run=1, terminate=0, reboot=0)
        if not c.PROCESS_ON_SERVER:
            if args.profile:
                run_log_profiling(None)
        if args.time_analysis:
            run_time_analysis(None)

def launch_symex(args):
    cluster_id = c.CLUSTER_MAP['spark']
    app_name = args.app_name
    app_jar = args.app_jar
    app_class = args.app_class
    app_args = args.app_args
    num_run = args.num_runs
    reuse_dataset = args.reuse_dataset
    max_executors = args.max_executors
    num_partitions = args.num_partitions
    arg_string = ''
    with utils.open_cfg(mode='w') as cfg:
        for s in cfg.sections():
            if s != 'hdfs':
                cfg.remove_section(s)
        cfg['main'] = {}
        cfg['main']['profile'] = 'true' if args.profile else 'false'
        cfg['main']['time_analysis'] = 'true' if args.time_analysis else 'false'
        cfg['main']['tool_on_master'] = 'false'
        cfg['main']['app_name'] = '{}'.format(app_name)
        cfg['main']['app_jar'] = '{}'.format(app_jar)
        cfg['main']['app_class'] = '{}'.format(app_class)
        cfg['app_args'] = {}
        app_arg_pos = 0
        for app_arg in app_args:
            cfg['app_args'][str(app_arg_pos)] = '{}'.format(app_arg)
            arg_string += ' {}'.format(app_arg)
            app_arg_pos += 1
        arg_string += ' {}'.format(str(num_partitions))
        cfg['main']['child_args_string'] = '{}'.format(arg_string)
        cfg['main']['num_partitions'] = str(num_partitions)
        if reuse_dataset:
            cfg['main']['delete_hdfs'] = str(not reuse_dataset)
        if max_executors:
            cfg['main']['max_executors'] = str(max_executors)
    print(bold('Launch {} Experiments for application {} on cluster {} with args: {}'.format(num_run, app_name,
                                                                                           cluster_id, arg_string)))
    
    run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
               cluster_id=cluster_id, run=1, terminate=0, reboot=0)
    if not c.PROCESS_ON_SERVER:
        if args.profile:
            run_log_profiling(None)
    if args.time_analysis:
        run_time_analysis(None)
    

def log_profiling(args):
    run_log_profiling(args.local)


def time_analysis(args):
    run_time_analysis(args.input_dir)

def check_clusters(args):
    cluster = args.cluster
    if cluster == 'all':
        run_check_cluster('spark')
        run_check_cluster('hdfs')
    else:
        run_check_cluster(cluster)


def main():
    parser = argparse.ArgumentParser(
        description=
            """
            xSpark Client
            """
    )

    subparsers = parser.add_subparsers()

    parser_setup = subparsers.add_parser('setup', help='add n nodes to the specified cluster')
    parser_launch_exp = subparsers.add_parser('launch_exp', help='launch experiments on already deployed spark cluster')
    parser_launch_symex = subparsers.add_parser('launch_symex', help='launch experiments on already deployed spark cluster')
    parser_reboot = subparsers.add_parser('reboot', help='reboots all the nodes of the specified cluster')
    parser_terminate = subparsers.add_parser('terminate', help='terminates all the nodes in the specified cluster')
    parser_log_profiling = subparsers.add_parser('log_profiling', help='runs the log_profiling')
    parser_time_analysis = subparsers.add_parser('time_analysis', help='runs the time_analysis')
    parser_check_cluster = subparsers.add_parser('check', help='checks the status of the specified cluster')
    parser_profile = subparsers.add_parser('profile', help='profiles and averages r times the specified application, '
                                                           'deploys the profiling file in xSpark and downloads the '
                                                           'results into the client machine')
    parser_submit = subparsers.add_parser('submit', help='submits the specified application and downloads the results '
                                                         'into the client machine')

    parser_profile_symex = subparsers.add_parser('profile_symex', help='profiles and averages r times the specified application, '
                                                           'deploys the profiling file in xSpark and downloads the '
                                                           'results into the client machine')
    parser_submit_symex = subparsers.add_parser('submit_symex', help='submits the specified application and downloads the results '
                                                         'into the client machine')

    parser_setup.add_argument('cluster', choices=['hdfs', 'spark', 'all', 'generic'], help='The specified cluster')
    parser_setup.add_argument('-n', '--num-instances', type=int, default=5, dest='num_instances',
                              help='Number of instances to be created per cluster')
    parser_setup.add_argument('-y', '--yes', dest='assume_yes', action='store_true',
                              help='Assume yes to the confirmation queries')
    parser_setup.add_argument('-a', '--appdir', type=str, dest='app_dir',
                                   help='the directory where the application will be installed')
    
    parser_reboot.add_argument('cluster', choices=['hdfs', 'spark', 'all', 'generic'], help='The specified cluster')

    parser_terminate.add_argument('cluster', choices=['hdfs', 'spark', 'all', 'generic'], help='The specified cluster')

    parser_launch_exp.add_argument('-e', '--executors', default=None, type=int, dest='max_executors',
                                   help='Maximum number of executors to be used in the experiments. '
                                        'If None, the number of executor will be equal to (number of Spark nodes - 1) '
                                        '[default: %(default)s]')
    parser_launch_exp.add_argument('-b', '--benchmark', default='pagerank', choices=['pagerank', 'kmeans', 'sort_by_key'],
                                   required=True, help='the benchmark application to run')
    parser_launch_exp.add_argument('-v', '--variable-parameter', dest='var_par', nargs='+', required=True,
                                   help="variable parameter for the selected benchmark "
                                        "(it will be considered num_v for pagerank, num_of_points for kmeans,"
                                        "scale_factor for sort_by_key)")
    parser_launch_exp.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs',
                                   help='Number of runs for each configuration')
    parser_launch_exp.add_argument('-p', '--num-partitions', required=True, type=int, dest='num_partitions',
                                   help='Number of partitions for each task')

    parser_launch_exp.add_argument("-P", "--profile", dest="profile", action="store_true",
                                   help="perform log profiling at the end of experiments"
                                        "[default: %(default)s]")
    parser_launch_exp.add_argument("-T", "--time_analysis", dest="time_analysis", action="store_true",
                                   help="perform time analysis at the end of experiments"
                                        "[default: %(default)s]")
    
    parser_launch_exp.add_argument("-R", "--reuse-dataset", dest="reuse_dataset", action="store_true",
                                   help="reuse (do not delete) benchmark data in hdfs master node"
                                        "[default: %(default)s]")
    parser_launch_symex.add_argument('-e', '--executors', default=None, type=int, dest='max_executors',
                                   help='Maximum number of executors to be used in the experiments. '
                                        'If None, the number of executor will be equal to (number of Spark nodes - 1) '
                                        '[default: %(default)s]')
    parser_launch_symex.add_argument('-a', '--application', default='app', type=str, dest='app_name',
                                   required=True, help='the symex application to run')
    parser_launch_symex.add_argument('app_args', metavar='F', type=str, nargs='+', help='a non-empty space separated list of commandline arguments')
    parser_launch_symex.add_argument('-j', '-jar', default=None, type=str, dest='app_jar',
                                   required=True, help='the jar absolute path of the symex application to run')
    parser_launch_symex.add_argument('-c', '--class', default=None, type=str, dest='app_class',
                                   required=True, help='the main class name of the symex application to run')
    parser_launch_symex.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs',
                                   help='Number of runs for each configuration')
    parser_launch_symex.add_argument('-p', '--num-partitions', required=True, type=int, dest='num_partitions',
                                   help='Number of partitions for each task')

    parser_launch_symex.add_argument("-P", "--profile", dest="profile", action="store_true",
                                   help="perform log profiling at the end of experiments"
                                        "[default: %(default)s]")
    parser_launch_symex.add_argument("-T", "--time_analysis", dest="time_analysis", action="store_true",
                                   help="perform time analysis at the end of experiments"
                                        "[default: %(default)s]")
    
    parser_launch_symex.add_argument("-R", "--reuse-dataset", dest="reuse_dataset", action="store_true",
                                   help="reuse (do not delete) benchmark data in hdfs master node"
                                        "[default: %(default)s]")



    
    parser_log_profiling.add_argument("-l", "--local", dest="local", action="store_true",
                                      help="use default local output folders"
                                           "[default: %(default)s]")

    parser_time_analysis.add_argument("-i", "--input-dir", dest="input_dir",
                                      help="input directory (where all the log files are located)"
                                           "[default: load from config file latest benchmark directory")

    parser_check_cluster.add_argument('cluster', choices=['hdfs', 'spark', 'all', 'generic'], help='The specified cluster')

    #parser_profile.add_argument('exp_file_path', nargs='?', default="", help='experiment file path')
    parser_profile.add_argument('exp_file_paths', metavar='F', type=str, nargs='+', help='a non-empty space separated list of experiment file paths')
    parser_profile.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')
    parser_profile.add_argument("-R", "--reuse-dataset", dest="reuse_dataset", action="store_true",
                                   help="reuse (do not delete) benchmark data in hdfs master node"
                                        "[default: %(default)s]")
    parser_profile.add_argument("-q", "--spark-seq", dest="spark_seq", action="store_true",
                                   help="Profile using Spark data sequencing home directory"
                                        "[default: %(default)s]")
    '''
    parser_profile.add_argument('exp_profile_name', nargs='?', default="", help='experiment profile_name')
    parser_profile.add_argument('-e', '--executors', default=None, type=int, dest='max_executors',
                                   help='Maximum number of executors to be used in the experiments. '
                                        'If None, the number of executor will be equal to (number of Spark nodes - 1) '
                                        '[default: %(default)s]')
    parser_profile.add_argument('-b', '--benchmark', default='pagerank', choices=['pagerank', 'kmeans', 'sort_by_key'],
                                   required=True, help='the benchmark application to run')
    parser_profile.add_argument('-v', '--variable-parameter', dest='var_par', nargs='+', required=True,
                                   help="variable parameter for the selected benchmark "
                                        "(it will be considered num_v for pagerank, num_of_points for kmeans,"
                                        "scale_factor for sort_by_key)")
    parser_profile.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')
    parser_profile.add_argument('-p', '--num-partitions', required=True, type=int, dest='num_partitions',
                                   help='Number of partitions for each task')
    parser_profile.add_argument("-l", "--local", dest="local", action="store_true",
                                      help="use default local output folders"
                                           "[default: %(default)s]")
    '''
    parser_profile_symex.add_argument('exp_file_paths', metavar='F', type=str, nargs='+', help='a non-empty space separated list of experiment file paths')
    parser_profile_symex.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')
    parser_profile_symex.add_argument("-R", "--reuse-dataset", dest="reuse_dataset", action="store_true",
                                   help="reuse (do not delete) benchmark data in hdfs master node"
                                        "[default: %(default)s]")
    parser_profile_symex.add_argument("-q", "--spark-seq", dest="spark_seq", action="store_true",
                                   help="Profile using Spark data sequencing home directory"
                                        "[default: %(default)s]")
    
    #parser_submit.add_argument('exp_file_path', nargs='?', default="", help='experiment file path')
    parser_submit.add_argument('exp_file_paths', metavar='F', type=str, nargs='+', help='a non-empty space separated list of experiment file paths')
    parser_submit.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')
    parser_submit.add_argument("-R", "--reuse-dataset", dest="reuse_dataset", action="store_true",
                                   help="reuse (do not delete) benchmark data in hdfs master node"
                                        "[default: %(default)s]")
    parser_submit_symex.add_argument('exp_file_paths', metavar='F', type=str, nargs='+', help='a non-empty space separated list of experiment file paths')
    parser_submit_symex.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')
    parser_submit_symex.add_argument("-R", "--reuse-dataset", dest="reuse_dataset", action="store_true",
                                   help="reuse (do not delete) benchmark data in hdfs master node"
                                        "[default: %(default)s]")
    
    parser_setup.set_defaults(func=setup)
    parser_reboot.set_defaults(func=reboot)
    parser_terminate.set_defaults(func=terminate)
    parser_profile.set_defaults(func=profile)
    parser_submit.set_defaults(func=submit)
    parser_profile_symex.set_defaults(func=profile_symex)
    parser_submit_symex.set_defaults(func=submit_symex)

    parser_launch_exp.set_defaults(func=launch_exp)
    parser_launch_symex.set_defaults(func=launch_symex)
    parser_log_profiling.set_defaults(func=log_profiling)
    parser_time_analysis.set_defaults(func=time_analysis)
    parser_check_cluster.set_defaults(func=check_clusters)

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    try:
        getattr(args, "func")
    except AttributeError:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
