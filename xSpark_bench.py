#from config import PROVIDER, NUM_INSTANCE, NUM_RUN, CLUSTER_ID, TERMINATE, RUN, REBOOT, CLUSTER_MAP, VAR_PAR_MAP, \
#                  PROCESS_ON_SERVER
#import config as c
from configure import config_instance as c
import libcloud.common.base
import argparse
import sys

from spark_log_profiling import processing as profiling
from spark_log_profiling import average_runs
from spark_time_analysis import run as run_ta
from colors import header, okblue, okgreen, warning, underline, bold, fail

import util.utils as utils

from factory_methods import BenchInstanceFactory
import argcomplete
import json
from pathlib import Path
libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True

def run_xspark(current_cluster, num_instance=c.NUM_INSTANCE, num_run=c.NUM_RUN, cluster_id=c.CLUSTER_ID, terminate=c.TERMINATE,
               run=c.RUN, reboot=c.REBOOT, assume_yes=False):
    """ Main function;
    * Launch spot request of NUMINSTANCE
    * Run Benchmark
    * Download Log
    * Plot data from log
    """
    print(header('run_xspark(num_instance={}, num_run={}, cluster_id={},terminate={}, run={}, reboot={})'
          .format(num_instance, num_run, cluster_id, terminate, run, reboot)))
    # get cfg_file and initialize main settings
    with utils.open_cfg(mode='w') as cfg:
        if 'main' not in cfg:
            cfg['main'] = {}
        cfg.set('main', 'current_cluster', current_cluster)

    bench_instance = BenchInstanceFactory.get_bench_instance(c.PROVIDER, cluster_id)
    setup_ok = True

    if num_instance > 0:
        println("setup_ok = bench_instance.setup(num_instance, assume_yes)")

    if reboot:
        println("bench_instance.reboot()")

    if setup_ok and run:
        bench_instance.run(num_run)

    if terminate:
        println("bench_instance.terminate()")

def run_xspark_disabled(current_cluster, num_instance=c.NUM_INSTANCE, num_run=c.NUM_RUN, cluster_id=c.CLUSTER_ID, terminate=c.TERMINATE,
               run=c.RUN, reboot=c.REBOOT, assume_yes=False):
    """ Main function;
    * Launch spot request of NUMINSTANCE
    * Run Benchmark
    * Download Log
    * Plot data from log
    """
    print(header('run_xspark(num_instance={}, num_run={}, cluster_id={},terminate={}, run={}, reboot={})'
          .format(num_instance, num_run, cluster_id, terminate, run, reboot)))
    # get cfg_file and initialize main settings
    with utils.open_cfg(mode='w') as cfg:
        if 'main' not in cfg:
            cfg['main'] = {}
        cfg.set('main', 'current_cluster', current_cluster)

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

def setup_cluster(cluster, num_instances, assume_yes):
    # termporary structure to save run configuration
    # TODO: IMPROVE THIS
    run_on_setup = {
        'spark': 0,
        'hdfs' : 0,
        'generic': 0
    }
    cluster_id = c.CLUSTER_MAP[cluster]
    print(bold('Setup {} with {} instances...'.format(cluster_id, num_instances)))
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
            deploy_profile(cluster_id, benchmark)
    # raise NotImplementedError()

def profile_disabled(args):
    cluster_id = c.CLUSTER_MAP['spark']
    var_par = args.var_par
    exp_profile_name = args.exp_profile_name if args.exp_profile_name else ""
    benchmark = args.benchmark
    num_run = args.num_runs
    max_executors = args.max_executors
    num_partitions = args.num_partitions
    for v in var_par:
        with utils.open_cfg(mode='w') as cfg:
            cfg['main'] = {}
            cfg['main']['profile'] = 'true'
            cfg['main']['tool_on_master'] = 'false'
            cfg['main']['benchmark'] = benchmark
            cfg['main']['iter_num'] = str(1) #vboxvm
            cfg[benchmark] = {}
            cfg[benchmark][c.VAR_PAR_MAP[benchmark]['var_name']] = '({}, {})'.format(c.VAR_PAR_MAP[benchmark]['default'][0], v)
            cfg[benchmark]['profile_name']= \
                '{}'.format(c.VAR_PAR_MAP[benchmark]['profile_name']) if not args.exp_profile_name else args.exp_profile_name
            cfg[benchmark]['num_partitions'] = str(num_partitions)
            if max_executors:
                cfg['main']['max_executors'] = max_executors
        print(bold('Profile {} performing {} runs for benchmark {} on cluster {} with {}={}...'.format(exp_profile_name, 
                                                                                                       num_run, benchmark,
                                                                                                       cluster_id,
                                                                                                       c.VAR_PAR_MAP[benchmark][
                                                                                                           'var_name'], v)))
        run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
                   cluster_id=cluster_id, run=1, terminate=0, reboot=0)
        #profiling.main()
        average_runs.main(profile_name=utils.get_cfg()[benchmark]['profile_name'])
        #run_log_profiling(args.local)
        deploy_profile(cluster_id, benchmark)
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
                cfg.remove_section(s)
            cfg['main'] = {}
            cfg['main']['tool_on_master'] = 'false'
            cfg['main']['experiment_file'] = exp_filepath
            cfg['main']['process_on_server'] = str(c.PROCESS_ON_SERVER)
            cfg['main']['iter_num'] = str(1) #vboxvm
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

    parser_setup.add_argument('cluster', choices=['hdfs', 'spark', 'all', 'generic'], help='The specified cluster')
    parser_setup.add_argument('-n', '--num-instances', type=int, default=5, dest='num_instances',
                              help='Number of instances to be created per cluster')
    parser_setup.add_argument('-y', '--yes', dest='assume_yes', action='store_true',
                              help='Assume yes to the confirmation queries')

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
    #parser_submit.add_argument('exp_file_path', nargs='?', default="", help='experiment file path')
    parser_submit.add_argument('exp_file_paths', metavar='F', type=str, nargs='+', help='a non-empty space separated list of experiment file paths')
    parser_submit.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')
    parser_submit.add_argument("-R", "--reuse-dataset", dest="reuse_dataset", action="store_true",
                                   help="reuse (do not delete) benchmark data in hdfs master node"
                                        "[default: %(default)s]")
    parser_setup.set_defaults(func=setup)
    parser_reboot.set_defaults(func=reboot)
    parser_terminate.set_defaults(func=terminate)
    parser_profile.set_defaults(func=profile)
    parser_submit.set_defaults(func=submit)

    parser_launch_exp.set_defaults(func=launch_exp)
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
