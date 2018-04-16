import copy
import time

import launch
import run as x_run
from config import CONFIG_DICT, PROVIDER, REGION, TAG, NUM_INSTANCE, NUM_RUN, CLUSTER_ID, TERMINATE, RUN, REBOOT
from credentials import AWS_ACCESS_ID, AWS_SECRET_KEY,\
    AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID

from libcloud.compute.providers import get_driver
from drivers.ccglibcloud.ec2spot import set_spot_drivers
from drivers.azurearm.driver import set_azurearm_driver

import libcloud.common.base
import argparse
import sys
from spark_log_profiling import processing as profiling
from spark_time_analysis import run as run_ta
from colors import header, okblue, okgreen, warning, underline, bold, fail

import util.utils as utils

libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True

cluster_map = {
    'hdfs': 'CSPARKHDFS',
    'spark': 'CSPARKWORK'
}


def run_xspark(current_cluster, num_instance=NUM_INSTANCE, num_run=NUM_RUN, cluster_id=CLUSTER_ID, terminate=TERMINATE,
               run=RUN, reboot=REBOOT, assume_yes=False):
    """ Main function;
    * Launch spot request of NUMINSTANCE
    * Run Benchmark
    * Download Log
    * Plot data from log
    """
    print(header('run_xspark(num_instance={}, num_run={}, cluster_id={},terminate={}, run={}, reboot={})'
          .format(num_instance, num_run, cluster_id, terminate, run, reboot)))
    cfg = utils.get_cfg()
    cfg['main'] = {}
    cfg.set('main', 'current_cluster', current_cluster)
    utils.write_cfg(cfg)

    if PROVIDER == "AWS_SPOT":
        set_spot_drivers()
        cls = get_driver("ec2_spot_" + REGION.replace('-', '_'))
        driver = cls(AWS_ACCESS_ID, AWS_SECRET_KEY)
    elif PROVIDER == "AZURE":
        set_azurearm_driver()
        cls = get_driver("CustomAzureArm")
        driver = cls(tenant_id=AZ_TENANT_ID,
                     subscription_id=AZ_SUBSCRIPTION_ID,
                     key=AZ_APPLICATION_ID, secret=AZ_SECRET, region=CONFIG_DICT["Azure"]["Location"])

    else:
        print("Unsupported provider", PROVIDER)
        return

    if num_instance > 0:

        # Create nodes
        if PROVIDER == "AWS_SPOT":
            nodes, spot_requests = launch.launch_libcloud(driver, num_instance, CONFIG_DICT, cluster_id, assume_yes)

        if PROVIDER == "AZURE":
            nodes = launch.launch_libcloud(driver, num_instance, CONFIG_DICT, cluster_id, assume_yes)

        # nodes is a list of "libcloud.compute.base.Node"

        print("CHECK SECURITY GROUP ALLOWED IP SETTINGS!!!")

        # Tag nodes
        if PROVIDER == "AWS_SPOT":
            for node in nodes:
                driver.ex_create_tags(node, TAG[0])
        elif PROVIDER == "AZURE":
            for node in nodes:
                driver.ex_create_tags(node, {"ClusterId": cluster_id})  # was CONFIG_DICT["Azure"]["ClusterId"]

        instance_ids = [n.id for n in nodes]

        # Wait for all the nodes to become RUNNNING
        print("Waiting for nodes to run")
        launch.wait_for_running_libcloud(driver, instance_ids, copy.deepcopy(instance_ids))

        time.sleep(15)

        # Wait for all the nodes to be pingable
        print("Waiting for nodes to be pingable")
        launch.wait_ping_libcloud(driver, instance_ids, copy.deepcopy(instance_ids))

    if reboot:
        print("Rebooting instances...")

        # Retrieve running nodes
        if PROVIDER == "AWS_SPOT":
            nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
            nodes = [n for n in nodes if driver.ex_describe_tags(node)['Value'] == cluster_id]
        elif PROVIDER == "AZURE":
            nodes = driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])
            nodes = [n for n in nodes if n.extra["tags"]["ClusterId"] == cluster_id]

        # Reboot nodes
        for node in nodes:
            driver.reboot_node(node)

        # Wait for all the nodes to be pingable
        instance_ids = [n.id for n in nodes]
        launch.wait_ping_libcloud(driver, instance_ids, copy.deepcopy(instance_ids))

    if run:
        for i in range(num_run):
            if PROVIDER == "AWS_SPOT":
                nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
                nodes = [n for n in nodes if driver.ex_describe_tags(n)['Value'] == cluster_id]
            elif PROVIDER == "AZURE":
                nodes = driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])
                nodes = [n for n in nodes if n.extra["tags"]["ClusterId"] == cluster_id]

            # nodes is a list of "libcloud.compute.base.Node"
            print("Found {} nodes".format(len(nodes)))

            x_run.run_benchmark(nodes)


    if terminate:
        print("Begin termination of instances and cleaning")

        # Cancel Spot Request
        if PROVIDER == "AWS_SPOT" and num_instance > 0:
            for s in spot_requests:
                driver.ex_cancel_spot_instance_request(s)
            print("Spot requests cancelled")

        ###################################################

        # Retrieve running nodes
        if PROVIDER == "AWS_SPOT":
            nodes = driver.list_nodes(ex_filters={'instance-state-name': ['running']})
            nodes = [n for n in nodes if driver.ex_describe_tags(n)['Value'] == cluster_id]
        elif PROVIDER == "AZURE":
            nodes = driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])
            nodes = [n for n in nodes if n.extra["tags"]["ClusterId"] == cluster_id]
        print("Found {} nodes".format(len(nodes)))

        # nodes is a list of "libcloud.compute.base.Node"

        # Destroy all nodes
        print("Destroying nodes")
        for node in nodes:
            driver.destroy_node(node)

        print(okgreen("All nodes destroyed"))



def setup_cluster(cluster, num_instances, assume_yes):
    # termporary structure to save run configuration (to be changed)
    run_on_setup = {
        'spark': 0,
        'hdfs' : 1
    }
    cluster_id = cluster_map[cluster]
    print(bold('Setup {} with {} instances...'.format(cluster_id, num_instances)))
    run_xspark(current_cluster=cluster, num_instance=num_instances, cluster_id=cluster_id,
               run=run_on_setup[cluster], terminate=0, reboot=0, assume_yes=assume_yes)


def kill_cluster(cluster):
    cluster_id = cluster_map[cluster]
    print(bold('Terminate {}...'.format(cluster_id)))
    run_xspark(current_cluster=cluster, num_instance=0, cluster_id=cluster_id, run=0, terminate=1, reboot=0)
    cfg = utils.get_cfg()
    cfg[cluster] = {}
    utils.write_cfg(cfg)


def run_log_profiling(local):
    out_folder = None
    if not local:
        cfg = utils.get_cfg()
        out_folder = cfg['main']['output_folder'] if 'main' in cfg and 'output_folder' in cfg['main'] else None
    profiling.main(out_folder)


def run_time_analysis(input_dir):
    if not input_dir:
        cfg = utils.get_cfg()
        input_dir = cfg['main']['output_folder'] if 'main' in cfg and 'output_folder' in cfg['main'] else None
    run_ta.main(input_dir)


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
    print(bold('Profile {} performing {} runs...'.format(args.exp_file_path, args.num_runs)))
    raise NotImplementedError()


def submit(args):
    print(bold('Submit {}...'.format(args.exp_file_path)))
    raise NotImplementedError()


def reboot(args):
    cluster = args.cluster
    cluster_id = cluster_map[cluster]
    print(bold('Reboot {}...'.format(cluster_id)))
    run_xspark(current_cluster=cluster, num_instance=0, cluster_id=cluster_id, run=0, terminate=0, reboot=1)


def terminate(args):
    cluster = args.cluster
    if cluster == 'all':
        kill_cluster('spark')
        kill_cluster('hdfs')
    else:
        kill_cluster(cluster)


def launch_exp(args):
    cluster_id = cluster_map['spark']
    num_v = args.num_v
    for v in num_v:
        cfg = utils.get_cfg()
        cfg['pagerank'] = {}
        cfg['pagerank']['num_v'] = v
        utils.write_cfg(cfg)
        print(bold('Launch Experiments on {} with {} vertices...'.format(cluster_id, v)))
        run_xspark(current_cluster='spark', num_instance=0, cluster_id=cluster_id, run=1, terminate=0, reboot=0)
        if args.profile:
            run_log_profiling(None)


def log_profiling(args):
    run_log_profiling(args.local)


def time_analysis(args):
    run_time_analysis(args.input_dir)


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
    '''
    parser_profile = subparsers.add_parser('profile', help='profiles and averages r times the specified application, '
                                                           'deploys the profiling file in xSpark and downloads the '
                                                          'results into the client machine')
    parser_submit = subparsers.add_parser('submit', help='submits the specified application and downloads the results '
                                                         'into the client machine')
    '''

    parser_setup.add_argument('cluster', choices=['hdfs', 'spark', 'all'], help='The specified cluster')
    parser_setup.add_argument('-n', '--num-instances', type=int, default=5, dest='num_instances',
                              help='Number of instances to be created per cluster')
    parser_setup.add_argument('-y', '--yes', dest='assume_yes', action='store_true',
                              help='Assume yes to the confirmation queries')

    parser_reboot.add_argument('cluster', choices=['hdfs', 'spark', 'all'], help='The specified cluster')

    parser_terminate.add_argument('cluster', choices=['hdfs', 'spark', 'all'], help='The specified cluster')

    parser_launch_exp.add_argument('-v', '--num-v', dest='num_v', nargs='+', required=True, help="number of vertices")
    parser_launch_exp.add_argument("-p", "--profile", dest="profile", action="store_true",
                                   help="perform log profiling at the end of experiments"
                                        "[default: %(default)s]")

    parser_log_profiling.add_argument("-l", "--local", dest="local", action="store_true",
                                      help="use default local output folders"
                                           "[default: %(default)s]")

    parser_time_analysis.add_argument("-i", "--input-dir", dest="input_dir",
                                      help="input directory (where all the log files are located)"
                                           "[default: load from config file latest benchmark directory")

    '''
    parser_profile.add_argument('exp_file_path', help='experiment file path')
    parser_profile.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')

    parser_submit.add_argument('exp_file_path', help='experiment file path')
    '''

    parser_setup.set_defaults(func=setup)
    parser_reboot.set_defaults(func=reboot)
    parser_terminate.set_defaults(func=terminate)
    '''
    parser_profile.set_defaults(func=profile)
    parser_submit.set_defaults(func=submit)
    '''

    parser_launch_exp.set_defaults(func=launch_exp)
    parser_log_profiling.set_defaults(func=log_profiling)
    parser_time_analysis.set_defaults(func=time_analysis)

    args = parser.parse_args()

    try:
        getattr(args, "func")
    except AttributeError:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
