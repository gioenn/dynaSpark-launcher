import launch
import boto3
import time
import copy
import run

from config import NUMINSTANCE, REGION, TAG, REBOOT, CLUSTER_ID, TERMINATE, RUN, NUM_RUN, CREDENTIAL_PROFILE


def main():
    """ Main function;
    - Launch spot request of NUMINSTANCE
    - Run Benchmark
    :return:
    """
    session = boto3.Session(profile_name=CREDENTIAL_PROFILE)
    client = session.client('ec2', region_name=REGION)

    if NUMINSTANCE > 0:
        spot_request_ids = launch.launch(client, NUMINSTANCE)

        print("CHECK SECURITY GROUP ALLOWED IP SETTINGS!!!")

        # Wait for our spots to fulfill
        launch.wait_for_fulfillment(client, spot_request_ids, copy.deepcopy(spot_request_ids))

        spot_instance_response = client.describe_spot_instance_requests(
            SpotInstanceRequestIds=spot_request_ids)
        instance_ids = [result["InstanceId"] for result in
                        spot_instance_response["SpotInstanceRequests"]]

        client.create_tags(Resources=instance_ids, Tags=TAG)

        # Wait Running
        launch.wait_for_running(client, instance_ids, copy.deepcopy(instance_ids))

        time.sleep(15)

        launch.wait_ping(client, instance_ids, copy.deepcopy(instance_ids))

    if REBOOT:
        print("Rebooting instances...")
        instances = client.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                     {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}])
        instance_ids = [x.id for x in instances]
        client.reboot_instances(InstanceIds=instance_ids)
        launch.wait_ping(client, instance_ids, copy.deepcopy(instance_ids))

    if RUN:
        for i in range(NUM_RUN):
            run.run_benchmark()

    if TERMINATE:
        instances = client.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                     {'Name': 'tag:ClusterId', 'Values': [CLUSTER_ID]}])
        instance_ids = [x.id for x in instances]
        # TODO get spot_request_ids
        launch.terminate(client, spot_request_ids, instance_ids)


if __name__ == "__main__":
    main()