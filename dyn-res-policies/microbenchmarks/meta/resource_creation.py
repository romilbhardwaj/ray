import argparse
import logging
import time
import numpy as np

import ray
from ray.tests.cluster_utils import Cluster

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')
logger.setLevel(logging.INFO)

def noop():
    return 1

def dummy(name, capacity, client_id = None):
    return 1

def create_res(name, capacity, client_id = None):
    return ray.experimental.create_resource(name, capacity, client_id)

def delete_res(name, client_id = None):
    return ray.experimental.delete_resource(name, client_id)

class Experiment(object):
    def __init__(self, num_nodes, num_res, redis_address, num_runs):
        self._redis_address = redis_address
        self._num_nodes = num_nodes
        self._num_res = num_res
        self._num_runs = num_runs

        self._cluster = None

        self.init_ray()

        self._client_ids = self.get_client_ids()

    def get_client_ids(self):
        client_table = ray.global_state.client_table()
        client_ids = [client["ClientID"] for client in client_table]
        logger.info("Got clientIds: " + str(",".join(client_ids)))
        return client_ids

    def shutdown(self):
        if self._cluster:
            self._cluster.shutdown()
        else:
            logger.warning("SHUTDOWN WARNING: Ray cluster was external, not sure if this method call would do anything.")
        ray.shutdown()

    def restart_ray(self):
        self.shutdown()
        self._redis_address = None
        self.init_ray()
        self._client_ids = self.get_client_ids()

    def init_ray(self):
        if self._redis_address:
            logger.info("Using predefined redis address {}".format(self._redis_address))
            redis_address = self._redis_address
        else:
            self._cluster = Cluster()
            for i in range(self._num_nodes):
                self._cluster.add_node(num_cpus=4, resources={'node{}'.format(i): 1})

            self._cluster.wait_for_nodes()
            self._redis_address = self._cluster.redis_address
            logger.info("Cluster init complete. Redis: {}".format(self._redis_address))
        logger.info("Connecting ray client.")
        ray.init(redis_address=self._redis_address)

    def warmup(self, func, args):
        '''
        Warmsup the cluster by running the function with the specified args on all machines in the cluster.
        :param func: python method
        :param args: list of args
        :return:
        '''
        for client_id in self._client_ids:
            create_res(str(client_id), 1, client_id)
        time.sleep(1)

        # Launch func on each node to warmup.
        tasks = []
        for client_id in self._client_ids:
            tasks.append(ray.remote(func)._remote(args=args, resources={client_id: 1}))
        ray.get(tasks)
        time.sleep(1)

        # Garbage collection
        for client_id in self._client_ids:
            delete_res(str(client_id), client_id)

    def exp_create_resources(self):
        # This experiement creates numnodes*numres resources in the cluster and returns the time it took till they
        # were available. Available can be either end of creation task submission, the moment when they were avilable
        # in the GCS or the moment they were published as heartbeat.
        assert self._num_res % self._num_nodes == 0
        logger.info("Start exp_create_resources experiment")
        self.warmup(create_res, args=["warmup", 1])
        logger.info("Warmup done")
        remote_create_res = ray.remote(create_res)
        remote_noop = ray.remote(noop)

        num_total_resources = self._num_res
        list_of_resources = [str(i) for i in range(0, num_total_resources)]
        res_req_dict = {k: 1 for k in list_of_resources}
        logger.info("Creating a total of {} resources".format(len(list_of_resources)))

        all_res_created = False

        # Start timing
        start = time.time()

        tasks = []
        i=0
        for client_id in self._client_ids:
            # Create resources of label i with capacity 1 on the node.
            for k in range(0, int(self._num_res/self._num_nodes)):
                tasks.append(remote_create_res._remote(args=[str(i), 1, client_id], num_cpus=0))
                i+=1

        end_submission = time.time()
        ray.get(tasks)
        end_gettasks = time.time()
        ray.get(remote_noop._remote(args=[], resources=res_req_dict))
        # while not all_res_created:
        #     print("try.")
        #     avail_res = ray.global_state.available_resources()
        #     all_res_created = all([str(r) in avail_res for r in range(0, i)])
        #     print("try done.")
        end_resavailable = time.time()

        submission_time = end_submission-start
        gettask_time = end_gettasks-start
        available_time = end_resavailable-start

        logger.info("*Time taken*")
        logger.info("start-end_submission: {:.2f}".format(submission_time))
        logger.info("start-end_gettasks: {:.2f}".format(gettask_time))
        logger.info("start-end_resavailable: {:.2f}".format(available_time))

        return [submission_time, gettask_time, available_time], ["Submission Time", "Get Task Time", "Resource Availability Time"]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dynres creation experiment')
    parser.add_argument('--num-nodes', default=1, type=int, help='The number of nodes in the cluster if running locally.')
    parser.add_argument('--num-res', default=1000, type=int, help='The number of resources to create total across the cluster.')
    parser.add_argument('--num-runs', default=1, type=int, help='The number of runs per experiment.')
    parser.add_argument('--redis-address', default=None, type=str,
                        help='The address of the redis server.')

    args = parser.parse_args()

    logger.info("Creating experiment object.")
    e = Experiment(**vars(args))

    create_res_data = []
    for i in range(0, args.num_runs):
        data, labels = e.exp_create_resources()
        create_res_data.append(data)
        e.restart_ray()

    create_res_data = np.array(create_res_data)
    mean_data = np.mean(create_res_data, axis=0)
    for i, label in enumerate(labels):
        logger.info("Mean {}: {}".format(label, mean_data[i]))
    input("Done.")
