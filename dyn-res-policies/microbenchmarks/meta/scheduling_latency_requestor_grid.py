import argparse
import logging
import numpy as np
import os

from resource_creation import Experiment

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')
logger.setLevel(logging.INFO)

def write_data(data, labels, filename, base_dir = '.'):
    os.makedirs(base_dir, exist_ok=True)
    data_file_name = filename
    data_path = os.path.join(base_dir, data_file_name)
    label_path = os.path.join(base_dir, "label.txt")
    np.savetxt(data_path, data)
    with open(label_path, 'w') as f:
        f.write(",".join(labels))

if __name__ == '__main__':
    NUM_NODE_RANGE = [1, 10]
    NUM_RES_RANGE = [10000]
    NUM_RES_REQUEST_RANGE = [1, 10, 50, 100, 500, 1000]
    NUM_RUNS = 10

    for num_nodes in NUM_NODE_RANGE:
        for num_res in NUM_RES_RANGE:
            logger.info("******* Scheduling latency. Testing "
                        "num nodes {}, num_res {}. Creating experiment object now.********"
                        .format(num_nodes, num_res, ))
            e = Experiment(num_nodes=num_nodes, num_res=num_res, redis_address=None, num_runs=None)
            e.setup_equal_resources(num_res)
            for num_res_request in NUM_RES_REQUEST_RANGE:
                logger.info("###### Running exp requesting {} resources. ###########".format(num_res_request))
                exp_data = []

                for i in range(0, NUM_RUNS):
                    data, labels = e.exp_scheduling_latency_requestor(num_res_request)
                    exp_data.append(data)
                    # e.restart_ray()

                exp_data = np.array(exp_data)
                mean_data = np.mean(exp_data, axis=0)

                logger.info("**Testing num nodes {}, num_res {}, num_res_request {}. Results: ".format(num_nodes, num_res, num_res_request))
                for i, label in enumerate(labels):
                    logger.info("Mean {}: {}".format(label, mean_data[i]))

                filename = "n{}_r{}_req{}.txt".format(num_nodes, num_res, num_res_request)
                write_data(exp_data, labels, filename, base_dir='./sched_latency_request/')
            e.shutdown()