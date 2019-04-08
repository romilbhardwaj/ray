import argparse
import logging
import numpy as np
import os

from .resource_creation import Experiment

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')
logger.setLevel(logging.INFO)

def write_data(num_nodes, num_res, data, labels, base_dir = '.'):
    os.makedirs(base_dir, exist_ok=True)
    data_file_name = "n{}_r{}.txt".format(num_nodes, num_res)
    data_path = os.path.join(base_dir, data_file_name)
    label_path = os.path.join(base_dir, "label.txt")
    np.savetxt(data_path, data)
    with open(label_path, 'w') as f:
        f.write(",".join(labels))

if __name__ == '__main__':
    NUM_NODE_RANGE = [1, 5, 10]
    NUM_RES_RANGE = [10, 100, 1000]
    NUM_RUNS = 5

    for num_nodes in NUM_NODE_RANGE:
        for num_res in NUM_RES_RANGE:
            logger.info("Testing num nodes {}, num_tasks {}. Creating experiment object now.".format(num_nodes, num_res))
            e = Experiment(num_nodes=num_nodes, num_res=num_res, redis_address=None, num_runs=None)

            create_res_data = []

            for i in range(0, NUM_RUNS):
                data, labels = e.exp_create_resources()
                create_res_data.append(data)
                e.restart_ray()

            create_res_data = np.array(create_res_data)
            mean_data = np.mean(create_res_data, axis=0)

            logger.info("**Testing num nodes {}, num_tasks {}. Results: ".format(num_nodes, num_res))
            for i, label in enumerate(labels):
                logger.info("Mean {}: {}".format(label, mean_data[i]))

            write_data(num_nodes, num_res, data, labels)

            e.shutdown()