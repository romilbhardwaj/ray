import random

from ray.utils import binary_to_object_id
from collections import Counter

import ray
import time
import ray.tests.cluster_utils

# This example demonstrates data co-locality by creating a resource where the data resides and launching a task there
NUM_NODES = 3

# Initialize cluster for testing
print("Initializing cluster..")
cluster = ray.tests.cluster_utils.Cluster()
for i in range(NUM_NODES):
    cluster.add_node(num_cpus=8)

cluster.wait_for_nodes()

print("Cluster init complete, connecting driver")
ray.init(redis_address=cluster.redis_address)

# Define task which generates data
@ray.remote
def data_generator(id):
    print("Generating data")
    # Create a resource on the local node with an identifier for the data
    ray.experimental.set_resource("data_{}".format(id), 1)
    return random.random()

@ray.remote
def data_co_located_task(obj_id):
    print("data co-located task")
    # Get the object id before operating on it. This should be fetched from the local plasma store.
    return ray.get(obj_id[0]) + 1

if __name__ == '__main__':
    # Launch the data_generator to create the data at some remote node
    print("Launching tasks")
    start_time = time.time()

    id = "mydata"
    data_obj_id = data_generator.remote(id)

    # Create co-located task
    colocated_result = data_co_located_task._remote(args=[[data_obj_id]], resources={"data_{}".format(id): 1})
    print("Getting task results")
    res = ray.get(colocated_result)
    end_time = time.time()
    print("Result - %s" % str(res))
    print("Time taken = %s" % str(end_time-start_time))