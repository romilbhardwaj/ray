import random

from ray.utils import binary_to_object_id
from collections import Counter

import ray
import time
import ray.tests.cluster_utils

# This example demonstrates data co-locality by fetching the clientId where a target object exists and launching a task there
NUM_NODES = 3

# Initialize cluster
print("Initializing cluster..")
cluster = ray.tests.cluster_utils.Cluster()
for i in range(NUM_NODES):
    cluster.add_node(num_cpus=8)

cluster.wait_for_nodes()

print("Cluster init complete, connecting driver")
ray.init(redis_address=cluster.redis_address)

print("Parsing client table and fetching clientIds")
client_table = ray.global_state.client_table()
client_ids = [client["ClientID"] for client in client_table]
print("ClientIds: " + str(",".join(client_ids)))

# Define task which generates data
@ray.remote
def data_generator():
    print("Generating data")
    return random.random()

@ray.remote
def data_co_located_task(data):
    print("data co-located task")
    return data + 1

# Launch the data_generator to create the data at some remote node
print("Launching tasks")
start_time = time.time()

data_obj_id = data_generator.remote()

# Create object where the data resides
def get_clientid_of_objectid(obj_id):
    obj_data = ray.global_state.object_table(obj_id)
    while (not obj_data):
        obj_data = ray.global_state.object_table(obj_id)    # wait for object to be created
    return binary_to_object_id(obj_data['Manager'])

destination_clientid = get_clientid_of_objectid(data_obj_id)

# Create colocation resource
ray.experimental.create_resource("my_colocation", 1, destination_clientid.hex())

colocated_result = data_co_located_task._remote(args=[data_obj_id], resources={"my_colocation": 1})
print("Getting task results")
res = ray.get(colocated_result)
end_time = time.time()
print("Result - %s" % str(res))
print("Time taken = %s" % str(end_time-start_time))