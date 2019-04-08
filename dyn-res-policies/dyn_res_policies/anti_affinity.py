import math
from collections import Counter

import ray
import time
import ray.tests.cluster_utils

# This example demonstrates implementation of anti-affinity with dynamic custom resources.
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

# Create anti-affinity resource
num_resources_to_create = 1
print("Creating %d anti-affnity resource on nodes" % num_resources_to_create)
for client_id in client_ids:
    ray.experimental.create_resource("anti_affinity", num_resources_to_create, client_id)

# Define first running task
@ray.remote
def task():
    print("Running first task")
    time.sleep(5)
    print("Completed first task")
    return str(ray.worker.global_worker.plasma_client.store_socket_name)

# Now we want to run instances of long_task, spread across the three nodes.
# Simply specify resource load_balancer as a requirement and submit
print("Launching tasks")
start_time = time.time()
task_results = []
task_results.append(task._remote(args=[], resources={"anti_affinity": 1}))
task_results.append(task._remote(args=[], resources={"anti_affinity": 1}))
print("Getting task results")
res = ray.get(task_results)
end_time = time.time()
print("Plasma store sockets:  " + str(Counter(res)))
print("Time taken = " + str(end_time-start_time))