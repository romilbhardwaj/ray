import math
from collections import Counter

import ray
import time
import ray.test.cluster_utils

# This example demonstrates co-location by creating a resource where the first task lands.
NUM_NODES = 3

# Initialize cluster
print("Initializing cluster..")
cluster = ray.test.cluster_utils.Cluster()
for i in range(NUM_NODES):
    cluster.add_node(num_cpus=8)
cluster.wait_for_nodes()

print("Cluster init complete, connecting driver")
ray.init(redis_address=cluster.redis_address)

print("Parsing client table and fetching clientIds")
client_table = ray.global_state.client_table()
client_ids = [client["ClientID"] for client in client_table]
print("ClientIds: " + str(",".join(client_ids)))

# Define first running task
@ray.remote
def first_task():
    print("Running first task")
    print("Create resource locally so that the other task can colocate")
    ray.experimental.create_resource("my_colocation", 1)
    print("Resource created, sleeping now.")
    time.sleep(10)
    print("Completed first task")
    return str(ray.worker.global_worker.plasma_client.store_socket_name)

@ray.remote
def co_located_task():
    print("Running co-located task")
    time.sleep(10)
    print("Completed co-located task")
    return str(ray.worker.global_worker.plasma_client.store_socket_name)

# Now we want to run instances of long_task, spread across the three nodes.
# Simply specify resource load_balancer as a requirement and submit
print("Launching tasks")
start_time = time.time()
task_results = []
task_results.append(first_task.remote())
task_results.append(co_located_task._remote(args=[], resources={"my_colocation": 1}))
print("Getting task results")
res = ray.get(task_results)
end_time = time.time()
print("Plasma store sockets:  " + str(Counter(res)))
print("Time taken = " + str(end_time-start_time))