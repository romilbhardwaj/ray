import math

import ray
import time
import ray.tests.cluster_utils
from collections import Counter


class PRIORITIES:
    ALWAYS = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3


def get_priority_resource_requirement(canoncial_priority):
    # Converts canonical priority to a resource requirement
    return math.pow(10, canoncial_priority)


# This example demonstrates priority based scheduling (without preemption)
NUM_NODES = 1
LOWEST_CANONICAL_PRIORITY = PRIORITIES.LOW  # Higher number = lower priority
PRIORITY_CAPACITY = get_priority_resource_requirement(LOWEST_CANONICAL_PRIORITY)

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

print("Creating priority resource with capacity %d on nodes." % PRIORITY_CAPACITY)
for client_id in client_ids:
    ray.experimental.create_resource("priority", PRIORITY_CAPACITY, client_id)


# Define task
@ray.remote
def task(task_id):
    print("Running %d" % task_id)
    time.sleep(5)
    return str(ray.worker.global_worker.plasma_client.store_socket_name)


print("Launching tasks")
task_results = []
start_time = time.time()

# Launch highest priority task
for i in range(0, 2):
    task_results.append(task._remote(args=[i], resources={"priority": 1}))

# Launch low priority task
task_results.append(task._remote(args=[42], resources={"priority": get_priority_resource_requirement(PRIORITIES.LOW)}))  # This will get scheduled only when all compelete

print("Getting task results")
res = ray.get(task_results)
end_time = time.time()
print("Plasma store sockets:  " + str(Counter(res)))
print("Time taken = " + str(end_time - start_time))
