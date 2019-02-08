import math

import ray
import time
import ray.test.cluster_utils

# This example demonstrates load_balancing where the number of tasks and nodes are fixed and known at the start
NUM_NODES = 3
NUM_TASKS = 6
NODE_INIT_RESOURCES = {"CPU": 100} # So we're not bottlenecked by CPU

# Initialize cluster
print("Initializing cluster..")
cluster = ray.test.cluster_utils.Cluster()
for i in range(NUM_NODES):
    cluster.add_node(resources=NODE_INIT_RESOURCES)
cluster.wait_for_nodes()

print("Cluster init complete, connecting driver")
ray.init(redis_address=cluster.redis_address)

print("Parsing client table and fetching clientIds")
client_table = ray.global_state.client_table()
client_ids = [client["ClientID"] for client in client_table]
print("ClientIds: " + str(",".join(client_ids)))

print("Creating loadbalancer resource on nodes")
num_resources_to_create = math.ceil(NUM_TASKS/NUM_NODES)
for client_id in client_ids:
    ray.experimental.create_resource("load_balancer", num_resources_to_create, client_id)

# time.sleep(1)
# Define long running task
@ray.remote
def long_task(task_id):
    print("Running %d" % task_id)
    time.sleep(10)
    return str(ray.get_resource_ids())

# Now we want to run 3 instances of long_task, spread across the three nodes.
# Simply specify resource load_balancer as a requirement and submit
print("Launching tasks")
task_results = []
for i in range(0, NUM_TASKS):
    task_results.append(long_task._remote(args=[i], resources={"load_balancer": 1}))

print("Getting task results")
res = ray.get(task_results)
print("Results " + str(res))