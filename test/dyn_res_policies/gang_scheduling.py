import math

import ray
import time
import ray.test.cluster_utils

# This example demonstrates gang scheduling by making sure that either all jobs complete, and if a node fails then all jobs are terminated.
NUM_NODES = 2
GANG_SIZE = 7
NODE_INIT_RESOURCES = {"CPU": 16} # So we're not bottlenecked by CPU

nodes = []

# Initialize cluster
print("Initializing cluster..")
cluster = ray.test.cluster_utils.Cluster()
for i in range(NUM_NODES):
    nodes.append(cluster.add_node(resources=NODE_INIT_RESOURCES))
cluster.wait_for_nodes()

print("Cluster init complete, connecting driver")
ray.init(redis_address=cluster.redis_address)

print("Parsing client table and fetching clientIds")
client_table = ray.global_state.client_table()
client_ids = [client["ClientID"] for client in client_table]
print("ClientIds: " + str(",".join(client_ids)))

print("Creating gang scheduling resource on nodes")
quotient, remainder = divmod(GANG_SIZE, NUM_NODES)
for i in range(0, NUM_NODES):
    if i >= remainder:
        num_resources_to_create = quotient + 1
    else:
        num_resources_to_create = quotient
    print(num_resources_to_create)
    ray.experimental.create_resource("my_gang", num_resources_to_create, client_ids[i])

time.sleep(5)
# Define task
@ray.remote
def long_task(task_id):
    print("Running %d" % task_id)
    time.sleep(10)
    return True

# Simply specify resource my_gang as a requirement and with capacity 1
print("Launching tasks")
start_time = time.time()
task_ids = []
for i in range(0, GANG_SIZE):
    task_ids.append(long_task._remote(args=[i], resources={"my_gang": 1}))
ready_ids, waiting_ids = ray.wait(task_ids, num_returns=GANG_SIZE, timeout=500)

# Loop to check if gang completed. If resource availability drops in between, exception is raised
i=0
while len(ready_ids) != GANG_SIZE:
    print("Waiting for tasks to complete or nodes to fail.")
    ready_ids, waiting_ids = ray.wait(task_ids, num_returns=GANG_SIZE, timeout=1000)
    my_gang_resources = ray.global_state.cluster_resources()["my_gang"]
    print("My gang resources: %d" % my_gang_resources)
    if my_gang_resources < GANG_SIZE:
        raise Exception("My_gang resource size dropped! Some node probably died. At this point, the application would handle this exception and either kill all tasks or restart with new resources")
    i+=1
    # Uncomment these lines to simulate failure at iteration 3.
    # if i==3:
    #     cluster.remove_node(nodes[1])

print("Gang completed. Getting task results.")
res = ray.get(task_ids)
end_time = time.time()
print("Results " + str(res))
print("Time taken = " + str(end_time-start_time))