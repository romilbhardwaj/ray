from collections import Counter

import ray
import time
import ray.test.cluster_utils

# This example demonstrates load_balancing where the number of tasks and nodes are not known at the start - tasks may arrive dynamically.
NUM_NODES = 2
NUM_TASKS = 10

# Initialize cluster
print("Initializing cluster..")
cluster = ray.test.cluster_utils.Cluster()
#cluster = ray.test.cluster_utils.Cluster(initialize_head=True, head_node_args={"resources": NODE_INIT_RESOURCES})
for i in range(NUM_NODES):
    cluster.add_node(num_cpus=8)

cluster.wait_for_nodes()

print("Cluster init complete, connecting driver")
ray.init(redis_address=cluster.redis_address)

print("Parsing client table and fetching clientIds")
client_table = ray.global_state.client_table()
client_ids = [client["ClientID"] for client in client_table]
print("ClientIds: " + str(",".join(client_ids)))

print("Creating loadbalancer resource on nodes")
num_resources_to_create = 1 # We don't know how many resources to create, so start with one and our watcher thread keeps adding
for client_id in client_ids:
    ray.experimental.create_resource("load_balancer", num_resources_to_create, client_id)

# Define long running task
@ray.remote
def long_task(task_id):
    print("Running %d" % task_id)
    time.sleep(10)
    print("Completed %d" % task_id)
    return str(ray.worker.global_worker.plasma_client.store_socket_name)

# Define a watcher which adds load_balancer resources as necessary.
@ray.remote
def resource_watcher():
    client_table = ray.global_state.client_table()
    client_ids = [client["ClientID"] for client in client_table]
    while True:
        avail_res = ray.global_state.available_resources()
        print(avail_res)
        if avail_res['load_balancer'] == 0:
            print("[ResourceWatcher]Out of load_balancer resources, creating more.")
            current_loadbalancer_total = ray.global_state.cluster_resources()['load_balancer']/len(client_ids)  # Assumes nodes do not drop out. If they do, then this should fetch client_ids in every loop check
            for client_id in client_ids:
                ray.experimental.create_resource('load_balancer', current_loadbalancer_total + 1, client_id)
        time.sleep(0.5)
    return str(ray.get_resource_ids())


# Run resource watcher
resource_watcher.remote()

# Now we want to run instances of long_task, spread across the three nodes.
# Simply specify resource load_balancer as a requirement and submit
print("Launching tasks")
start_time = time.time()
task_results = []
for i in range(0, NUM_TASKS):
    task_results.append(long_task._remote(args=[i], resources={"load_balancer": 1}))


print("Getting task results")
res = ray.get(task_results)
end_time = time.time()
print("Plasma store sockets:  " + str(Counter(res)))
# print("Time taken = " + str(end_time-start_time))