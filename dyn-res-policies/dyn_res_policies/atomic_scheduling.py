import ray
import time
import ray.tests.cluster_utils

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

# Define reservation actor that holds resources
@ray.remote
class reservation_actor():
    # Create colocation resource
    ray.experimental.create_resource("reserved_node", 2)

@ray.remote
def gpu_task():
    time.sleep(5)
    return 2

@ray.remote
def cpu_task():
    time.sleep(5)
    return 1

# Launch the data_generator to create the data at some remote node
print("Launching tasks")
start_time = time.time()

actor = reservation_actor._remote(args=[], kwargs={}, num_cpus=4)
colocated_cpu = cpu_task._remote(args=[], resources={"reserved_node": 1})
colocated_gpu = gpu_task._remote(args=[], resources={"reserved_node": 1})
print("Getting task results")
res = ray.get([colocated_cpu, colocated_gpu])
end_time = time.time()
print("Result - %s" % str(res))
print("Time taken = %s" % str(end_time-start_time))