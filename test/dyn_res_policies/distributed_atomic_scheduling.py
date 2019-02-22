import ray
import time
import ray.test.cluster_utils

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

# Define reservation actor that holds resources
@ray.remote
class reservation_actor():
    # Create colocation resource
    ray.experimental.create_resource("reserved_node", 1)
    def alive(self):
        return True

@ray.remote
def distributed_atomic_task():
    time.sleep(5)
    return 1

# Launch the data_generator to create the data at some remote node
print("Launching tasks")
start_time = time.time()

NUM_NODES_TO_RESERVE = 3
MAX_RESERVATION_TRIES = 10

reservation_actors = []
reservation_alive_checks = []
for i in range(0,NUM_NODES_TO_RESERVE):
    act = reservation_actor._remote(args=[], kwargs={}, num_cpus=6)
    reservation_alive_checks.append(act.alive.remote())
    reservation_actors.append(act)

ready_ids, waiting_ids = ray.wait(reservation_alive_checks, num_returns=NUM_NODES_TO_RESERVE, timeout=0.5)
try_count = 0
while len(ready_ids) != NUM_NODES_TO_RESERVE:
    print("Waiting for reservations to succeed. Try count: %d" % try_count)
    ready_ids, waiting_ids = ray.wait(reservation_alive_checks, num_returns=NUM_NODES_TO_RESERVE, timeout=1)
    try_count+=1;
    if try_count == MAX_RESERVATION_TRIES:
        # Cleanup - kill running actors
        for act in reservation_actors:
            del act
        raise Exception("Reservation failed.")

print("Reservation successful.")
results = []
# Launch tasks on reserved nodes
for i in range(0,2):
    results.append(distributed_atomic_task._remote(args=[], resources={"reserved_node": 1}))

print("Getting task results")
res = ray.get(results)
end_time = time.time()
print("Result - %s" % str(res))
print("Time taken = %s" % str(end_time-start_time))