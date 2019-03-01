import ray
import time
import ray.test.cluster_utils
from SoftConstraintManager import SoftConstraintManager
# This example demonstrates the implementation of ordinal soft constraints

# Initialize cluster and create two nodes with resources
print("Initializing cluster..")
cluster = ray.test.cluster_utils.Cluster()
node1_resources = {'my_data1': 1, 'my_gpu': 1}
node2_resources = {'my_data2': 1, 'my_tpu': 1}
cluster.add_node(num_cpus=8, resources=node1_resources)
cluster.add_node(num_cpus=8, resources=node2_resources)

cluster.wait_for_nodes()

print("Cluster init complete, connecting driver")
ray.init(redis_address=cluster.redis_address)

scm = SoftConstraintManager(launch_wait_time=5)   # Wait for 5 seconds before trying the next ordinal soft constraint

# Define ordinal soft constraints:
ord_soft_constraints = [
    {'my_data1': 1, 'my_tpu': 1},
    {'my_data2': 1, 'my_tpu': 1},
    {'my_data1': 1}
]
hard_constraints = {'my_gpu': 1}

# Note that this method is not decorated with ray.remote. The SoftConstraintManager takes care of this.
def my_task(i):
    return i

launched_task, launched_resources = scm.launch_task(ord_soft_constraints, hard_constraints, my_task, [1])
print("Launched task. Resources constraints satisfied: {}".format(launched_resources))
result = ray.get(launched_task)
print("Result is {}".format(result))