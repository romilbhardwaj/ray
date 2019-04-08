from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import sys
import time
import numpy as np
import argparse

from ray.utils import binary_to_object_id


@ray.remote
def noop(obj):
    # a task that takes a dependency on obj and does nothing
    return 0


@ray.remote(resources={'node1': 1})
def gen_node1(num_small, num_big, bytes_small, bytes_big):
    # generate num_small small objects and num_big big objects on this node
    small_obj = np.random.randint(0xff, size=bytes_small, dtype='uint8')
    big_obj = np.random.randint(0xff, size=bytes_big, dtype='uint8')

    small_oids = [ray.put(small_obj) for _ in range(num_small)]
    big_oids = [ray.put(big_obj) for _ in range(num_big)]

    return (small_oids, big_oids)


@ray.remote(resources={'node2': 1})
def gen_node2(num_small, num_big, bytes_small, bytes_big):
    # generate num_small small objects and num_big big objects on this node
    small_obj = np.random.randint(0xff, size=bytes_small, dtype='uint8')
    big_obj = np.random.randint(0xff, size=bytes_big, dtype='uint8')

    small_oids = [ray.put(small_obj) for _ in range(num_small)]
    big_oids = [ray.put(big_obj) for _ in range(num_big)]

    return (small_oids, big_oids)


def gen_data(num_small, num_big, bytes_small, bytes_big):
    t1 = time.time()
    node1oid = gen_node1.remote(num_small >> 1, num_big >> 1, bytes_small, bytes_big)
    node2oid = gen_node2.remote(num_small >> 1, num_big >> 1, bytes_small, bytes_big)
    # unpack the generated data futures tuple
    (small_oids1, big_oids1) = ray.get(node1oid)
    (small_oids2, big_oids2) = ray.get(node2oid)
    t2 = time.time()
    print("Generated {},{} small and {},{} big objects sized {},{} bytes in {} seconds"
          .format(len(small_oids1), len(small_oids2),
                  len(big_oids1), len(big_oids2),
                  bytes_small, bytes_big,
                  t2 - t1))

    return (small_oids1, small_oids2, big_oids1, big_oids2)


def get_clientid_of_objectid(obj_id):
    obj_data = ray.global_state.object_table(obj_id)
    while (not obj_data):
        obj_data = ray.global_state.object_table(obj_id)  # wait for object to be created
    return binary_to_object_id(obj_data['Manager']).hex()


def warmup():
    # Warmup machines by running noop before the experiment on both nodes
    warmup_tasks = []
    warmup_tasks.append(noop._remote(args=[1], resources={'node1': 1}))
    warmup_tasks.append(noop._remote(args=[1], resources={'node2': 1}))
    ray.get(warmup_tasks)
    return True


def write_logs(data, filename):
    with open(filename, 'a') as f:
        for row in data:
            f.write(",".join(str(v) for v in row) + "\n")


def main_tasks(num_small, num_big, bytes_small, bytes_big, locality_aware):
    small_oids1, small_oids2, big_oids1, big_oids2 = \
        gen_data(num_small, num_big, bytes_small, bytes_big)

    small_oids = small_oids1 + small_oids2
    big_oids = big_oids1 + big_oids2
    assert (len(small_oids) == len(small_oids1) + len(small_oids2))
    assert (len(big_oids) == len(big_oids1) + len(big_oids2))

    all_oids = small_oids + big_oids
    assert (len(all_oids) == len(small_oids) + len(big_oids))
    np.random.shuffle(all_oids)  # randomly permute all objects

    print("Warming up nodes")
    warmup()

    # submit n+N tasks
    if locality_aware:
        print("Running locality aware scheduling")
    else:
        print("Running random scheduling")

    t1 = time.time()
    if locality_aware:
        task_oids = []
        for oid in all_oids:
            destination_client = get_clientid_of_objectid(oid)
            res_name = oid.hex()
            ray.experimental.create_resource(res_name, 1, destination_client)
            task_oids.append(noop._remote(args=[oid], resources={res_name: 1}))
    else:
        task_oids = [noop.remote(oid) for oid in all_oids]
    t2 = time.time()
    r, p = ray.wait(task_oids, num_returns=len(task_oids))
    t3 = time.time()

    submission_time = t2 - t1
    execution_time = t3 - t1
    total_task_count = len(all_oids)
    trial_type = "locality_aware" if locality_aware else "random"
    fname ='{}_{}.csv'.format(trial_type, bytes_big)

    print("Submitted {} tasks in {} seconds.".format(len(all_oids), t2 - t1))
    print("Executed {} tasks in {} sec {} tasks/sec".format(len(r), t3 - t1, len(r) / (t3 - t1)))

    data = [[trial_type, total_task_count, bytes_big, submission_time, execution_time]]
    write_logs(data, filename=fname)


def main(num_big, num_small, bytes_small, bytes_big, redis_address, locality_aware):
    print("Starting benchmark with {} big objects, {} small objects attached to {}. Locality aware: {}"
          .format(num_big, num_small, redis_address, locality_aware))
    #
    if redis_address is None:
        import ray.tests.cluster_utils

        print("Initializing cluster.")
        NUM_NODES = 3
        cluster = ray.tests.cluster_utils.Cluster()
        for i in range(NUM_NODES):
            cluster.add_node(num_cpus=4, resources={'node{}'.format(i): 1})

        cluster.wait_for_nodes()
        print("Cluster init complete, connecting driver")
        ray.init(redis_address=cluster.redis_address)
    else:
        # attach to an existing Ray cluster
        ray.init(redis_address=redis_address)

    # call the task based benchmark
    main_tasks(num_small, num_big, bytes_small, bytes_big, locality_aware)
    cluster.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ray locality experiment')
    parser.add_argument('--num-big', default=10, type=int, help='The number of big objects to use.')
    parser.add_argument('--num-small', default=0, type=int, help='The number of small objects to use.')
    parser.add_argument('--bytes-small', default=1 << 10, type=int, help='The size of small objects in bytes.')
    parser.add_argument('--bytes-big', default=1 << 20, type=int, help='The size of big objects in bytes.')
    parser.add_argument('--redis-address', default=None, type=str,
                        help='The address of the redis server.')
    parser.add_argument('--locality-aware', action='store_true',
                        help='Enforce data-locality. Uses random placement if not specified.')

    args = parser.parse_args()

    main(**vars(args))
