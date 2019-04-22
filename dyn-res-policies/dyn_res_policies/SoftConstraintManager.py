import ray

import time


@ray.remote
class SoftConstraintManagerState(object):
    def __init__(self):
        self.placement_successful = {}  # Map from task id to bool, used as a check if task has already been launched
        self.task_id_counter = 0

    def launch_callback(self, task_id):
        f = open('/tmp/log.txt', 'a', buffering=1)
        if task_id not in self.placement_successful:
            f.write("[LaunchCallback] Task {} is not valid.\n".format(task_id))
            return False
        if self.placement_successful[task_id]:
            f.write("[LaunchCallback] Task {} already launched, returning false.\n".format(task_id))
            return False
        else:
            f.write("[LaunchCallback] Task {} launched successfully. Marking as launched.\n".format(task_id))
            self.placement_successful[task_id] = True
            return True

    def get_task_id(self):
        f = open('/tmp/log.txt', 'a', buffering=1)
        retval = self.task_id_counter
        self.task_id_counter += 1
        self.placement_successful[retval] = False
        f.write("[GetTaskId] Returning {}. Incremented task_id_counter is {}.\n".format(retval, self.task_id_counter))
        return retval

    def is_placement_successful(self, task_id):
        return self.placement_successful[task_id]

class SoftConstraintManager(object):
    def __init__(self, launch_wait_time = 5):
        '''

        :param launch_wait_time: Duration to wait before moving on to the next soft constraint
        '''
        self.state_actor = SoftConstraintManagerState.remote()
        self.launch_wait_time = launch_wait_time

    @staticmethod
    @ray.remote
    def func_wrapper(func, func_args, task_id, state_actor):
        f = open('/tmp/log.txt', 'a', buffering=1)
        f.write("[FuncWrapper]{} launched!\n".format(task_id))
        is_valid_run = ray.get(state_actor.launch_callback.remote(task_id))
        if is_valid_run:
            f.write("[FuncWrapper] This is a valid run!\n")
            return func(*func_args)
        return None

    def launch_task(self, ord_soft_constraints, hard_constraints, func, args):
        '''
        :param ord_soft_constraints: List of dicts, specifying soft constraint priority order
        :param hard_constraints: dict, specifying hard constraints
        :param func: Function to run
        :return: Blocks until task is launched
        '''

        f = open('/tmp/log.txt', 'a', buffering=1)
        this_task_id = ray.get(self.state_actor.get_task_id.remote())

        for soft_constraint_instance in ord_soft_constraints:
            f.write("[LaunchTask] Trying soft constraint {}, task_id {}".format(soft_constraint_instance, this_task_id) + "\n")
            combined_constraint = {**soft_constraint_instance, **hard_constraints}  # Hard will override any conflicting soft constraints
            attempt = SoftConstraintManager.func_wrapper._remote(args=[func, args, this_task_id, self.state_actor], resources=combined_constraint)

            # Wait for a few seconds for launch_callback to be called and self.placement_successful to be updated.
            time.sleep(5)
            placed = ray.get(self.state_actor.is_placement_successful.remote(this_task_id))
            if placed:
                f.write("[LaunchTask] Soft constraint {} satisfied. Task launched.".format(soft_constraint_instance) + "\n")
                return [attempt, combined_constraint]

        f.write("[LaunchTask] None of the soft-constraint was satisfied. Launching with only hard constraint and returning." + "\n")
        attempt = SoftConstraintManager.func_wrapper._remote(args=[func, args, this_task_id, self.state_actor], resources=hard_constraints)
        return [attempt, hard_constraints]