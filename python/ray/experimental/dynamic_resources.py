import ray


def set_resource(resource_name, capacity, client_id=None):
    """
    Creates, updates or deletes a custom resource for a target clientId. If the resource already exists, it's capacity is updated to the new value.
    If the capacity is set to 0, the resource is deleted.
    If ClientID is not specified or set to None, the resource is created on the local client where the actor is running.
    :param resource_name: Name of the resource to be created
    :type str
    :param capacity: Capacity of the new resource. Resource is deleted if capacity is 0.
    :type float
    :param client_id: ClientId where the resource is to be created or updated.
    :type str
    :return: None
    """
    if client_id != None:
        client_id_obj = ray.ClientID(ray.utils.hex_to_binary(client_id))
    else:
        client_id_obj = ray.ClientID.nil()
    return ray.worker.global_worker.raylet_client.set_resource(
        resource_name, capacity, client_id_obj)