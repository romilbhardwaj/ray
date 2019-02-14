import ray

def create_resource(resource_name, capacity, client_id=None):
    """
    Creates a custom resource for a target clientId. If the resource already exists, it's capacity is updated to the new value.
    If ClientID is not specified or set to None, the resource is created on the local client where the actor is running.
    :param resource_name: Name of the resource to be created
    :type str
    :param capacity: Capacity of the new resource.
    :type float
    :param client_id:
    :type str
    :return: None
    """
    if client_id != None:
        client_id_obj = ray.ClientID(ray.utils.hex_to_binary(client_id))
    else:
        client_id_obj = ray.ClientID.nil()
    return ray.worker.global_worker.raylet_client.create_resource(resource_name, capacity, client_id_obj)      #TODO(romilb): Should this return none when client id not exist?


def delete_resource(resource_name, client_id=None):
    """
    Deletes the specified custom resource on the target clientId.
    If ClientID is not specified or set to None, the resource is deleted on the local client where the actor is running.
    :param resource_name: Name of the resource to be deleted
    :type str
    :param client_id:
    :type str
    :return: None
    """
    if client_id != None:
        client_id_obj = ray.ClientID(ray.utils.hex_to_binary(client_id))
    else:
        client_id_obj = ray.ClientID.nil()
    return ray.worker.global_worker.raylet_client.delete_resource(resource_name, client_id_obj)