""" This is the rooting logic for the storm modelling API """

import hug

@hug.get("/{topology_id}")
def topology(topology_id: str):
    return f"You want info on the Storm topology: {topology_id}!"
