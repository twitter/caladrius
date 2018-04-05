""" This is the routing logic for the modelling section of the Caladrius API
"""

import hug

from caladrius.api.model.topology import root as topology_root
from caladrius.api.model.traffic import root as traffic_root

@hug.extend_api("/topology")
def model_topology_api():
    return [topology_root]

@hug.extend_api("/traffic")
def model_traffic_api():
    return [traffic_root]
