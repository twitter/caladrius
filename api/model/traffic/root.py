""" This is the routing logic for the topology performance modelling section of
the Caladrius API """

import hug

from caladrius.api.model.traffic import heron
from caladrius.api.model.traffic import storm

@hug.extend_api("/heron")
def model_heron_traffic_api():
    return [heron]

@hug.extend_api("/storm")
def model_storm_traffic_api():
    return [storm]
