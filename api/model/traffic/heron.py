""" This is the rooting logic for the Apache Heron traffic modelling API """

import logging

import hug

from falcon import HTTP_202

LOG: logging.Logger = logging.getLogger(__name__)

@hug.get("/{topology_id}", examples="duration=120&units=m")
def topology_traffic(topology_id: str, duration: hug.types.number,
                     units: str=None):

    if not units:
        units = "m"

    LOG.info("%s%s traffic prediction requested for Heron topology: %s",
             str(duration), units, topology_id)

    return (f"You want an incoming traffic prediction for the next "
            f"{duration}{units} for the currently running Heron "
            f"topology: {topology_id}")
