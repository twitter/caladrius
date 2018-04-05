""" This is the rooting logic for the Apache Heron topology performance
modelling API """

import logging

import hug

from falcon import HTTP_202

LOG: logging.Logger = logging.getLogger(__name__)

@hug.get("/{topology_id}")
def topology(topology_id: str):
    return f"You want info on the Heron topology: {topology_id}"

@hug.get("/current/{topology_id}")
def current_topology(topology_id: str):
    return (f"You want info on the currently running Heron topology: "
            f"{topology_id}")

@hug.get("/proposed/{topology_id}", examples="model_id=1234")
def get_proposed_topology(model_id: int, topology_id: str):
    msg:str = (f"Results requested for model: {model_id} of "
               f"topology: {topology_id}")
    LOG.info(msg)
    return msg

@hug.post("/proposed/{topology_id}")
def proposed_topology(body, topology_id: str, response):
    response.status = HTTP_202
