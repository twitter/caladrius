""" This module contains methods for extracting information from the Heron
Tracker services REST API:
https://twitter.github.io/heron/docs/operators/heron-tracker-api/
"""
import logging

from typing import List, Dict, Union

import requests

LOG: logging.Logger = logging.getLogger(__name__)

def get_logical_plan(tracker_url: str, cluster: str, environ: str,
                     topology: str) -> Dict[str, Union[int, str]]:
    """ Get the logical plan dictionary from the heron tracker api.

    Arguments:
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        A dictionary containing details of the spouts and bolts.
    """

    logical_url: str = tracker_url + "/topologies/logicalplan"

    response: requests.Response = requests.get(logical_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    response.raise_for_status()

    LOG.info("Fetched logical plan for topology: %s", topology)

    return response.json()["result"]

def get_physical_plan(tracker_url: str, cluster: str, environ: str,
                      topology: str) -> Dict[str, Union[int, str]]:
    """ Get the logical plan dictionary from the heron tracker api.

    Arguments:
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        A dictionary containing details of the containers and stream managers
        for the specified topology.
    """

    physical_url: str = tracker_url + "/topologies/physicalplan"

    response: requests.Response = requests.get(physical_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    response.raise_for_status()

    LOG.info("Fetched physical plan for topology: %s", topology)

    return response.json()["result"]

def parse_instance_name(instance_name: str):
    """ Parses the instance name string returned by the Heron Tracker API into
    a dictionary with instance information.

    Arguments:
        instance_name (str): Instance name string in the form:
                             container_<container_num>_<component>_<task_id>

    Returns:
        A dictionary with the following keys:
            container : The container id as a integer
            component : The component name string
            task_id : The instances task id as an integer
    """

    parts: List[str] = instance_name.split("_")

    return {"container" : int(parts[1]), "component" : parts[2],
            "task_id" : int(parts[3])}

def get_topology_info(tracker_url: str, cluster: str, environ: str,
                      topology: str) -> Dict[str, Union[int, str]]:
    """ Get the information dictionary from the heron tracker api. This
    contains the logical and physical plans as well as other information on the
    topology.

    Arguments:
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        A dictionary containing all the details the tracker has on the
        specified topology.
    """

    info_url: str = tracker_url + "/topologies/info"

    response: requests.Response = requests.get(info_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    response.raise_for_status()

    LOG.info("Fetched information for topology: %s", topology)

    return response.json()["result"]
