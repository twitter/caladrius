""" This module contains methods for extracting information from the Heron
Tracker services REST API:
https://twitter.github.io/heron/docs/operators/heron-tracker-api/
"""
import logging

from typing import Dict, Union

import requests

LOG: logging.Logger = logging.getLogger(__name__)

HERON_TRACKER_URL: str = "http://heron-tracker-new.prod.heron.service.smf1.twitter.com"

def get_logical_plan(cluster: str, environ: str, topology: str
                    ) -> Dict[str, Union[int, str]]:
    """ Get the logical plan dictionary from the heron tracker api.

    Arguments:
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        A dictionary containing details of the spouts and bolts.
    """

    logical_url = HERON_TRACKER_URL + "/topologies/logicalplan"

    response: requests.Response = requests.get(logical_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    response.raise_for_status()

    LOG.info("Fetched logical plan for topology: %s", topology)

    return response.json()["result"]

def get_physical_plan(cluster: str, environ: str, topology: str
                     ) -> Dict[str, Union[int, str]]:
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

    physical_url = HERON_TRACKER_URL + "/topologies/physicalplan"

    response: requests.Response = requests.get(physical_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    response.raise_for_status()

    LOG.info("Fetched physical plan for topology: %s", topology)

    return response.json()["result"]
