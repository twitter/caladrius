""" This module contains methods for extracting information from the Heron
Tracker services REST API:
https://twitter.github.io/heron/docs/operators/heron-tracker-api/
"""
import logging

from typing import List, Dict, Union, Any

import requests

LOG: logging.Logger = logging.getLogger(__name__)

#pylint: disable=too-many-arguments

def get_logical_plan(tracker_url: str, cluster: str, environ: str,
                     topology: str) -> Dict[str, Union[int, str]]:
    """ Get the logical plan dictionary from the heron tracker api.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        A dictionary containing details of the spouts and bolts.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    LOG.info("Fetching logical plan for topology: %s", topology)

    logical_url: str = tracker_url + "/topologies/logicalplan"

    response: requests.Response = requests.get(logical_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        LOG.error("Logical plan request for topology: %s , cluster: %s, "
                  "environment: %s failed with error code: %s", topology,
                  cluster, environ, str(response.status_code))
        raise err

    return response.json()["result"]

def get_physical_plan(tracker_url: str, cluster: str, environ: str,
                      topology: str) -> Dict[str, Union[int, str]]:
    """ Get the logical plan dictionary from the heron tracker api.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        A dictionary containing details of the containers and stream managers
        for the specified topology.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    LOG.info("Fetching physical plan for topology: %s", topology)

    physical_url: str = tracker_url + "/topologies/physicalplan"

    response: requests.Response = requests.get(physical_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        LOG.error("Physical plan request for topology: %s , cluster: %s, "
                  "environment: %s failed with error code: %s", topology,
                  cluster, environ, str(response.status_code))
        raise err

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
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        A dictionary containing all the details the tracker has on the
        specified topology.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    info_url: str = tracker_url + "/topologies/info"

    response: requests.Response = requests.get(info_url,
                                               params={"cluster" : cluster,
                                                       "environ" : environ,
                                                       "topology" : topology})

    response.raise_for_status()

    LOG.info("Fetched information for topology: %s", topology)

    return response.json()["result"]

def get_metrics(tracker_url: str, cluster: str, environ: str, topology: str,
                component: str, interval: int, metrics: Union[str, List[str]]
               ) -> Dict[str, Any]:
    """ Gets aggregated metrics for the specified component in the specified
    topology. Metrics are aggregated over the supplied interval.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.
        component (str): The name of the topology component.
        interval (int): The period in seconds (counted backwards from now) over
                        which metrics should be aggregated.
        metrics (str or list):  A metrics name or list of metrics names to be
                                returned.

    Returns:
        A dictionary containing aggregate metrics for the specified topology
        component.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    payload: Dict[str, Union[str, int]] = {
        "cluster" : cluster, "environ" : environ, "topology" : topology,
        "component" : component, "interval" : interval, "metricname" : metrics}

    metrics_url: str = tracker_url + "/topologies/metrics"

    response: requests.Response = requests.get(metrics_url, params=payload)

    response.raise_for_status()

    LOG.info("Fetched aggregate summaries for metrics: %s from topology: %s",
             str(metrics), topology)

    return response.json()["result"]

def get_metrics_timeline(tracker_url: str, cluster: str, environ: str,
                         topology: str, component: str, start_time: int,
                         end_time: int, metrics: Union[str, List[str]]
                        ) -> Dict[str, Any]:
    """ Gets metrics timelines for the specified component in the specified
    topology. Metrics are aggregated into one minuet intervals keyed by POSIX
    UTC timestamps (in seconds) for the start of each interval.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.
        component (str): The name of the topology component.
        start_time (int):   The start point of the timeline. This should be a
                            UTC POSIX timestamp in seconds.
        end_time (int): The end point of the timeline. This should be a UTC
                        POSIX timestamp in seconds.
        metrics (str or list):  A metrics name or list of metrics names to be
                                returned.

    Returns:
        A dictionary containing metrics timelines for the specified topology
        component.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    duration: int = end_time - start_time

    if duration > 10800:
        LOG.warning("Duration of metrics timeline interval for metrics: %s of "
                    "topology: %s was greater than the 3 hours of data stored "
                    "by the Topology Master", str(metrics), topology)

    payload: Dict[str, Union[str, int]] = {
        "cluster" : cluster, "environ" : environ, "topology" : topology,
        "component" : component, "starttime" : start_time,
        "endtime" : end_time, "metricname" : metrics}

    metrics_timeline_url: str = tracker_url + "/topologies/metricstimeline"

    response: requests.Response = requests.get(metrics_timeline_url,
                                               params=payload)
    response.raise_for_status()

    LOG.info("Fetched timeline(s) for metric(s): %s from topology: %s over a "
             "period of %d seconds", str(metrics), topology, duration)

    return response.json()["result"]

def issue_metrics_query(tracker_url: str, cluster: str, environ: str,
                        topology: str, start_time: int, end_time: int,
                        query: str) -> Dict[str, Any]:
    """ Issues the supplied query and runs it against the metrics for the
    supplied topology in the interval defined by the start and end times. For
    query syntax see:

    https://apache.github.io/incubator-heron/docs/operators/heron-tracker-api/#metricsquery

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.
        start_time (int):   The start point of the timeline. This should be a
                            UTC POSIX timestamp in seconds.
        start_time (int):   The end point of the timeline. This should be a
                            UTC POSIX timestamp in seconds.
        query (str):    The query string to be issued to the Tracker API.

    Returns:
        A dictionary containing the query results.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    duration: int = end_time - start_time

    if duration > 10800:
        LOG.warning("Duration of metrics timeline interval for metrics query: "
                    " %s for topology: %s was greater than the 3 hours of "
                    " data stored by the Topology Master", query, topology)

    payload: Dict[str, Union[str, int]] = {
        "cluster" : cluster, "environ" : environ, "topology" : topology,
        "starttime" : start_time, "endtime" : end_time, "query" : query}

    metrics_query_url: str = tracker_url + "/topologies/metricsquery"

    response: requests.Response = requests.get(metrics_query_url,
                                               params=payload)
    response.raise_for_status()

    LOG.info("Fetched results of query: %s from topology: %s over a "
             "period of %d seconds", query, topology, duration)

    return response.json()["result"]
