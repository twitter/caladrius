# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for extracting and analysing information from
the Heron Tracker services REST API:
https://twitter.github.io/heron/docs/operators/heron-tracker-api/
"""
import logging

from typing import List, Dict, Union, Any, Tuple, cast

import requests

import pandas as pd

LOG: logging.Logger = logging.getLogger(__name__)

# pylint: disable=too-many-arguments


def get_topologies(tracker_url: str, cluster: str = None,
                   environ: str = None) -> pd.DataFrame:
    """ Gets the details from the Heron Tracker API of all registered
    topologies. The results can be limited to a specific cluster and
    environment.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  Optional cluster to limit search results to.
        environ (str):  Optional environment to limit the search to (eg. prod,
                        devel, test, etc).

    Returns:
        pandas.DataFrame:   A DataFrame containing details of all topologies
        registered with the Heron tracker. This has the columns for:

        * topology: The topology ID.
        * cluster: The cluster the topology is running on.
        * environ: The environment the topology is running in.
        * user: The user that uploaded the topology.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """
    LOG.info("Fetching list of available topologies")

    topo_url: str = tracker_url + "/topologies"

    response: requests.Response = requests.get(topo_url,
                                               params={"cluster": cluster,
                                                       "environ": environ})
    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        LOG.error("Request for topology list for cluster: %s, environment: %s "
                  "failed with error code: %s", cluster, environ,
                  str(response.status_code))
        raise err

    results: Dict[str, Any] = response.json()["result"]

    output: List[Dict[str, str]] = []

    for cluster_name, cluster_dict in results.items():
        for user, user_dict in cluster_dict.items():
            for environment, topology_list in user_dict.items():
                for topology in topology_list:
                    row: Dict[str, str] = {
                        "cluster": cluster_name,
                        "user": user,
                        "environ": environment,
                        "topology": topology}
                    output.append(row)

    return pd.DataFrame(output)


def get_logical_plan(tracker_url: str, cluster: str, environ: str,
                     topology: str) -> Dict[str, Any]:
    """ Get the logical plan dictionary from the heron tracker API.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        Dict[str, Any]:   A dictionary containing details of the spouts and
        bolts.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    LOG.info("Fetching logical plan for topology: %s", topology)

    logical_url: str = tracker_url + "/topologies/logicalplan"

    response: requests.Response = requests.get(logical_url,
                                               params={"cluster": cluster,
                                                       "environ": environ,
                                                       "topology": topology})

    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        LOG.error("Logical plan request for topology: %s , cluster: %s, "
                  "environment: %s failed with error code: %s", topology,
                  cluster, environ, str(response.status_code))
        raise err

    return response.json()["result"]


def get_physical_plan(tracker_url: str, cluster: str, environ: str,
                      topology: str) -> Dict[str, Any]:
    """ Get the physical plan dictionary from the heron tracker API.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        Dict[str, Any]: A dictionary containing details of the containers and
        stream managers for the specified topology.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    LOG.info("Fetching physical plan for topology: %s", topology)

    physical_url: str = tracker_url + "/topologies/physicalplan"

    response: requests.Response = requests.get(physical_url,
                                               params={"cluster": cluster,
                                                       "environ": environ,
                                                       "topology": topology})

    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        LOG.error("Physical plan request for topology: %s , cluster: %s, "
                  "environment: %s failed with error code: %s", topology,
                  cluster, environ, str(response.status_code))
        raise err

    return response.json()["result"]


def get_packing_plan(tracker_url: str, cluster: str, environ: str,
                      topology: str) -> Dict[str, Any]:
    """ Get the packing plan dictionary from the heron tracker API.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        Dict[str, Any]: A dictionary containing details of the containers
        for the specified topology, in terms of their resource allocations.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    LOG.info("Fetching packing plan for topology: %s", topology)

    packing_url: str = tracker_url + "/topologies/packingplan"

    response: requests.Response = requests.get(packing_url,
                                               params={"cluster": cluster,
                                                       "environ": environ,
                                                       "topology": topology})

    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        LOG.error("Packing plan request for topology: %s , cluster: %s, "
                  "environment: %s failed with error code: %s", topology,
                  cluster, environ, str(response.status_code))
        raise err

    return response.json()["result"]


def parse_instance_name(instance_name: str) -> Dict[str, Union[str, int]]:
    """ Parses the instance name string returned by the Heron Tracker API into
    a dictionary with instance information.

    Arguments:
        instance_name (str): Instance name string in the form:
                             container_<container_num>_<component>_<task_id>

    Returns:
        Dict[str, Union[str, int]]: A dictionary with the following keys:
        *container* : The container id as a integer,
        *component* : The component name string,
        *task_id* : The instances task id as an integer.
    """

    parts: List[str] = instance_name.split("_")

    if len(parts) == 4:
        component: str = parts[2]
    elif len(parts) > 4:
        component = "_".join(parts[2:-2])

    return {"container": int(parts[1]), "component": component,
            "task_id": int(parts[-1])}


def get_topology_info(tracker_url: str, cluster: str, environ: str,
                      topology: str) -> Dict[str, Union[int, str]]:
    """ Get the information dictionary from the heron tracker API. This
    contains the logical and physical plans as well as other information on the
    topology.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        Dict[str, Union[str, int]]: A dictionary containing all the details the
        tracker has on the specified topology.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    info_url: str = tracker_url + "/topologies/info"

    response: requests.Response = requests.get(info_url,
                                               params={"cluster": cluster,
                                                       "environ": environ,
                                                       "topology": topology})

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
        Dict[str, Any]: A dictionary containing aggregate metrics for the
        specified topology component.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    payload: Dict[str, Union[str, List[str], int]] = {
        "cluster": cluster, "environ": environ, "topology": topology,
        "component": component, "interval": interval, "metricname": metrics}

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
        Dict[str, Any]: A dictionary containing metrics timelines for the
        specified topology  component.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    duration: int = end_time - start_time

    if duration > 10800:
        LOG.warning("Duration of metrics timeline interval for metrics: %s of "
                    "topology: %s was greater than the 3 hours of data stored "
                    "by the Topology Master", str(metrics), topology)

    payload: Dict[str, Union[str, List[str], int]] = {
        "cluster": cluster, "environ": environ, "topology": topology,
        "component": component, "starttime": start_time,
        "endtime": end_time, "metricname": metrics}

    metrics_timeline_url: str = tracker_url + "/topologies/metricstimeline"

    response: requests.Response = requests.get(metrics_timeline_url,
                                               params=payload)
    response.raise_for_status()

    LOG.info("Fetched timeline(s) for metric(s): %s of component: %s from "
             "topology: %s over a period of %d seconds", str(metrics),
             component, topology, duration)

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
        Dict[str, Any]: A dictionary containing the query results.

    Raises:
        requests.HTTPError: If a non 200 status code is returned.
    """

    duration: int = end_time - start_time

    if duration > 10800:
        LOG.warning("Duration of metrics timeline interval for metrics query: "
                    " %s for topology: %s was greater than the 3 hours of "
                    " data stored by the Topology Master", query, topology)

    payload: Dict[str, Union[str, int]] = {
        "cluster": cluster, "environ": environ, "topology": topology,
        "starttime": start_time, "endtime": end_time, "query": query}

    metrics_query_url: str = tracker_url + "/topologies/metricsquery"

    response: requests.Response = requests.get(metrics_query_url,
                                               params=payload)
    response.raise_for_status()

    LOG.info("Fetched results of query: %s from topology: %s over a "
             "period of %d seconds", query, topology, duration)

    return response.json()["result"]


def get_incoming_streams(logical_plan: Dict[str, Any],
                         component_name: str) -> List[str]:
    """ Gets a list of input stream names for the supplied component in the
    supplied logical plan.

    Arguments:
        logical_plan (dict):    Logical plan dictionary returned by the Heron
                                Tracker API.
        component (str):    The name of the component whose incoming streams
                            are to be extracted.

    Returns:
        List[str]:  A list of incoming stream names.
    """

    return [input_stream["stream_name"]
            for input_stream
            in logical_plan["bolts"][component_name]["inputs"]]


def incoming_sources_and_streams(logical_plan: Dict[str, Any],
                                 component_name: str
                                 ) -> List[Tuple[str, str]]:
    """ Gets a list of (source component, input stream name) tuples for the
    supplied component in the supplied logical plan.

    Arguments:
        logical_plan (dict):    Logical plan dictionary returned by the Heron
                                Tracker API.
        component (str):    The name of the component whose incoming streams
                            are to be extracted.

    Returns:
        List[str]:  A list of (source component name, incoming stream name)
        tuples.
    """

    return [(input_stream["component_name"], input_stream["stream_name"])
            for input_stream
            in logical_plan["bolts"][component_name]["inputs"]]


def get_outgoing_streams(logical_plan: Dict[str, Any],
                         component_name: str) -> List[str]:
    """ Gets a list of output stream names for the supplied component in the
    supplied logical plan.

    Arguments:
        logical_plan (dict):    Logical plan dictionary returned by the Heron
                                Tracker API.
        component (str):    The name of the component whose outgoing streams
                            are to be extracted.

    Returns:
        List[str]:  A list of outgoing stream names.
    """

    # Check if this is a spout
    if logical_plan["spouts"].get(component_name):
        comp_type: str = "spouts"
    else:
        comp_type = "bolts"

    return [output_stream["stream_name"]
            for output_stream
            in logical_plan[comp_type][component_name]["outputs"]]


def get_component_task_ids(tracker_url: str, cluster: str, environ: str,
                           topology: str) -> Dict[str, List[int]]:
    """ Get a dictionary mapping from component name to a list of integer task
    id for the instances belonging to that component.

    Arguments:
        tracker_url (str):  The base url string for the Heron Tracker instance.
        cluster (str):  The cluster the topology is running in.
        environ (str):  The environment the topology is running in (eg. prod,
                        devel, test, etc).
        topology (str): The topology name.

    Returns:
        Dict[str, List[int]]:   A dictionary mapping from component name to a
        list of integer task id for the instances belonging to that component.
    """

    pplan: Dict[str, Any] = get_physical_plan(tracker_url, cluster, environ,
                                              topology)

    output: Dict[str, List[int]] = {}

    for comp_type in ["bolts", "spouts"]:
        for comp_name, instance_list in pplan[comp_type].items():
            output[comp_name] = [cast(int,
                                      parse_instance_name(i_name)["task_id"])
                                 for i_name in instance_list]

    return output
