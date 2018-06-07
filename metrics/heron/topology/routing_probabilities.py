# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for calculating the routing probabilities of
heron topologies. """

import logging

import datetime as dt

from typing import Union, List, Dict

import pandas as pd

from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.metrics.heron.topology import groupings
from caladrius.common.heron import tracker

LOG: logging.Logger = logging.getLogger(__name__)


def calculate_inter_instance_rps(metrics_client: HeronMetricsClient,
                                 topology_id: str, cluster: str, environ: str,
                                 start: dt.datetime, end: dt.datetime
                                 ) -> pd.DataFrame:
    """ Get a DataFrame with the instance to instance routing probabilities for
    each source instance's output streams.

    Arguments:
        metrics_client (HeronMetricsClient):    The metrics client from which
                                                to extract transfer count data
                                                from.
        topology_id (str):  The topology identification string.
        cluster (str): The cluster the topology is running on.
        environ (str): The environment the topology is running in.
        start (dt.datetime):    The UTC datetime object for the start of the
                                metrics gathering widow.
        end (dt.datetime):  The UTC datetime object for the end of the metrics
                            gathering widow.

    Returns:
        pandas.DataFrame: A DataFrame with the following columns:

        * source_component: The source instance's component name.
        * source_task: The source instances task ID.
        * stream: The stream ID string for the outgoing stream from the source.
        * destination_component: The destination instance's component name.
        * destination_task: The destination instance's task ID.
        * routing_probability: The probability (between 0 and 1) that a tuple
          leaving the source instance on the specified stream will be routed to
          the destination instance.
    """

    LOG.info("Calculating instance to instance routing probabilities for "
             "topology %s for period from %s to %s", topology_id,
             start.isoformat(), end.isoformat())

    # Get the receive counts for the topology
    rec_counts: pd.DataFrame = metrics_client.get_receive_counts(
        topology_id, cluster, environ, start, end)

    # Get the instance to instance transfers
    transfer_counts: pd.DataFrame = rec_counts.groupby(
        ["source_component", "source_task", "stream", "component", "task"]
        )["receive_count"].sum().reset_index()
    transfer_counts.rename(index=str,
                           columns={"receive_count": "transfer_count"},
                           inplace=True)

    # Get the total emitted by each instance onto each stream
    total_emissions: pd.DataFrame = rec_counts.groupby(
        ["source_component", "source_task", "stream", "component"]
        )["receive_count"].sum().reset_index()
    total_emissions.rename(index=str,
                           columns={"receive_count": "total_emitted"},
                           inplace=True)

    # Merge the total emissions from each instance and the total transferred
    # between instances into a single DataFrame
    merged_counts: pd.DataFrame = total_emissions.merge(
        transfer_counts, on=["source_component", "source_task", "stream",
                             "component"])

    # Calculate the routing probability
    merged_counts["routing_probability"] = (merged_counts["transfer_count"] /
                                            merged_counts["total_emitted"])

    merged_counts["routing_probability"].fillna(0, inplace=True)

    merged_counts.rename(index=str,
                         columns={"component": "destination_component",
                                  "task": "destination_task"},
                         inplace=True)

    return merged_counts[["source_component", "source_task", "stream",
                          "destination_component", "destination_task",
                          "routing_probability"]]


def calculate_ISAP(metrics_client: HeronMetricsClient, topology_id: str,
                   cluster: str, environ: str, start: dt.datetime,
                   end: dt.datetime, **kwargs: Union[str, int, float]
                   ) -> pd.DataFrame:
    """ Calculates the Instance Stream Activation Proportion (ISAP) for each
    instance in the specified topology. This is the proportion, relative to the
    total activation of all instances of the same component, that each instance
    is active for each (stream, source component) combination.

    Under certain situation the ISAP can be used as a proxy for the routing
    probability of incoming connection to the instance.

    Arguments:
        metrics_client (HeronMetricsClient):    The metrics client from which
                                                to extract transfer count data
                                                from.
        topology_id (str):  The topology identification string.
        cluster (str): The cluster the topology is running in.
        environ (str): The environment the topology is running in.
        start (dt.datetime):    The UTC datetime object for the start of the
                                metrics gathering widow.
        end (dt.datetime):  The UTC datetime object for the end of the metrics
                            gathering widow.
        **kwargs:   Additional keyword arguments required by the methods of
                    the supplied metrics client instance.

    Returns:
        pandas.DataFrame:   A DataFrame with the following columns:

        * timestamp: The UTC timestamp for the metric.
        * component: The component this metric comes from.
        * task: The instance ID number for the instance that the metric
          comes from.
        * container:  The ID for the container this metric comes from.
        * stream: The name of the incoming stream from which the tuples
          that lead to this metric came from.
        * source_component: The name of the component the stream's source
          instance belongs to.
        * execute_count: The execute count during the metric time period.
        * component_total: The total number of tuples executed by all
          instances of the component in that metric time period.
        * ISAP: The instance stream activation proportion for the given
          instance in that metric time period.
    """

    LOG.info("Calculating ISAP for topology %s over a %d second period from "
             "%s to %s", topology_id, (end-start).total_seconds(),
             start.isoformat(), end.isoformat())

    execute_counts: pd.DataFrame = metrics_client.get_execute_counts(
        topology_id, cluster, environ, start, end, **kwargs)

    ex_counts_totals: pd.DataFrame = execute_counts.merge(
        execute_counts.groupby(
            ["component", "stream", "source_component", "timestamp"])
        .execute_count.sum().reset_index()
        .rename(index=str, columns={"execute_count": "component_total"}),
        on=["component", "stream", "source_component", "timestamp"])

    ex_counts_totals["ISAP"] = (ex_counts_totals["execute_count"] /
                                ex_counts_totals["component_total"])

    ex_counts_totals["ISAP"].fillna(0, inplace=True)

    return ex_counts_totals


def calc_current_inter_instance_rps(
    metrics_client: HeronMetricsClient, topology_id: str, cluster: str,
    environ: str, start: dt.datetime, end: dt.datetime, tracker_url: str,
    **kwargs: Union[str, int, float]) -> pd.DataFrame:
    """ Get a DataFrame with the instance to instance routing probabilities for
    each source instance's output streams from a currently running topology.
    This method uses several assumptions to infer the routing probabilities
    between connections.

    This method will not work for calculating routing probabilities of
    connections that come from a source component that was itself a recipient
    of a fields connection. This method relies on the source instances of
    fields grouped connections receiving the same key distribution as all
    other source instances of that component. i.e. it assumes the sources of
    all fields groupings only receive shuffle groupings.

    This method also assumes that the spout instances will emit equal key
    distributions if they are the source of fields grouped connections.

    Arguments:
        metrics_client (HeronMetricsClient):    The metrics client from which
                                                to extract transfer count data
                                                from.
        topology_id (str):  The topology identification string.
        cluster (str):    The cluster parameter for the Heron Tracker API.
        environ (str):    The environ parameter (prod, devel, test etc) for
                            the Heron Tracker API.
        start (dt.datetime):    The UTC datetime object for the start of the
                                metrics gathering widow.
        end (dt.datetime):  The UTC datetime object for the end of the metrics
                            gathering widow.
        tracker_url (str):    The URL for the Heron Tracker API. This method
                              needs to analyse the logical and physical plans
                              of the specified topology so needs access to
                              this API.

    Returns:
        pandas.DataFrame:   A DataFrame with the following columns:

        * source_component: The source instance's component name.
        * source_task: The source instances task ID.
        * stream: The stream ID string for the outgoing stream from the source.
        * destination_component: The destination instance's component name.
        * destination_task: The destination instance's task ID.
        * routing_probability: The probability (between 0 and 1) that a tuple
          leaving the source instance on the specified stream will be routed to
          the destination instance.

    Raises:
        RuntimeError:   If any of the specified key word arguments are not
                        supplied.
        NotImplementedError:    It the specified topology has a fields grouped
                                connection leading into another fields grouped
                                connection. This is not a currently supported
                                scenario.
    """

    LOG.info("Calculating instance to instance routing probabilities for "
             "topology %s for period from %s to %s", topology_id,
             start.isoformat(), end.isoformat())

    LOG.debug("Checking for fields to fields grouped connections")

    if groupings.has_fields_fields(tracker_url, topology_id, cluster, environ):
        grouping_msg: str = (
            f"The topology {topology_id} has at least one fields grouped "
            f"connection where the source of the connection also received a "
            f"fields grouped connection. This means the key distribution into "
            f"the source could be unbalanced (across the component) and this "
            f"method does not yet support this scenario.")
        LOG.error(grouping_msg)
        raise NotImplementedError(grouping_msg)

    isap: pd.DataFrame = calculate_ISAP(metrics_client, topology_id, cluster,
                                        environ, start, end, **kwargs)

    # Remove system hearbeat streams
    isap = isap[~isap.source_component.str.contains("__")]

    # Munge the frame into the correct format. Take an average of the whole
    # time series for each instance
    # TODO: Look at other summary methods for ISAP time series
    r_probs: pd.DataFrame = (isap.groupby(["task", "component", "stream",
                                           "source_component"])
                             .ISAP
                             .mean()
                             .reset_index()
                             .rename(index=str,
                                     columns={"ISAP": "routing_probability",
                                              "task": "destination_task",
                                              "component":
                                              "destination_component"}))

    comp_task_ids: Dict[str, List[int]] = \
        tracker.get_component_task_ids(tracker_url, cluster, environ,
                                       topology_id)

    output: List[Dict[str, Union[str, int, float]]] = []

    for row in r_probs.itertuples():
        for source_task in comp_task_ids[row.source_component]:
            output.append({
                "source_task": source_task,
                "source_component": row.source_component,
                "stream": row.stream,
                "destination_task": row.destination_task,
                "destination_component": row.destination_component,
                "routing_probability": row.routing_probability})

    return pd.DataFrame(output)
