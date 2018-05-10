""" This module contains methods for calculating the routing probabilities of
heron topologies. """

import logging

import datetime as dt

import pandas as pd

from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

def calculate_inter_instance_rps(metrics_client: HeronMetricsClient,
                                 topology_id: str, start: dt.datetime,
                                 end: dt.datetime) -> pd.DataFrame:
    """ Get a DataFrame with the instance to instance routing probabilities for
    each source instance's output streams.

    Arguments:
        metrics_client (HeronMetricsClient):    The metrics client from which
                                                to extract transfer count data
                                                from.
        topology_id (str):  The topology identification string.
        start (dt.datetime):    The UTC datetime object for the start of the
                                metrics gathering widow.
        end (dt.datetime):  The UTC datetime object for the end of the metrics
                            gathering widow.

    Returns:
        A DataFrame with the following columns:

        source_component: The source instance's component name.
        source_task: The source instances task ID.
        stream: The stream ID string for the outgoing stream from the source.
        destination_component: The destination instance's component name.
        destination_task: The destination instance's task ID.
        routing_probability: The probability (between 0 and 1) that a tuple
        leaving the source instance on the specified stream will be routed to
        the destination instance.
    """

    LOG.info("Calculating instance to instance routing probabilities for "
             "topology %s for period from %s to %s", topology_id,
             start.isoformat(), end.isoformat())

    # Get the receive counts for the topology
    rec_counts: pd.DataFrame = metrics_client.get_receive_counts(topology_id,
                                                                 start=start,
                                                                 end=end)

    # Get the instance to instance transfers
    transfer_counts: pd.DataFrame = rec_counts.groupby(
        ["source_component", "source_task", "stream", "component", "task"]
        )["receive_count"].sum().reset_index()
    transfer_counts.rename(index=str,
                           columns={"receive_count" : "transfer_count"},
                           inplace=True)

    # Get the total emitted by each instance onto each stream
    total_emissions: pd.DataFrame = rec_counts.groupby(
        ["source_component", "source_task", "stream", "component"]
        )["receive_count"].sum().reset_index()
    total_emissions.rename(index=str,
                           columns={"receive_count" : "total_emitted"},
                           inplace=True)

    # Merge the total emissions from each instance and the total transferred
    # between instances into a single DataFrame
    merged_counts: pd.DataFrame = total_emissions.merge(
        transfer_counts, on=["source_component", "source_task", "stream",
                             "component"])

    # Calculate the routing probability
    merged_counts["routing_probability"] = (merged_counts["transfer_count"] /
                                            merged_counts["total_emitted"])

    merged_counts.rename(index=str,
                         columns={"component" : "destination_component",
                                  "task" : "destination_task"},
                         inplace=True)

    return merged_counts[["source_component", "source_task", "stream",
                          "destination_component", "destination_task",
                          "routing_probability"]]

def calc_current_inter_instance_rps(metrics_client: HeronMetricsClient,
                                    topology_id: str, start: dt.datetime,
                                    end: dt.datetime) -> pd.DataFrame:

    pass


