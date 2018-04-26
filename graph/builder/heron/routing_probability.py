""" This module contains methods for calculating the routing probabilities of
heron topologies. """

import logging

import datetime as dt

from typing import List, Dict, Union

import pandas as pd

from gremlin_python.process.traversal import P
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.structure.graph import Edge

from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.client.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)

def get_comp_links_by_grouping(
        graph_traversal: GraphTraversalSource, grouping: str
    ) -> List[Dict[str, str]]:
    """ Gets a list of component connection dictionaries. These describe all
    source->stream->destination connections with the specified grouping value
    in the topology available via the supplied graph traversal source.

    Arguments:
        graph_traversal (GraphTraversalSource): A GraphTraversalSource instance
                                                linked to the topology subgraph
                                                whose connections are to be
                                                queried.
        grouping (str): The stream grouping of the connections to be returned.

    Returns:
        A list of dictionaries each containing "source", "stream" and
        "destination" keys of the component and stream name respectively.
    """

    component_connections: List[Dict[str, str]] = \
        (graph_traversal.V().hasLabel(P.within("bolt", "spout")).as_("source")
         .outE("logically_connected").has("grouping", grouping).as_("stream")
         .inV().as_("destination").select("source", "stream", "destination")
         .by("component").by("stream").by("component").dedup().toList())

    return component_connections

def set_shuffle_routing_probs(graph_client: GremlinClient,
                              topology_id: str, topology_ref: str) -> None:
    """ This method will set the routing probability for shuffle connections in
    the graph with the supplied topology ID and reference.

    Arguments:
        graph_client (GremlinClient):   The client instance for the graph
                                        database.
        topology_id (str):  The topology identification string.
        topology_ref (str): The topology reference string.
    """

    LOG.info("Calculating routing probabilities for shuffle grouped logical "
             "connections in the graph of topology %s reference %s",
             topology_id, topology_ref)

    topology_traversal: GraphTraversalSource = \
        graph_client.topology_subgraph(topology_id, topology_ref)

    for comp_conn in get_comp_links_by_grouping(topology_traversal, "SHUFFLE"):

        LOG.debug("Calculating routing probabilities for logical connections "
                  "between instances of %s and %s on the %s stream",
                  comp_conn["source"], comp_conn["destination"],
                  comp_conn["stream"])

        # Calculate the shuffle grouped connections routing probability based
        # on the number of downstream instances for this connections
        shuffle_rp: float = (topology_traversal.V()
                             .has("component", comp_conn["destination"])
                             .count().math("1/_").next())

        # Apply the calculated routing probability to all logical connections
        # with this stream name between the source and destination instances
        # of the these components
        (topology_traversal.V().has("component", comp_conn["source"])
         .outE("logically_connected").has("stream", comp_conn["stream"])
         .property("routing_probability", shuffle_rp)
         .inV().has("component", comp_conn["destination"])
         .iterate())

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

def set_fields_routing_probs(graph_client: GremlinClient,
                             metrics_client: HeronMetricsClient,
                             topology_id: str, topology_ref: str,
                             start: dt.datetime, end: dt.datetime) -> None:
    """ Sets the routing probabilities for fields grouped logical connections
    in physical graph with the supplied topology ID and reference. Routing
    probabilities are calculated using metrics from the defined time window.

    Arguments:
        graph_client (GremlinClient):   The client instance for the graph
                                        database.
        metrics_client (HeronMetricsClient): The client instance for metrics
                                             database.
        topology_id (str):  The topology identification string.
        topology_ref (str): The topology reference string.
        start (dt.datetime):    The UTC datetime object for the start of the
                                metrics gathering widow.
        end (dt.datetime):  The UTC datetime object for the end of the metrics
                            gathering widow.
    """

    LOG.info("Setting fields grouping routing probabilities for topology %s "
             "reference %s using metrics data from %s to %s", topology_id,
             topology_ref, start.isoformat(), end.isoformat())

    topology_traversal: GraphTraversalSource = \
        graph_client.topology_subgraph(topology_id, topology_ref)

    i_to_i_rps: pd.DataFrame = calculate_inter_instance_rps(metrics_client,
                                                            topology_id, start,
                                                            end)

    # Re-index the DataFrame to make selecting RPs faster
    i_to_i_rps.set_index(["source_task", "stream", "destination_task"],
                         inplace=True)

    # Get a list of all fields grouped connections in the physical graph
    fields_connections: List[Dict[str, Union[int, str, Edge]]] = \
        (topology_traversal.V()
         .outE("logically_connected")
         .has("grouping", "FIELDS")
         .project("source_task", "stream", "edge", "destination_task")
         .by(__.outV().properties("task_id").value())
         .by(__.properties("stream").value())
         .by()
         .by(__.inV().properties("task_id").value())
         .toList())

    LOG.debug("Processing %d fields grouped connections for topology %s "
              "reference %s", len(fields_connections), topology_id,
              topology_ref)

    connection: Dict[str, Union[int, str, Edge]]
    for connection in fields_connections:

        LOG.debug("Processing connection from instance %d to %d on stream %s",
                  connection["source_task"], connection["destination_task"],
                  connection["stream"])

        routing_prob: float = (i_to_i_rps.loc[connection["source_task"],
                                              connection["stream"],
                                              connection["destination_task"]]
                               ["routing_probability"])

        (topology_traversal.E(connection["edge"])
         .property("routing_probability", routing_prob).next())
