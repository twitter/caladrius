# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for calculating the arrival rate at each
instance of a topology. """

import logging

import datetime as dt

from typing import List, Dict, Tuple, Union, DefaultDict, cast
from collections import defaultdict
from functools import lru_cache

import pandas as pd

from gremlin_python.process.graph_traversal import \
    (GraphTraversalSource, not_, out, outE, loops, constant, properties, inV,
     outV, in_)
from gremlin_python.process.traversal import P
from gremlin_python.structure.graph import Vertex
from gremlin_python import statics

from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.analysis.heron.io_ratios import lstsq_io_ratios

# TODO: make this function configurable
from caladrius.metrics.heron.topology.routing_probabilities import \
    calc_current_inter_instance_rps as calculate_inter_instance_rps

# Type definitions
ARRIVAL_RATES = DefaultDict[int, DefaultDict[Tuple[str, str], float]]
OUTPUT_RATES = DefaultDict[int, Dict[str, float]]

LOG: logging.Logger = logging.getLogger(__name__)


def get_levels(topo_traversal: GraphTraversalSource) -> List[List[Vertex]]:
    """ Gets the levels of the logical graph. The traversal starts with the
    source spouts and performs a breadth first search through the logically
    connected vertices.

    Arguments:
        topo_traversal (GraphTraversalSource):  A traversal source instance
                                                mapped to the topology subgraph
                                                whose levels are to be
                                                calculated.

    Returns:
        A list where each entry is a list of Vertex instances representing a
        level within the logical graph. The first level will be the spout
        instances.
    """

    # Only load the static enums we need so we don't pollute the globals dict
    keys = statics.staticEnums["keys"]
    values = statics.staticEnums["values"]
    local_scope = statics.staticEnums["local"]

    # Repeatedly traverse the tree defined by the logical connections, grouping
    # each group (or set because we us de-duplicate) of vertices by their depth
    # in the tree. This depth is based the current number of times the repeat
    # step has run (loops). So you end up with a map of integer depth to list
    # of vertices which is emitted by the cap step. After this we just put the
    # Hash Map in key order (ascending) and then take only the values (the
    # lists of vertices) and unfold them into a list.
    # The first group by(-1) statement is so that the spout vertices are
    # included at the top of the list
    levels: List[List[Vertex]] = (
        topo_traversal.V().hasLabel("spout")
        .group("m").by(constant(-1))
        .repeat(out("logically_connected").dedup().group("m").by(loops()))
        .until(not_(outE("logically_connected")))
        .cap("m")
        .order(local_scope).by(keys)
        .select(values).unfold().toList())

    return levels


@lru_cache()
def _setup_arrival_calcs(metrics_client: HeronMetricsClient,
                         graph_client: GremlinClient,
                         topology_id: str, cluster: str, environ: str,
                         topology_ref: str, start: dt.datetime,
                         end: dt.datetime, io_bucket_length: int,
                         tracker_url: str, **kwargs: Union[str, int, float]
                         ) -> Tuple[pd.DataFrame, List[List[Vertex]],
                                    pd.DataFrame, Dict[Vertex, List[int]],
                                    Dict[Vertex, List[int]]]:
    """ Helper method which sets up the data needed for the arrival rate
    calculations. This is a separate cached method as these data are not
    effected by the traffic (spout_state) and so do not need to be recalculated
    for a new traffic level for the same topology id/ref. """

    topo_traversal: GraphTraversalSource = \
        graph_client.topology_subgraph(topology_id, topology_ref)

    # Calculate the routing probabilities for the defined metric gathering
    # period
    i2i_rps: pd.Series = (calculate_inter_instance_rps(
        metrics_client, topology_id, cluster, environ, start, end, tracker_url,
        **kwargs).set_index(["source_task", "destination_task", "stream"])
     ["routing_probability"])

    # Get the vertex levels for the logical graph tree
    LOG.info("Calculating levels for topology %s reference %s", topology_id,
             topology_ref)
    levels: List[List[Vertex]] = get_levels(topo_traversal)
    LOG.debug("Found %d levels is topology %s reference %s", len(levels),
              topology_id, topology_ref)

    # Calculate the input output ratios for each instances using data from the
    # defined metrics gathering period
    coefficients: pd.Series = lstsq_io_ratios(
        metrics_client, graph_client, topology_id, cluster, environ, start,
        end, io_bucket_length, **kwargs).set_index(["task", "output_stream",
                                                    "input_stream",
                                                    "source_component"]
                                                   )["coefficient"]

    # Get the details of the incoming and outgoing physical connections for
    # stream manager in the topology

    # Get a dictionary mapping from stream manager id string to a list of the
    # instances (within each container) that will send tuples to each stream
    # manager
    sending_instances: Dict[Vertex, List[int]] = \
        (topo_traversal.V().hasLabel("stream_manager")
         .group().by("id").by(in_("physically_connected")
                              .hasLabel(P.within("spout", "bolt"))
                              .values("task_id")
                              .fold())
         .next())

    # Get a dictionary mapping from stream manager id string to a list of the
    # instances (within each container) that will receive tuples from each
    # stream manager
    receiving_instances: Dict[Vertex, List[int]] = \
        (topo_traversal.V().hasLabel("stream_manager")
         .group().by("id").by(out("physically_connected")
                              .hasLabel("bolt").values("task_id").fold())
         .next())

    return (i2i_rps, levels, coefficients, sending_instances,
            receiving_instances)


def _calculate_arrivals(topo_traversal: GraphTraversalSource,
                        source_vertex: Vertex, arrival_rates: ARRIVAL_RATES,
                        output_rates: DefaultDict[int, Dict[str, float]],
                        i2i_rps: pd.DataFrame) -> ARRIVAL_RATES:

    # Get all downstream edges and vertices for this source vertex
    out_edges: List[Dict[str, Union[str, int, float]]] = \
        (topo_traversal.V(source_vertex).outE("logically_connected")
         .project("source_task", "source_component", "stream_name",
                  "destination_task", "destination_component")
         .by(outV().properties("task_id").value())
         .by(outV().properties("component").value())
         .by(properties("stream").value())
         .by(inV().properties("task_id").value())
         .by(inV().properties("component").value())
         .toList())

    if not out_edges:
        return arrival_rates

    source_task: int = cast(int, out_edges[0]["source_task"])
    source_component: str = cast(str, out_edges[0]["source_component"])

    LOG.debug("Processing output from source instance %s_%d",
              source_component, source_task)

    for out_edge in out_edges:
        stream: str = cast(str, out_edge["stream_name"])
        try:
            stream_output: float = cast(float,
                                        output_rates[source_task][stream])
        except KeyError:
            LOG.debug("No output rate information for source task %d on "
                      "stream %s. Skipping the outgoing edge", source_task,
                      stream)
            continue

        destination_task: int = cast(int, out_edge["destination_task"])

        try:
            r_prob: float = float(i2i_rps.loc(axis=0)[source_task,
                                                      destination_task,
                                                      stream])
        except KeyError:
            LOG.debug("Unable to find routing probability for connection from "
                      "task %d to %d on stream %s", source_task,
                      destination_task, stream)

            edge_output: float = 0.0
        else:

            edge_output = (stream_output * r_prob)

            LOG.debug("Output from %s-%d to %s-%d on stream %s is "
                      "calculated as %f * %f = %f", source_component,
                      source_task, out_edge["destination_component"],
                      destination_task, stream, stream_output, r_prob,
                      edge_output)

        arrival_rates[destination_task][
            (stream, source_component)] += edge_output

    return arrival_rates


def _calculate_outputs(topo_traversal: GraphTraversalSource,
                       source_vertex: Vertex,
                       arrival_rates: ARRIVAL_RATES,
                       output_rates: DefaultDict[int, Dict[str, float]],
                       coefficients: pd.Series,
                       ) -> DefaultDict[int, Dict[str, float]]:

    source_task: int = (topo_traversal.V(source_vertex)
                        .properties("task_id").value().next())

    in_streams: List[Dict[str, str]] = \
        (topo_traversal.V(source_vertex).inE("logically_connected")
         .project("stream_name", "source_component")
         .by(properties("stream").value())
         .by(outV().properties("component").value())
         .dedup()
         .toList())

    out_streams: List[str] = \
        (topo_traversal.V(source_vertex)
         .outE("logically_connected").values("stream")
         .dedup().toList())

    for out_stream in out_streams:
        output_rate: float = 0.0
        for in_stream in in_streams:
            in_stream_name: str = in_stream["stream_name"]
            source_component: str = in_stream["source_component"]

            stream_arrivals: float = \
                arrival_rates[source_task][(in_stream_name,
                                            source_component)]

            try:
                coefficent: float = float(coefficients.loc[
                    source_task, out_stream, in_stream_name,
                    source_component])
            except KeyError:
                LOG.debug("No coefficient available for source task %d, "
                          "out stream %s, in stream %s from component %s",
                          source_task, out_stream, in_stream_name,
                          source_component)
            else:
                output_rate += (stream_arrivals * coefficent)

        # It is possible that some of the IO coefficients may be negative,
        # implying that the more you receive on an input stream the less you
        # output to a given output stream. If we anticipate a large arrival on
        # this negative input stream and low on other positive streams then it
        # is possible that the predicted output rate could be negative (which
        # is obviously meaningless).
        if output_rate < 0.0:
            output_rate = 0.0

        output_rates[source_task][out_stream] = output_rate

    return output_rates


def _convert_arrs_to_df(arrival_rates: ARRIVAL_RATES) -> pd.DataFrame:

    output: List[Dict[str, Union[str, float]]] = []

    for task_id, incoming_streams_dict in arrival_rates.items():
        for (incoming_stream, source_component), arrival_rate \
                in incoming_streams_dict.items():
            row: Dict[str, Union[str, float]] = {
                "task": task_id,
                "incoming_stream": incoming_stream,
                "source_component": source_component,
                "arrival_rate": arrival_rate}
            output.append(row)

    return pd.DataFrame(output)


def _calc_strmgr_in_out(sending_instances: Dict[str, List[int]],
                        receiving_instances: Dict[str, List[int]],
                        output_rates: OUTPUT_RATES,
                        arrival_rates: ARRIVAL_RATES) -> pd.DataFrame:

    strmgr_outgoing: Dict[str, float] = {}

    for stream_manager, sending_task_list in sending_instances.items():
        total_sent: float = 0.0
        for task_id in sending_task_list:
            total_sent += sum(output_rates[task_id].values())

        strmgr_outgoing[stream_manager] = total_sent

    strmgr_incoming: Dict[str, float] = {}

    for stream_manager, receiving_task_list in receiving_instances.items():
        total_receieved: float = 0.0
        for task_id in receiving_task_list:
            total_receieved += sum(arrival_rates[task_id].values())

        strmgr_incoming[stream_manager] = total_receieved

    # Convert the stream manager dictionaries into a DataFrame. It is possible
    # that a container could only hold spouts (in which chase would have no
    # entry in the incoming dict) or only sinks (therefore no entries in the
    # outgoing dict) so take the union of keys from both dicts and add None to
    # the DF if the key is missing
    strmgr_output: List[Dict[str, Union[str, float, None]]] = []
    for strmgr in (set(strmgr_incoming.keys()) or set(strmgr_outgoing.keys())):
        row: Dict[str, Union[str, float, None]] = {
            "id": strmgr,
            "incoming": strmgr_incoming.get(strmgr, None),
            "outgoing": strmgr_outgoing.get(strmgr, None)}
        strmgr_output.append(row)

    return pd.DataFrame(strmgr_output)


def calculate(graph_client: GremlinClient, metrics_client: HeronMetricsClient,
              topology_id: str, cluster: str, environ: str, topology_ref: str,
              start: dt.datetime, end: dt.datetime, io_bucket_length: int,
              tracker_url: str, spout_state: Dict[int, Dict[str, float]],
              **kwargs: Union[str, int, float]
              ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """

    Arguments:
        graph_client (GremlinClient):   The client instance for the graph
                                        database.
        metrics_client (HeronMetricsClient):    The client instance for the
                                                metrics database.
        topology_id (str):  The topology identification string.
        cluster: (str): The cluster the topology is running on.
        environ (str): The environment the topology is running in.
        topology_ref (str): The reference string for the topology physical
                            graph to be used in the calculations.
        start (dt.datetime):    The UTC datetime instance representing the
                                start of the metric gathering window.
        end (dt.datetime):  The UTC datetime instance representing the end of
                            the metric gathering window.
        io_bucket_length (int): The length in seconds that metrics should be
                                aggregated for use in IO ratio calculations.
        tracker_url (str):  The URL for the Heron Tracker API
        spout_state (dict): A dictionary mapping from instance task id to a
                            dictionary that maps from output stream name to the
                            output rate for that spout instance. The units of
                            this rate (TPS, TPM etc) will be the same for the
                            arrival rates.
        **kwargs:   Any additional key word arguments required by the metrics
                    client query methods. NOTE: This is passed to a cached
                    method so all kwargs must be hashable. Un-hashable
                    arguments will be removed before being supplied.

    Returns:
        pd.DataFrame:   A DataFrame containing the arrival rate at each
                        instance.
        pd.DataFrame:   A DataFrame containing the input and output rate of
                        each stream  manager.

    Raises:
        RuntimeError:   If there is no entry in the graph database for the
                        supplied topology id and ref.
    """

    # First check that there is a physical graph for the supplied reference in
    # the graph database
    graph_client.raise_if_missing(topology_id, topology_ref)

    LOG.info("Calculating arrival rates for topology %s reference %s using "
             "metrics from a %d second period from %s to %s", topology_id,
             topology_ref, (end-start).total_seconds(), start.isoformat(),
             end.isoformat())

    i2i_rps, levels, coefficients, sending_instances, receiving_instances = \
        _setup_arrival_calcs(metrics_client, graph_client, topology_id,
                             cluster, environ, topology_ref, start, end,
                             io_bucket_length, tracker_url, **kwargs)

    topo_traversal: GraphTraversalSource = \
        graph_client.topology_subgraph(topology_id, topology_ref)

    arrival_rates: ARRIVAL_RATES = defaultdict(lambda: defaultdict(float))
    output_rates: OUTPUT_RATES = defaultdict(dict)
    output_rates.update(spout_state)

    # Step through the tree levels and calculate the output from each level and
    # the arrivals at the next. Skip the final level as its arrival rates are
    # calculated in the previous step and it has no outputs.
    for level_number, level in enumerate(levels[:-1]):

        LOG.debug("Processing topology level %d", level_number)

        if level_number != 0:
            # If this is not a spout level then we need to calculate the output
            # from the instances in this level.
            for source_vertex in level:

                output_rates = _calculate_outputs(topo_traversal,
                                                  source_vertex, arrival_rates,
                                                  output_rates, coefficients)

        # Calculate the arrival rates at the instances down stream on the next
        # level down
        for source_vertex in level:

            arrival_rates = _calculate_arrivals(topo_traversal, source_vertex,
                                                arrival_rates, output_rates,
                                                i2i_rps)

    # At this stage we have the output and arrival amount for all logically
    # connected elements. We now need to map these on to the stream managers to
    # calculate their incoming and outgoing tuple rates.
    strmgr_in_out: pd.DataFrame = _calc_strmgr_in_out(
        sending_instances, receiving_instances, output_rates, arrival_rates)

    return _convert_arrs_to_df(arrival_rates), strmgr_in_out
