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
     outV, inE)
from gremlin_python.structure.graph import Vertex
from gremlin_python import statics

from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.analysis.heron.io_ratios import lstsq_io_ratios
from caladrius.graph.analysis.heron.routing_probabilities import \
    calculate_inter_instance_rps

# Type definitions
ARRIVAL_RATES = DefaultDict[int, DefaultDict[Tuple[str, str], float]]

LOG: logging.Logger = logging.getLogger(__name__)

def get_levels(graph_client: GremlinClient, topology_id: str,
               topology_ref: str) -> List[List[Vertex]]:

    LOG.info("Calculating levels for topology %s reference %s", topology_id,
             topology_ref)

    topo_traversal: GraphTraversalSource = \
        graph_client.topology_subgraph(topology_id, topology_ref)

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

    LOG.debug("Found %d levels is topology %s reference %s", len(levels),
              topology_id, topology_ref)

    return levels

@lru_cache()
def _setup_arrival_calcs(metrics_client: HeronMetricsClient,
                         graph_client: GremlinClient,
                         topology_id: str, topology_ref: str,
                         start: dt.datetime, end: dt.datetime,
                         io_bucket_length: int,
                         **kwargs: Union[str, int, float]
                        ) -> Tuple[pd.DataFrame, List[List[Vertex]],
                                   pd.DataFrame]:

    # Calculate the routing probabilities for the defined metric gathering
    # period
    i_to_i_rps: pd.DataFrame = calculate_inter_instance_rps(
        metrics_client, topology_id, start, end)

    i2i_rps: pd.Series = i_to_i_rps.set_index(
        ["source_task", "destination_task", "stream"])["routing_probability"]

    # Get the vertex levels for the logical graph tree
    levels: List[List[Vertex]] = get_levels(graph_client, topology_id,
                                            topology_ref)

    # Calculate the input output ratios for each instances using data from the
    # defined metrics gathering period
    io_ratios: pd.DataFrame = lstsq_io_ratios(metrics_client, graph_client,
                                              topology_id, start, end,
                                              io_bucket_length, **kwargs)

    # Index the coefficients for each input stream
    coefficients: pd.Series = io_ratios.set_index(
        ["task", "output_stream", "input_stream",
         "source_component"])["coefficient"]

    return i2i_rps, levels, coefficients

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

    source_task: int = cast(int, out_edges[0]["source_task"])
    source_component: str = cast(str, out_edges[0]["source_component"])

    LOG.debug("Processing output from source instance %s-%d",
              source_component, source_task)

    for out_edge in out_edges:
        stream: str = cast(str, out_edge["stream_name"])
        stream_output: float = cast(float,
                                    output_rates[source_task][stream])
        destination_task: int = cast(int, out_edge["destination_task"])

        r_prob: float = float(i2i_rps.loc[source_task, destination_task,
                                          stream])

        edge_output: float = (stream_output * r_prob)

        LOG.debug("Output from %s-%d to %s-%d on stream %s is "
                  "calculated as %f", source_component, source_task,
                  out_edge["destination_component"], destination_task,
                  edge_output, edge_output)

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

            coefficent: float = float(coefficients.loc[
                source_task, out_stream, in_stream_name,
                source_component])

            output_rate += (stream_arrivals * coefficent)

        output_rates[source_task][out_stream] = output_rate

    return output_rates

def _convert_to_df(arrival_rates: ARRIVAL_RATES) -> pd.DataFrame:

    output: List[Dict[str, Union[str, float]]] = []

    for task_id, incoming_streams_dict in arrival_rates.items():
        for (incoming_stream, source_component), arrival_rate \
                in incoming_streams_dict.items():
            row: Dict[str, Union[str, float]] = {
                "task" : task_id,
                "incoming_stream" : incoming_stream,
                "source_component" : source_component,
                "arrival_rate" : arrival_rate}
            output.append(row)

    return pd.DataFrame(output)

def calculate_arrival_rates(graph_client: GremlinClient,
                            metrics_client: HeronMetricsClient,
                            topology_id: str, topology_ref: str,
                            start: dt.datetime, end: dt.datetime,
                            io_bucket_length: int,
                            spout_state: Dict[int, Dict[str, float]],
                            **kwargs: Union[str, int, float]) -> pd.DataFrame:

    LOG.info("Calculating arrival rates for topology %s reference %s using "
             "metrics from a %d second period from %s to %s", topology_id,
             topology_ref, (end-start).total_seconds(), start.isoformat(),
             end.isoformat())

    i2i_rps, levels, coefficients = _setup_arrival_calcs(
        metrics_client, graph_client, topology_id, topology_ref, start, end,
        io_bucket_length, **kwargs)

    topo_traversal: GraphTraversalSource = \
        graph_client.topology_subgraph(topology_id, topology_ref)

    arrival_rates: ARRIVAL_RATES = defaultdict(lambda: defaultdict(float))
    output_rates: DefaultDict[int, Dict[str, float]] = defaultdict(dict)
    output_rates.update(spout_state)

    # Step through the tree levels and calculate the output from each level and
    # the arrivals at the next
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

    return _convert_to_df(arrival_rates)
