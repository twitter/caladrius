""" This module contains methods for calculating the routing probabilities of
heron topologies. """

from typing import List, Dict, Union

from gremlin_python.process.traversal import P
from gremlin_python.process.graph_traversal import __
from gremlin_python.structure.graph import Vertex
from gremlin_python.process.graph_traversal import GraphTraversalSource

def get_component_connections_by_grouping(
    graph_traversal: GraphTraversalSource, grouping: str
    ) -> List[Dict[str, str]]:

    component_connections: List[Dict[str, str]] = \
        (graph_traversal.V().hasLabel(P.within("bolt","spout")).as_("source")
         .outE("logically_connected").has("grouping", grouping).as_("stream")
         .inV().as_("destination").select("source", "stream", "destination")
         .by("component").by("stream_name").by("component").dedup().toList())

    return component_connections

def set_shuffle_routing_probs(
        graph_traversal: GraphTraversalSource) -> None:
    """ This method will set the routing probability for shuffle connections in
    the graph available to the traversal source.
    """

    for comp_conn in get_component_connections_by_grouping(graph_traversal,
                                                           "SHUFFLE"):

        # Calculate the shuffle grouped connections routing probability based
        # on the number of downstream instances for this connections
        shuffle_rp: float = (graph_traversal.V()
                             .has("component", comp_conn["destination"])
                             .count().math("1/_").next())

        # Apply the calculated routing probability to all logical connections
        # with this stream name between the source and destination instances
        # of the these components
        (graph_traversal.V().has("component", comp_conn["source"])
         .outE("logically_connected").has("stream_name", comp_conn["stream"])
         .property("routing_probability", shuffle_rp)
         .inV().has("component", comp_conn["destination"])
         .iterate())
