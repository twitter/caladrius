""" This module contains methods for calculating the routing probabilities of
heron topologies. """

from gremlin_python.process.graph_traversal import GraphTraversalSource

def set_shuffle_rps(graph_traversal: GraphTraversalSource) -> None:
    """ This method will set the routing probability for shuffle connections in
    the graph available to the traversal source.
    """
    pass
