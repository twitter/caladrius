""" This module contains classes and methods for connecting to and
communicating with a Gremlin Server instance. """

import logging

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __, GraphTraversalSource
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.driver.driver_remote_connection \
        import DriverRemoteConnection

from caladrius.graph.client.graph_client import GraphClient

LOG: logging.Logger = logging.getLogger(__name__)

class GremlinClient(GraphClient):
    """ Graph client implementation for the TinkerPop Gremlin Server """

    def __init__(self, config: dict, graph_name: str = "g") -> None:
        super().__init__(config)
        self.gremlin_server_url: str = self.config["gremlin.server.url"]

        # Create remote graph traversal object
        LOG.info("Connecting to graph database at: %s",
                 self.gremlin_server_url)

        self.graph_name: str = graph_name
        self.graph: Graph = Graph()
        self.connect()

    def connect(self):
        """ Creates (or refreshes) the remote connection to the gremlin server.
        """
        self.graph_traversal: GraphTraversalSource = \
            self.graph.traversal().withRemote(DriverRemoteConnection(
                f"ws://{self.gremlin_server_url}/gremlin", self.graph_name))

    def topology_ref_exists(self, topology_id: str, topology_ref: str) -> bool:
        """ Checks weather vertices exist in the graph database with the
        supplied topology id and ref values.

        Arguments:
            topology_id (str):  The topology identification string.
            topology_ref (str): The reference string to check for.

        Returns:
            Boolean flag indicating if vertices with the supplied ID and
            reference are present (True) or not (False) in the graph database.
        """

        num_vertices: int = len((self.graph_traversal.V()
                                 .has("topology_id", topology_id)
                                 .has("topology_ref", topology_ref)
                                 .toList()))

        if num_vertices:
            LOG.debug("%d vertices with the topology id: %s and reference: %s "
                      "are present in the graph database.", num_vertices,
                      topology_id, topology_ref)
            return True
        else:
            LOG.debug("Topology: %s reference: %s is not present in the graph "
                      "database", topology_id, topology_ref)

        return False

    def topology_subgraph(self, topology_id: str,
                          topology_ref: str) -> GraphTraversalSource:
        """ Gets a gremlin graph traversal source limited to the sub-graph of
        vertices with the supplied topology ID and topology reference
        properties.

        Arguments:
            topology_id (str):  The topology identification string.
            topology_ref (str): The reference string for the version of the
                                topology you want to sub-graph.

        Returns:
            A GraphTraversalSource instance linked to the desired sub-graph
        """

        LOG.info("Creating traversal source for topology %s subgraph with "
                 "reference: %s", topology_id, topology_ref)

        topo_graph_traversal: GraphTraversalSource = \
            self.graph_traversal.withStrategies(
                SubgraphStrategy(vertices=__.has("topology_ref", topology_ref)
                                 .has("topology_id", topology_id)))

        return topo_graph_traversal
