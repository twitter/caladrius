# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains classes and methods for connecting to and
communicating with a Gremlin Server instance. """

import logging
import errno

from socket import error as socket_error

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import has, GraphTraversalSource
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.driver.driver_remote_connection \
        import DriverRemoteConnection

from caladrius.config.keys import ConfKeys

LOG: logging.Logger = logging.getLogger(__name__)


class GremlinClient(object):
    """ Client class for the TinkerPop Gremlin Server """

    def __init__(self, config: dict, graph_name: str = "g") -> None:
        self.config: dict = config
        self.gremlin_server_url: str = \
            self.config[ConfKeys.GREMLIN_SERVER_URL.value]

        # Create remote graph traversal object
        LOG.info("Connecting to graph database at: %s",
                 self.gremlin_server_url)

        self.graph_name: str = graph_name
        self.graph: Graph = Graph()
        self.connect()

    def __hash__(self) -> int:

        return hash((self.gremlin_server_url, self.graph_name))

    def __eq__(self, other: object) -> bool:

        if not isinstance(other, GremlinClient):
            return False

        if ((self.gremlin_server_url == other.gremlin_server_url) and
                (self.graph_name == other.graph_name)):
            return True

        return False

    def connect(self) -> None:
        """ Creates (or refreshes) the remote connection to the gremlin server.

        Raises:
            ConnectionRefusedError: If the gremlin sever at the configured
                                    address cannot be found.
        """

        connect_str: str = f"ws://{self.gremlin_server_url}/gremlin"

        try:
            self.graph_traversal: GraphTraversalSource = \
                self.graph.traversal().withRemote(
                    DriverRemoteConnection(connect_str, self.graph_name))
        except socket_error as serr:
            if serr.errno != errno.ECONNREFUSED:
                # Not the error we are looking for, re-raise
                LOG.error("Socket error occurred")
                raise serr
            # connection refused
            msg: str = (f"Connection to gremlin sever at: "
                        f"{self.gremlin_server_url} using connection string: "
                        f"{connect_str} was refused. Is the server active?")
            LOG.error(msg)
            raise ConnectionRefusedError(msg)

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

        return False

    def raise_if_missing(self, topology_id: str, topology_ref: str) -> None:
        """ Checks weather vertices exist in the graph database with the
        supplied topology id and ref values and raises a error if they don't.

        Arguments:
            topology_id (str):  The topology identification string.
            topology_ref (str): The reference string to check for.

        Raises:
            RuntimeError:   If vertices with the supplied topology ID and
                            reference are not present in the graph database.
        """
        if not self.topology_ref_exists(topology_id, topology_ref):
            msg: str = (f"Topology: {topology_id} reference: {topology_ref} "
                        f"is not present in the graph database")
            LOG.error(msg)
            raise RuntimeError(msg)

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

        LOG.debug("Creating traversal source for topology %s subgraph with "
                  "reference: %s", topology_id, topology_ref)

        topo_graph_traversal: GraphTraversalSource = \
            self.graph_traversal.withStrategies(
                SubgraphStrategy(vertices=has("topology_ref", topology_ref)
                                 .has("topology_id", topology_id)))

        return topo_graph_traversal
