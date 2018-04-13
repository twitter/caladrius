""" This module contains classes and methods for constructing graph
representations of Heron logical and physical plans within the Caladrius Graph
Database."""

import logging

from typing import List, Dict, Union

from gremlin_python.structure.graph import Graph, Vertex
from gremlin_python.driver.driver_remote_connection \
        import DriverRemoteConnection

from caladrius.graph.heron.heron_tracker \
        import get_logical_plan, get_physical_plan, parse_instance_name

LOG: logging.Logger = logging.getLogger(__name__)

class HeronGraphBuilder(object):

    def __init__(self, config: dict) -> None:
        self.config = config
        self.tracker_url = config["heron.tracker.url"]
        self.graph_db_url = config["caladrius.graph.db.url"]

        self.graph = Graph()
        self.graph_traversal = self.graph.traversal().withRemote(
            DriverRemoteConnection(f"ws://{self.graph_db_url}/gremlin", 'g'))

    def _create_stream_managers(self, topology_id: str, topology_ref: str,
                                physical_plan: Dict[str, Union[str, int]],
                                logical_plan: Dict[str, Union[str, int]]
                               ) -> None:

        LOG.info("Creating stream managers vertices")

        # Create the stream manager vertices
        for stream_manager in physical_plan["stmgrs"].values():

            LOG.debug("Creating vertex for stream manager: %s",
                      stream_manager["id"])

            (self.graph_traversal.addV("stream_manager")
             .property("id", stream_manager["id"])
             .property("host", stream_manager["host"])
             .property("port", stream_manager["port"])
             .property("shell_port", stream_manager["shell_port"])
             .property("topology_id", topology_id)
             .property("topology_ref", topology_ref)
             .next())

        # Connect all stream managers to each other
        # TODO: Remove this step and create physical connections based on tuple
        # flow only.
        LOG.info("Creating connections between stream managers")
        stream_managers: List[Vertex] = (self.graph_traversal.V()
                                         .hasLabel("stream_manager")
                                         .has("topology_id", topology_id)
                                         .has("topology_ref", topology_ref)
                                         .toList())

        for strmg in stream_managers:
            for other_strmg in [x for x in stream_managers if x != strmg]:
                (self.graph_traversal.V(strmg)
                 .addE("physically_connected").to(other_strmg).next())
                (self.graph_traversal.V(other_strmg)
                 .addE("physically_connected").to(strmg).next())

    def _create_spouts(self, topology_id: str, topology_ref: str,
                       physical_plan: Dict[str, Union[str, int]],
                       logical_plan: Dict[str, Union[str, int]]) -> None:

        # Create the spouts
        physical_spouts: Dict[str, List[str]] = physical_plan["spouts"]

        for spout_name, spout_data in logical_plan["spouts"].items():
            LOG.debug("Creating vertices for instances of spout component: %s",
                      spout_name)
            for instance_name in physical_spouts[spout_name]:

                instance: Dict[str, Union[str, int]] = \
                        parse_instance_name(instance_name)

                LOG.debug("Creating vertex for instance: %s", instance_name)

                stream_manager_id: str = \
                    physical_plan["instances"][instance_name]["stmgrId"]

                spout = (self.graph_traversal.addV("spout")
                         .property("container", instance["container"])
                         .property("task_id", instance["task_id"])
                         .property("component", spout_name)
                         .property("stream_manager", stream_manager_id)
                         .property("spout_type", spout_data["spout_type"])
                         .property("spout_source", spout_data["spout_source"])
                         .property("topology_id", topology_id)
                         .property("topology_ref", topology_ref)
                         .next())

                # Connect this spout to its stream manager in both directions
                LOG.debug("Creating physical connection between instance: %s "
                          "and stream manager: %s", instance_name,
                          stream_manager_id)

                strmg = (self.graph_traversal.V()
                         .hasLabel("stream_manager")
                         .has("id", stream_manager_id)
                         .has("topology_id", topology_id)
                         .has("topology_ref", topology_ref)
                         .next())

                (self.graph_traversal.V(spout).addE("physically_connected")
                 .to(strmg).next())

    def _create_bolts(self, topology_id: str, topology_ref: str,
                       physical_plan: Dict[str, Union[str, int]],
                       logical_plan: Dict[str, Union[str, int]]) -> None:

        # Create all the bolt vertices
        physical_bolts: Dict[str, List[str]] = physical_plan["bolts"]

        for bolt_name in logical_plan["bolts"]:
            LOG.debug("Creating vertices for instances of bolt component: %s",
                      bolt_name)
            for instance_name in physical_bolts[bolt_name]:

                instance: Dict[str, Union[str, int]] = \
                        parse_instance_name(instance_name)

                LOG.debug("Creating vertex for instance: %s", instance_name)

                stream_manager_id: str = \
                    physical_plan["instances"][instance_name]["stmgrId"]

                bolt = (self.graph_traversal.addV("bolt")
                        .property("container", instance["container"])
                        .property("task_id", instance["task_id"])
                        .property("component", bolt_name)
                        .property("stream_manager", stream_manager_id)
                        .property("topology_id", topology_id)
                        .property("topology_ref", topology_ref)
                        .next())

                # Connect this bolt to its stream manager in both directions
                LOG.debug("Creating physical connection between instance: %s "
                          "and stream manager: %s", instance_name,
                          stream_manager_id)

                strmg: Vertex = (self.graph_traversal.V()
                                 .hasLabel("stream_manager")
                                 .has("id", stream_manager_id)
                                 .has("topology_id", topology_id)
                                 .has("topology_ref", topology_ref)
                                 .next())

                # TODO: Remove this step and create physical plan connections
                # based on logical tuple flow
                (self.graph_traversal.V(bolt).addE("physically_connected")
                 .to(strmg).next())
                (self.graph_traversal.V(strmg).addE("physically_connected")
                 .to(bolt).next())

    def _create_logical_connections(self, topology_id: str, topology_ref: str,
                                    logical_plan: Dict[str, Union[str, int]]
                                   ) -> None:

        # Add all the logical connections between the topology's instances
        LOG.info("Adding logical connections to topology %s instances",
                 topology_id)

        for bolt_name, bolt_data in logical_plan["bolts"].items():

            LOG.debug("Adding logical connections for instances of bolt: %s",
                      bolt_name)

            # Get a list of all instance vertices for this bolt
            destination_instances: List[Vertex] = (
                self.graph_traversal.V()
                .has("topology_id", topology_id)
                .has("topology_ref", topology_ref)
                .has("component", bolt_name)
                .toList())

            for incoming_stream in bolt_data["inputs"]:
                source_instances: List[Vertex] = (
                    self.graph_traversal.V()
                    .has("topology_id", topology_id)
                    .has("topology_ref", topology_ref)
                    .has("component", incoming_stream["component_name"])
                    .toList())

                for destination in destination_instances:
                    for source in source_instances:
                        (self.graph_traversal.V(source)
                         .addE("logically_connected")
                         .property("stream_name",
                                   incoming_stream["stream_name"])
                         .property("grouping", incoming_stream["grouping"])
                         .to(destination).next())

    def build_topology_graph(self, topology_id: str, topology_ref: str,
                             cluster: str, environ: str):

        LOG.info("Building topology %s from cluster %s, environ %s",
                 topology_id, cluster, environ)

        logical_plan: Dict[str, Union[str, int]] = \
                get_logical_plan(self.tracker_url, cluster, environ,
                                 topology_id)

        physical_plan: Dict[str, Union[str, int]] = \
                get_physical_plan(self.tracker_url, cluster, environ,
                                  topology_id)


        self._create_stream_managers(topology_id, topology_ref, physical_plan,
                                     logical_plan)

        self._create_spouts(topology_id, topology_ref, physical_plan,
                            logical_plan)

        self._create_bolts(topology_id, topology_ref, physical_plan,
                           logical_plan)

        self._create_logical_connections(topology_id, topology_ref,
                                         logical_plan)
