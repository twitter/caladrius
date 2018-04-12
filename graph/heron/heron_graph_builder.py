""" This module contains classes and methods for constructing graph
representations of Heron logical and physical plans within the Caladrius Graph
Database."""

import logging

from typing import List, Dict, Union

from gremlin_python.structure.graph import Graph
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

    def build_topology_graph(self, topology_id: str, cluster: str,
                             environ: str):

        LOG.info("Building topology %s from cluster %s, environ %s",
                 topology_id, cluster, environ)

        # Create the reference for this topology graph so it can be
        # distinguished from other layouts for this topology
        # TODO: Figure out how to do the referencing
        topology_ref: str = "test"

        logical_plan: Dict[str, Union[str, int]] = \
                get_logical_plan(self.tracker_url, cluster, environ,
                                 topology_id)

        physical_plan: Dict[str, Union[str, int]] = \
                get_physical_plan(self.tracker_url, cluster, environ,
                                  topology_id)

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

        # Create the spouts
        physical_spouts: Dict[str, List[str]] = physical_plan["spouts"]

        for spout_name, spout_data in logical_plan["spouts"].items():
            LOG.debug("Creating vertices for instances of spout component: %s",
                      spout_name)
            for instance_name in physical_spouts[spout_name]:

                instance: Dict[str, Union[str, int]] = \
                        parse_instance_name(instance_name)

                LOG.debug("Creating vertex for instance: %s", instance_name)

                stream_manager: str = \
                    physical_plan["instances"][instance_name]["stmgrId"]

                spout = (self.graph_traversal.addV("spout")
                         .property("container", instance["container"])
                         .property("task_id", instance["task_id"])
                         .property("component", spout_name)
                         .property("stream_manager", stream_manager)
                         .property("spout_type", spout_data["spout_type"])
                         .property("spout_source", spout_data["spout_source"])
                         .property("topology_id", topology_id)
                         .property("topology_ref", topology_ref)
                         .next())

                # Connect this spout to its stream manager
                LOG.debug("Creating physical connection between instance: %s "
                          "and stream manager: %s", instance_name,
                          stream_manager)

                #(self.graph_traversal.V()
                #.hasLabel("stream_manager"))

        # Process the bolts and add logical connections
        physical_bolts: Dict[str, List[str]] = physical_plan["bolts"]

        for bolt_name, bolt_data in logical_plan["bolts"].items():
            LOG.debug("Creating vertices for instances of bolt component: %s",
                      bolt_name)
            for instance_name in physical_bolts[bolt_name]:

                instance: Dict[str, Union[str, int]] = \
                        parse_instance_name(instance_name)

                LOG.debug("Creating vertex for instance: %s", instance_name)

                stream_manager: str = \
                    physical_plan["instances"][instance_name]["stmgrId"]

                (self.graph_traversal.addV("bolt")
                 .property("container", instance["container"])
                 .property("task_id", instance["task_id"])
                 .property("component", bolt_name)
                 .property("stream_manager", stream_manager)
                 .property("topology_id", topology_id)
                 .property("topology_ref", topology_ref)
                 .next())
