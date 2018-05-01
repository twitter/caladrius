""" This module contains classes and methods for constructing graph
representations of Heron logical and physical plans within the Caladrius Graph
Database."""

import logging

import datetime as dt

from typing import List, Dict, Union, Any

from gremlin_python.process.traversal import P
from gremlin_python.process.graph_traversal import __
from gremlin_python.structure.graph import Vertex, Edge

from caladrius.common.heron import tracker
from caladrius.graph.client.gremlin.client import GremlinClient
from caladrius.graph.builder.heron.routing_probability \
    import set_shuffle_routing_probs, set_fields_routing_probs
from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

# pylint: disable = too-many-arguments

def _create_stream_managers(graph_client: GremlinClient, topology_id: str,
                            topology_ref: str, physical_plan: Dict[str, Any]
                           ) -> None:

    LOG.info("Creating stream managers and container vertices")

    for stream_manager in physical_plan["stmgrs"].values():

        # Create the stream manager vertex
        LOG.debug("Creating vertex for stream manager: %s",
                  stream_manager["id"])

        strmg: Vertex = (graph_client.graph_traversal
                         .addV("stream_manager")
                         .property("id", stream_manager["id"])
                         .property("host", stream_manager["host"])
                         .property("port", stream_manager["port"])
                         .property("topology_id", topology_id)
                         .property("topology_ref", topology_ref)
                         .next())

        # Create the stream manager vertex
        container: int = int(stream_manager["id"].split("-")[1])

        LOG.debug("Creating vertex for container: %d", container)

        cont: Vertex = (graph_client.graph_traversal
                        .addV("container")
                        .property("id", container)
                        .property("topology_id", topology_id)
                        .property("topology_ref", topology_ref)
                        .next())

        # Connect the stream manager to the container
        LOG.debug("Connecting stream manager %s to be within container %d",
                  stream_manager["id"], container)
        (graph_client.graph_traversal.V(strmg).addE("is_within").to(cont)
         .next())

def _create_spouts(graph_client: GremlinClient, topology_id: str,
                   topology_ref: str,
                   physical_plan: Dict[str, Any],
                   logical_plan: Dict[str, Any]) -> None:

    # Create the spouts
    physical_spouts: Dict[str, List[str]] = physical_plan["spouts"]

    for spout_name, spout_data in logical_plan["spouts"].items():
        LOG.debug("Creating vertices for instances of spout component: %s",
                  spout_name)
        for instance_name in physical_spouts[spout_name]:

            instance: Dict[str, Union[str, int]] = \
                    tracker.parse_instance_name(instance_name)

            LOG.debug("Creating vertex for spout instance: %s", instance_name)

            stream_manager_id: str = \
                physical_plan["instances"][instance_name]["stmgrId"]

            spout: Vertex = (graph_client.graph_traversal
                             .addV("spout")
                             .property("container", instance["container"])
                             .property("task_id", instance["task_id"])
                             .property("component", spout_name)
                             .property("stream_manager", stream_manager_id)
                             .property("spout_type",
                                       spout_data["spout_type"])
                             .property("spout_source",
                                       spout_data["spout_source"])
                             .property("topology_id", topology_id)
                             .property("topology_ref", topology_ref)
                             .next())

            # Connect the spout to its container vertex
            (graph_client.graph_traversal.V(spout).addE("is_within")
             .to(graph_client.graph_traversal.V()
                 .hasLabel("container")
                 .has("topology_id", topology_id)
                 .has("topology_ref", topology_ref)
                 .has("id", instance["container"])
                )
             .next())

def _create_bolts(graph_client: GremlinClient, topology_id: str,
                  topology_ref: str,
                  physical_plan: Dict[str, Any],
                  logical_plan: Dict[str, Any]) -> None:

    # Create all the bolt vertices
    physical_bolts: Dict[str, List[str]] = physical_plan["bolts"]

    for bolt_name in logical_plan["bolts"]:
        LOG.debug("Creating vertices for instances of bolt component: %s",
                  bolt_name)
        for instance_name in physical_bolts[bolt_name]:

            instance: Dict[str, Union[str, int]] = \
                tracker.parse_instance_name(instance_name)

            LOG.debug("Creating vertex for bolt instance: %s",
                      instance_name)

            stream_manager_id: str = \
                physical_plan["instances"][instance_name]["stmgrId"]

            bolt: Vertex = (graph_client.graph_traversal
                            .addV("bolt")
                            .property("container", instance["container"])
                            .property("task_id", instance["task_id"])
                            .property("component", bolt_name)
                            .property("stream_manager", stream_manager_id)
                            .property("topology_id", topology_id)
                            .property("topology_ref", topology_ref)
                            .next())

            # Connect the bolt to its container vertex
            (graph_client.graph_traversal.V(bolt).addE("is_within")
             .to(graph_client.graph_traversal.V()
                 .hasLabel("container")
                 .has("topology_id", topology_id)
                 .has("topology_ref", topology_ref)
                 .has("id", instance["container"])
                )
             .next())

def _create_logical_connections(graph_client: GremlinClient, topology_id: str,
                                topology_ref: str,
                                logical_plan: Dict[str, Any]
                               ) -> None:

    # Add all the logical connections between the topology's instances
    LOG.info("Adding logical connections to topology %s instances",
             topology_id)

    for bolt_name, bolt_data in logical_plan["bolts"].items():

        LOG.debug("Adding logical connections for instances of "
                  "destination bolt: %s", bolt_name)

        # Get a list of all instance vertices for this bolt
        destination_instances: List[Vertex] = (
            graph_client.graph_traversal.V()
            .has("topology_id", topology_id)
            .has("topology_ref", topology_ref)
            .has("component", bolt_name)
            .toList())

        for incoming_stream in bolt_data["inputs"]:
            source_instances: List[Vertex] = (
                graph_client.graph_traversal.V()
                .has("topology_id", topology_id)
                .has("topology_ref", topology_ref)
                .has("component", incoming_stream["component_name"])
                .toList())

            for destination in destination_instances:
                for source in source_instances:
                    (graph_client.graph_traversal.V(source)
                     .addE("logically_connected")
                     .property("stream",
                               incoming_stream["stream_name"])
                     .property("grouping", incoming_stream["grouping"])
                     .to(destination).next())

def _create_physical_connections(graph_client: GremlinClient, topology_id: str,
                                 topology_ref: str) -> None:

    LOG.info("Creating physical connections for topology: %s, reference: "
             "%s", topology_id, topology_ref)

    # First get all logically connected pairs
    logical_pairs: List[Dict[str, Union[Vertex, Edge]]] = (
        graph_client.graph_traversal.V()
        .has("topology_id", topology_id)
        .has("topology_ref", topology_ref)
        .hasLabel(P.within("bolt", "spout")).as_("source")
        .outE("logically_connected").as_("l_edge")
        .inV().as_("destination")
        .select("source", "l_edge", "destination")
        .toList())

    LOG.debug("Processing %d logical connections", len(logical_pairs))

    for pair in logical_pairs:
        source = pair["source"]
        destination = pair["destination"]
        logical_edge = pair["l_edge"]

        # Are these instances within the same container
        # TODO: There is probably a better Gremlin query to do this
        # automatically
        source_container: Vertex = (graph_client.graph_traversal
                                    .V(source)
                                    .out("is_within")
                                    .hasLabel("container")
                                    .next())

        destination_container: Vertex = (graph_client.graph_traversal
                                         .V(destination)
                                         .out("is_within")
                                         .hasLabel("container")
                                         .next())

        # Get the source stream manager, which will be the start of the
        # physical connection path
        source_strmg: Vertex = (graph_client.graph_traversal
                                .V(source_container)
                                .in_("is_within")
                                .hasLabel("stream_manager")
                                .next())

        # Connect the source instance to its stream manager, checking first
        # if the connection already exists
        (graph_client.graph_traversal.V(source)
         .coalesce(__.out("physically_connected").is_(source_strmg),
                   __.addE("physically_connected").to(source_strmg))
         .next())

        if source_container == destination_container:

            # If the source and destination instances are in the same
            # container then they share the same stream manager so just use
            # the source stream manager found above. Connect the source
            # stream manager to the destination instance

            (graph_client.graph_traversal.V(source_strmg)
             .coalesce(__.out("physically_connected").is_(destination),
                       __.addE("physically_connected").to(destination))
             .next())

            # Set the logical edge for this pair to "local"
            (graph_client.graph_traversal.E(logical_edge)
             .property("type", "local").next())

        else:

            # Get the share stream manager for the destination instance
            destination_strmg: Vertex = (graph_client.graph_traversal
                                         .V(destination_container)
                                         .in_("is_within")
                                         .hasLabel("stream_manager")
                                         .next())

            # Connect the two stream managers (if they aren't already)
            (graph_client.graph_traversal.V(source_strmg)
             .coalesce(
                 __.out("physically_connected").is_(destination_strmg),
                 __.addE("physically_connected").to(destination_strmg))
             .next())

            (graph_client.graph_traversal.V(destination_strmg)
             .coalesce(__.out("physically_connected").is_(destination),
                       __.addE("physically_connected").to(destination))
             .next())

            # Set the logical edge for this pair to "remote"
            (graph_client.graph_traversal.E(logical_edge)
             .property("type", "remote").next())

def create_physical_graph(graph_client: GremlinClient,
                          topology_id: str, topology_ref: str,
                          logical_plan: Dict[str, Union[str, int]],
                          physical_plan: Dict[str, Union[str, int]]) -> None:
    """ This method will build the physical graph of the specified topology
    in the caladrius graph database. It will attach the supplied reference
    to all vertices of this topology physical graph.

    Arguments:
        graph_client (GremlinClient):   The client instance for the graph
                                        database.
        topology_id (str):  The topology identification string
        topology_ref (str): The unique reference string for this topology
                            physical graph.
        logical_plan (dict):    Dictionary describing the logical plan of the
                                topology. This should match the format of the
                                logical plan returned by the Heron tracker
                                API.
        physical_plan (dict):   Dictionary describing the physical plan of the
                                topology. This should match the format of the
                                physical plan returned by the Heron tracker
                                API.
    Raises:
        RuntimeError:   If the graph database already contains entries with
                        the supplied topology ID and reference.
    """

    if graph_client.topology_ref_exists(topology_id, topology_ref):
        msg: str = (f"A graph of topology {topology_id} with reference "
                    f"{topology_ref} is already present in the graph "
                    f"database.")
        LOG.error(msg)
        raise RuntimeError(msg)

    LOG.info("Building physical graph for topology %s reference %s",
             topology_id, topology_ref)

    _create_stream_managers(graph_client, topology_id, topology_ref,
                            physical_plan)

    _create_spouts(graph_client, topology_id, topology_ref, physical_plan,
                   logical_plan)

    _create_bolts(graph_client, topology_id, topology_ref, physical_plan,
                  logical_plan)

    _create_logical_connections(graph_client, topology_id, topology_ref,
                                logical_plan)

    _create_physical_connections(graph_client, topology_id, topology_ref)

def populate_physical_graph(graph_client: GremlinClient,
                            metrics_client: HeronMetricsClient,
                            topology_id: str, topology_ref: str,
                            start: dt.datetime, end: dt.datetime) -> None:
    """ Populates the specified graph with metrics gathered from the defined
    time period.

    Arguments:
        graph_client (GremlinClient):   The client instance for the graph
                                        database.
        metrics_client (HeronMetricsClient):    The client instance for the
                                                metrics database.
        start (dt.datetime):    UTC datetime instance for the start of the
                                metrics gathering period.
        end (dt.datetime):  UTC datetime instance for the end of the metrics
                            gathering period.
        topology_id (str):  The topology identification string
        topology_ref (str): The unique reference string for this topology
                            physical graph.

    Raises:
        RuntimeError:   If the graph database does not contain a graph with the
                        supplied ID and reference.
    """

    if not graph_client.topology_ref_exists(topology_id, topology_ref):
        msg: str = (f"A graph of topology {topology_id} with reference "
                    f"{topology_ref} is not present in the graph "
                    f"database and therefore cannot be populated")
        LOG.error(msg)
        raise RuntimeError(msg)

    LOG.info("Populating topology %s reference %s physical graph with "
             "with metrics data from a %d second window from %s and %s",
             topology_id, topology_ref, (end - start).total_seconds(),
             start.isoformat(), end.isoformat())

    set_shuffle_routing_probs(graph_client, topology_id, topology_ref)

    set_fields_routing_probs(graph_client, metrics_client, topology_id,
                             topology_ref, start, end)
