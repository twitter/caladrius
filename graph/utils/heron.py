# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains utility methods for dealing with physical graphs in
the graph database."""

import logging

import datetime as dt

from typing import List, Dict, Any, Optional, Tuple
from gremlin_python.process.graph_traversal import (not_, out, outE)

from caladrius.graph.gremlin.client import GremlinClient
from caladrius.graph.builder.heron import builder
from caladrius.common.heron import tracker
from caladrius.common.heron import zookeeper

LOG: logging.Logger = logging.getLogger(__name__)


def get_all_paths(graph_client: GremlinClient, topology_id: str) -> List[List[str]]:
    """ Gets all paths from sources to sinks.

    Arguments:
        graph_client (GremlinClient): The client instance for the graph
                                      database.
        topology_id (str):  The topology ID string.

    Returns:
        All possible paths from sources to sinks.
    """
    paths = graph_client.graph_traversal.V().hasLabel("spout").\
        has("topology_id", topology_id).repeat(out("logically_connected").simplePath()).\
        until(not_(outE("logically_connected"))).path().dedup().toList()

    paths_list = [[] for x in range(len(paths))]

    for x in range(len(paths)):
        for v in paths[x]:
            paths_list[x].append(graph_client.graph_traversal.V().hasId(v).values('task_id').next())

    return paths_list

def get_current_refs(graph_client: GremlinClient,
                     topology_id: str) -> List[str]:
    """ Gets a list of topology reference strings for graphs with the supplied
    topology id.

    Arguments:
        graph_client (GremlinClient): The client instance for the graph
                                      database.
        topology_id (str):  The topology ID string.

    Returns:
        A list of topology reference strings.
    """

    refs: List[str] = (graph_client.graph_traversal.V()
                       .has("topology_id", topology_id)
                       .values("topology_ref").dedup().toList())

    return [ref for ref in refs if "current" in ref]


def most_recent_graph_ref(graph_client: GremlinClient, topology_id: str
                          ) -> Optional[Tuple[str, dt.datetime]]:
    """ Gets the most recent topology reference, for the supplied topology ID
    in a tuple with the creation datetime object.

    Arguments:
        graph_client (GremlinClient): The client instance for the graph
                                      database.
        topology_id (str):  The topology ID string.

    Returns:
        A 2-tuple where the first item is the topology reference string and the
        second if the graph creation datetime.
    """

    current_refs: List[str] = get_current_refs(graph_client, topology_id)

    if current_refs:
        time_list: List[Tuple[str, dt.datetime]] = []
        for ref in current_refs:
            timestamp: str = ref.split("/")[1].split("+")[0]
            time_dt: dt.datetime = dt.datetime.strptime(timestamp,
                                                        "%Y-%m-%dT%H:%M:%S.%f")
            time_list.append((ref, time_dt.astimezone(dt.timezone.utc)))

        return sorted(time_list, key=lambda x: x[1])[-1]
    else:
        LOG.info("No graphs found for topology %s", topology_id)

    return None


def _physical_plan_still_current(topology_id: str,
                                 most_recent_graph_ts: dt.datetime,
                                 zk_connection: str, zk_root_node: str,
                                 zk_time_offset: int) -> bool:

    recent_topo_update_ts: dt.datetime = \
        zookeeper.last_topo_update_ts(zk_connection, zk_root_node, topology_id,
                                      zk_time_offset)

    if most_recent_graph_ts > recent_topo_update_ts:
        return True

    return False

def _build_graph(graph_client: GremlinClient, tracker_url: str, cluster: str,
                 environ: str, topology_id: str, ref_prefix: str = "current"
                 ) -> str:

    topology_ref: str = (ref_prefix + "/" +
                         dt.datetime.now(dt.timezone.utc).isoformat())

    logical_plan: Dict[str, Any] = \
        tracker.get_logical_plan(tracker_url, cluster, environ, topology_id)

    physical_plan: Dict[str, Any] = \
        tracker.get_physical_plan(tracker_url, cluster, environ, topology_id)

    builder.create_physical_graph(graph_client, topology_id,
                                  topology_ref, logical_plan,
                                  physical_plan)

    return topology_ref


def graph_check(graph_client: GremlinClient, zk_config: Dict[str, Any],
                tracker_url: str, cluster: str, environ: str,
                topology_id: str) -> str:
    """ Checks to see if the specified topology has an entry in the graph
    database and if so whether that entry was created since the latest change
    to the physical plan object stored in the ZooKeeper cluster (defined in the
    supplied config object)

    Arguments:
        graph_client (GremlinClient): The client instance for the graph
                                      database.
        zk_config (dict):   A dictionary containing ZK config information.
                            "heron.statemgr.connection.string" and
                            "heron.statemgr.root.path" should be present.
        tracker_url (str):  The URL for the Heron Tracker API.
        cluster (str):  The name of the cluster the topology is running on.
        environ (str):  The environment the topology is running in.
        topology_id (str):  The topology ID string.

    Returns:
        The topology reference for the physical graph that was either found or
        created in the graph database.
    """

    most_recent_graph: Optional[Tuple[str, dt.datetime]] = \
        most_recent_graph_ref(graph_client, topology_id)

    if not most_recent_graph:

        LOG.info("There are currently no physical graphs in the database "
                 "for topology %s", topology_id)

        topology_ref: str = _build_graph(graph_client, tracker_url, cluster,
                                         environ, topology_id)

    elif not _physical_plan_still_current(
            topology_id, most_recent_graph[1],
            zk_config["heron.statemgr.connection.string"],
            zk_config["heron.statemgr.root.path"],
            zk_config["zk.time.offset"]):

        LOG.info("The physical plan for topology %s has changed since "
                 "the last physical graph (reference: %s) was built",
                 topology_id, most_recent_graph[0])

        topology_ref = _build_graph(graph_client, tracker_url, cluster,
                                    environ, topology_id)
    else:

        topology_ref = most_recent_graph[0]
        LOG.info("The current physical plan for topology %s is already "
                 "represented in the graph database with reference %s",
                 topology_id, topology_ref)

    return topology_ref
