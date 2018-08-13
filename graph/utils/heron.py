# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains utility methods for dealing with physical graphs in
the graph database."""

import logging
import datetime as dt
import os
import json
from collections import defaultdict
from multiprocessing import Process, Queue
from string import Template
from typing import List, Dict, Any, Optional, Tuple
from gremlin_python.process.graph_traversal import outE

from caladrius.graph.gremlin.client import GremlinClient
from caladrius.graph.builder.heron import builder
from caladrius.common.heron import tracker
from caladrius.common.heron import zookeeper

LOG: logging.Logger = logging.getLogger(__name__)

# The format is paths/topologyname_cluster_environ_time.json
file_path_template = Template("paths/$topology-$cluster-$environ-$time.json")


def find_all_paths(parent_to_child, start, path=[], path_dict=defaultdict()):
    """This is a recursive function that finds all paths from the given start node to
    sinks and returns them."""
    p = path.copy()
    p.append(start)
    if start not in parent_to_child:
        return p, path_dict
    paths = []
    for node in parent_to_child[start]:
        if node in path_dict:
            new_paths = path_dict[node]
        else:
            new_paths, path_dict = find_all_paths(parent_to_child, node, p, path_dict)
            path_dict[node] = new_paths.copy()
        if type(new_paths[0]) is int:
            # there is only one downstream path from current node
            paths.append(new_paths)
        else:
            # there are multiple downstream paths from current node
            for new_path in new_paths:
                paths.append(new_path)
    # LOG.info("Returning paths for node: %d", start)
    return paths, path_dict


def path_helper(parent_to_child: dict, spouts: List) -> List:
    """This is a helper function that creates a separate process for every spout
    and uses that process to calculate all paths to sinks for that spout. This function is
    expected to be compute intensive and so, multi-processing is required."""
    paths: List = []
    path_dict = defaultdict()
    for spout in spouts:
        results, path_dict = find_all_paths(parent_to_child, spout, path_dict=path_dict)
        for result in results:
            paths.append(result)
    return paths


def get_all_paths(graph_client: GremlinClient, topology_id: str) -> List[List[str]]:
    """ This function first gets all spouts from gremlin. Then it creates a dictionary,
    mapping all tasks to downstream tasks. It passes this dictionary along to another function
    that calculates all paths from the provided spouts to sinks.

    Arguments:
        graph_client (GremlinClient): The client instance for the graph
                                      database.
        topology_id (str):  The topology ID string.

    Returns:
        All possible paths from sources to sinks.
    """
    LOG.info("Graph size: %d vertices %d edges",
             graph_client.graph_traversal.V().count().toList()[0],
             graph_client.graph_traversal.E().count().toList()[0])
    conn_type = "logically_connected"
    start: dt.datetime = dt.datetime.now()

    spouts = graph_client.graph_traversal.V().has("topology_id", topology_id).\
        hasLabel("spout").where(outE(conn_type)).dedup().toList()
    spout_tasks = []
    parent_to_child = dict()
    for spout in spouts:
        downstream_task_vertices = [spout]
        spout_tasks.append(graph_client.graph_traversal.V(spout).properties('task_id').value().next())
        while len(downstream_task_vertices) != 0:
            for vertex in downstream_task_vertices:
                vertex_task_id = graph_client.graph_traversal.V(vertex).properties('task_id').value().next()
                downstream_task_vertices = graph_client.graph_traversal.V(vertex).out(conn_type).dedup().toList()
                downstream_task_ids = graph_client.graph_traversal.V(vertex).out(conn_type).properties(
                    'task_id').value().dedup().toList()
                if len(downstream_task_vertices) != 0:
                    parent_to_child[vertex_task_id] = downstream_task_ids

    paths: List = path_helper(parent_to_child, spout_tasks)

    LOG.info("Number of paths returned: %d", len(paths))
    end: dt.datetime = dt.datetime.now()
    LOG.info("Time spent in fetching all paths: %d seconds", (end - start).total_seconds())

    return paths


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
        second is the graph creation datetime.
    """

    LOG.info("Finding the most recent graph reference for topology: %s",
             topology_id)

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

    LOG.info("Checking if the physical plan in the graph database for "
             "topology: %s is still current", topology_id)

    recent_topo_update_ts: dt.datetime = \
        zookeeper.last_topo_update_ts_html(zk_connection, zk_root_node, topology_id,
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


def read_paths(zk_config: Dict[str, any], topology_id: str, cluster: str, environ: str,) -> List:
    zookeeper_url = zk_config["heron.statemgr.connection.string"]
    parts = zookeeper_url.split(".")
    parts[1] = cluster
    zookeeper_url = ".".join(parts)

    recent_topo_update_ts: dt.datetime = zookeeper.last_topo_update_ts_html(zookeeper_url,
                                                                       zk_config["heron.statemgr.root.path"],
                                                                       topology_id, zk_config["zk.time.offset"])
    file_name = file_path_template.substitute(topology=topology_id,
                                              cluster=cluster,
                                              environ=environ,
                                              time=recent_topo_update_ts.strftime('%m_%d_%Y_%I_%M_%S'))

    with open(file_name) as file:
        path_data = json.load(file)

    return path_data["paths"]


def paths_check(graph_client: GremlinClient, zk_config: Dict[str, any],
                cluster: str, environ: str, topology_id: str):
    """ Checks to see if we have a file containing all paths for the topology)

        Arguments:
            graph_client (GremlinClient): The client instance for the graph
                                          database.
            zk_config (dict):   A dictionary containing ZK config information.
                                "heron.statemgr.connection.string" and
                                "heron.statemgr.root.path" should be present.
            cluster (str):  The name of the cluster the topology is running on.
            environ (str):  The environment the topology is running in.
            topology_id (str):  The topology ID string.
        """

    zookeeper_url = zk_config["heron.statemgr.connection.string"]
    parts = zookeeper_url.split(".")
    parts[1] = cluster
    zookeeper_url = ".".join(parts)

    LOG.info(zookeeper_url)
    recent_topo_update_ts: dt.datetime =  zookeeper.last_topo_update_ts_html(zookeeper_url,
                                                                        zk_config["heron.statemgr.root.path"],
                                                                        topology_id, zk_config["zk.time.offset"])

    # test to see if a file exists with the right name
    file_name = file_path_template.substitute(topology=topology_id,
                                              cluster=cluster,
                                              environ=environ,
                                              time=recent_topo_update_ts.strftime('%m_%d_%Y_%I_%M_%S'))

    LOG.info("Paths file: %s", file_name)
    if not os.path.exists(file_name):
        # fetch paths and then write to file
        all_paths = get_all_paths(graph_client, topology_id)

        # writing
        with open(file_name, "w+") as file:
            json.dump({'paths': all_paths}, file)


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

    zookeeper_url = zk_config["heron.statemgr.connection.string"]
    parts = zookeeper_url.split(".")
    parts[1] = cluster
    zookeeper_url = ".".join(parts)

    LOG.info("Zookeeper URL: %s", zookeeper_url)

    if not most_recent_graph:

        LOG.info("There are currently no physical graphs in the database "
                 "for topology %s", topology_id)

        topology_ref: str = _build_graph(graph_client, tracker_url, cluster,
                                         environ, topology_id)

    elif not _physical_plan_still_current(
            topology_id, most_recent_graph[1],
            zookeeper_url,
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
