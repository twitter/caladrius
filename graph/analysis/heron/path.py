# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for analysing the paths through a topology graph. """

import logging

from typing import List, Dict, Union, Iterator
from functools import lru_cache

from gremlin_python.structure.graph import Vertex, Edge
from gremlin_python.process.graph_traversal import GraphTraversalSource, out, has, in_

from caladrius.graph.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)


def get_component_instance_lists(
    graph_client: GremlinClient, topology_id: str, topology_ref: str
) -> Dict[str, List[Vertex]]:
    """ Gets a dictionary mapping component name to lists of vertex instances of that
    component in the topology graph with the specified reference.

    Arguments:
        graph_client (GremlinClient):   The graph database client instance.
        topology_id (str):  The topology identification string.
        topology_ref (str): The topology graph identification string.

    Returns:
        Dict[str, List[Vertex]]:    A dictionary mapping component name to lists of vertex
        instances of that component in the topology graph.
    """

    sgt: GraphTraversalSource = graph_client.topology_subgraph(
        topology_id, topology_ref
    )

    component_names: List[str] = sgt.V().values("component").dedup().toList()

    output: Dict[str, List[Vertex]] = {}

    for component_name in component_names:

        output[component_name] = sgt.V().has("component", component_name).toList()

    return output


@lru_cache()
def get_source_and_sink_comps(
    graph_client: GremlinClient, topology_id: str, topology_ref: str
) -> Dict[str, List[str]]:
    """ Gets a dictionary with "sources" and "sinks" keys linking to lists of component
    names for the sources (instances with no incoming logical edges) and sinks (instances
    with no outgoing logical edges). This method is cached as the logical plan is fixed
    for the lifetime of a topology.

    Arguments:
        graph_client (GremlinClient):   The graph database client instance.
        topology_id (str):  The topology identification string.
        topology_ref (str): The topology graph identification string.

    Returns:
        Dict[str, List[str]]:   A dictionary with "sources" and "sinks" keys linking to
        lists of component names for the sources (instances with no incoming logical
        edges) and sinks (instances with no outgoing logical edges).
    """

    sgt: GraphTraversalSource = graph_client.topology_subgraph(
        topology_id, topology_ref
    )

    sources: List[str] = sgt.V().where(
        in_("logically_connected").count().is_(0)
    ).values("component").dedup().toList()

    sinks: List[str] = sgt.V().where(out("logically_connected").count().is_(0)).values(
        "component"
    ).dedup().toList()

    return {"sources": sources, "sinks": sinks}


@lru_cache()
def get_component_paths(
    graph_client: GremlinClient, topology_id: str, topology_ref: str
) -> List[List[str]]:
    """ Gets all component level paths through the specified topology. This method is
    cached as the component paths are fixed for the lifetime of a topology.

    Arguments:
        graph_client (GremlinClient):   The graph database client instance.
        topology_id (str):  The topology identification string.
        topology_ref (str): The topology graph identification string.

    Returns:
        List[List[str]]:    A list of component name string path lists. For example
        [["A", "B", "D"], ["A", "C", "D"]
    """

    sources_sinks: Dict[str, List[str]] = get_source_and_sink_comps(
        graph_client, topology_id, topology_ref
    )

    sgt: GraphTraversalSource = graph_client.topology_subgraph(
        topology_id, topology_ref
    )

    output: List[List[str]] = []

    for source in sources_sinks["sources"]:
        # Pick a start vertex for this source
        start: Vertex = sgt.V().has("component", source).next()
        for sink in sources_sinks["sinks"]:
            LOG.debug(
                "Finding paths from source component: %s to sink component: %s",
                source,
                sink,
            )
            # Find one path from the source vertex to any sink vertex and emit the
            # components as well as the edges.
            full_path: List[Union[str, Edge]] = (
                sgt.V(start)
                .repeat(out("logically_connected").simplePath())
                .until(has("component", sink))
                .path()
                .by("component")
                .by()
                .limit(1)
                .next()
            )

            # Filter out the edges and keep the component strings
            path: List[str] = [
                element for element in full_path if isinstance(element, str)
            ]

            output.append(path)

    return output


def path_combinations(comp_instances, depth=0) -> Iterator[List[Vertex]]:
    """ Returns a generator function which will yield combinations of instances in the
    supplied list of instance lists. Each call to next on the returned generator return a
    list of Vertex instances one form each of lists in the supplied list of instance
    lists. The Vertex's will be taken in the same order as the supplied lists. """

    for instance in comp_instances[0]:
        if len(comp_instances) > 1:
            for result in path_combinations(comp_instances[1:], depth + 1):
                yield [instance] + result
        else:
            yield [instance]


def create_path_generators(
    graph_client: GremlinClient, topology_id: str, topology_ref: str
) -> Dict[str, Iterator[List[Vertex]]]:
    """ This method creates generator functions which will provide instance paths for each
    of the possible component paths through the topology.

    Arguments:
        graph_client (GremlinClient):   The graph database client instance.
        topology_id (str):  The topology identification string.
        topology_ref (str): The topology graph identification string.

    Returns:
        A dictionary mapping from the component level path string to a generator for the
        instance paths (list of Vertex instances). Each key is a string of the form
        "{source component}->{component 1}->{component N}->{sink component}". Each call to
        next() on the returned generator will yield a list of Vertex instances for a
        particular path through the instances of that component path.
    """

    comp_instance_lists: Dict[str, List[Vertex]] = get_component_instance_lists(
        graph_client, topology_id, topology_ref
    )

    path_generators: Dict[str, Iterator[List[Vertex]]] = {}

    for comp_path in get_component_paths(graph_client, topology_id, topology_ref):

        LOG.debug(
            "Creating generator for instance paths for component path: %s",
            str(comp_path),
        )

        comp_path_instance_lists: List[List[Vertex]] = [
            comp_instance_lists[comp] for comp in comp_path
        ]

        path_generators["->".join(comp_path)] = path_combinations(
            comp_path_instance_lists
        )

    return path_generators
