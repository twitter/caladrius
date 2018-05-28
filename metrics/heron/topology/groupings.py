# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for analysing and summarising the groupings
within Heron topologies. """

import logging

from typing import Set, Tuple, Dict, Any, DefaultDict
from collections import defaultdict

from caladrius.common.heron import tracker

LOG: logging.Logger = logging.getLogger(__name__)


def summary(tracker_url: str, topology_id: str, cluster: str,
            environ: str) -> Dict[str, int]:
    """ Gets a summary of the numbers of each stream grouping type in the
    specified topology.

    Arguments:
        tracker_url (str):  The URL for the Heron Tracker API
        topology_id (str):  The topology ID string
        cluster (str):  The name of the cluster the topology is running on
        environ (str):  The environment the topology is running in

    Returns:
        A dictionary mapping from stream grouping name to the count for the
        number of these type of stream groupings in the topology. Also includes
        counts for stream combination, e.g. SHUFFLE->FIELDS : 2 implies that
        there are 2 cases where the source component of a fields grouped stream
        has an incoming shuffle grouped stream.
    """
    lplan: Dict[str, Any] = tracker.get_logical_plan(tracker_url, cluster,
                                                     environ, topology_id)

    stream_set: Set[Tuple[str, str, str]] = set()

    for bolt_details in lplan["bolts"].values():
        for input_stream in bolt_details["inputs"]:
            stream_set.add((input_stream["stream_name"],
                            input_stream["component_name"],
                            input_stream["grouping"]))

    grouping_counts: DefaultDict[str, int] = defaultdict(int)
    for _, source_component, grouping in stream_set:
        grouping_counts[grouping] += 1

        # Now look at the inputs in to this source component and count the
        # types of input grouping
        if source_component in lplan["bolts"]:
            for in_stream in lplan["bolts"][source_component]["inputs"]:
                in_grouping: str = in_stream["grouping"]
                grouping_counts[in_grouping + "->" + grouping] += 1

    return dict(grouping_counts)


def has_fields_fields(tracker_url: str, topology_id: str, cluster: str,
                      environ: str) -> bool:
    """ Performs a check to see if the specified topology has components
    connected via a fields grouping where the source component of that
    connection also receives a fields grouping.

    Arguments:
        tracker_url (str):  The URL for the Heron Tracker API
        topology_id (str):  The topology ID string
        cluster (str):  The name of the cluster the topology is running on
        environ (str):  The environment the topology is running in

    Returns:
        A boolean indicating if there is a field grouping source that also
        receives a fields grouping.
    """

    grouping_summary: Dict[str, int] = summary(tracker_url, topology_id,
                                               cluster, environ)

    if "FIELDS->FIELDS" in grouping_summary:
        return True

    return False
