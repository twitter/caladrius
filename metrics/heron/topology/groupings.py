""" This module contains methods for analysing and summarising the groupings
within Heron topologies. """
import logging

from typing import Set, Tuple, Dict, Any, DefaultDict
from collections import defaultdict

import pandas as pd

from requests import HTTPError

from common.heron import tracker

LOG: logging.Logger = logging.getLogger(__name__)

def summary(tracker_url: str, topology_id: str, cluster: str,
            environ: str) -> Dict[str, int]:

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

def summerise(tracker_url: str) -> pd.DataFrame:

    topologies: pd.DataFrame = tracker.get_topologies(tracker_url)

    output: pd.DataFrame = None

    for (cluster, environ), data in topologies.groupby(["cluster", "environ"]):
        for topology in data.topology:

            try:
                grouping_summary: Dict[str, int] = \
                    summary(tracker_url, topology, cluster, environ)
            except HTTPError:
                LOG.warning("Unable to fetch grouping summary for topology: "
                            "%s, cluster: %s, environ: %s", topology, cluster,
                            environ)
            else:
                grouping_summary["topology"] = topology
                grouping_df: pd.DataFrame = pd.DataFrame([grouping_summary])

                if output is None:
                    output = grouping_df
                else:
                    output = output.append(grouping_df)

    output = output.merge(topologies, on="topology")

    return output

def has_fields_fields(tracker_url: str, topology_id: str, cluster: str,
                      environ: str) -> bool:

    grouping_summary: Dict[str, int] = summary(tracker_url, topology_id,
                                               cluster, environ)

    if "FIELDS->FIELDS" in grouping_summary:
        return True

    return False
