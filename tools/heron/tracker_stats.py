""" Script for displaying statistics for all topologies registered with a
Heron Tracker instance."""
import sys

from typing import List, Dict, Union

import pandas as pd

from caladrius import logs
from caladrius.metrics.heron.topology import groupings

if __name__ == "__main__":

    logs.setup()

    tracker_url: str = sys.argv[1]

    GROUPING_SUMMARY: pd.DataFrame = groupings.summarise(tracker_url)

    print("-------------------")
    print("Heron Tracker Stats")
    print("-------------------")

    TOTAL_TOPOS: int = GROUPING_SUMMARY.topology.count()
    TOPOS_BY_CLUSTER = pd.DataFrame(
        GROUPING_SUMMARY.groupby("cluster").topology.count())
    TOPOS_BY_CLUSTER["percentage"] = ((TOPOS_BY_CLUSTER.topology /
                                       TOTAL_TOPOS) * 100)
    TOPOS_BY_ENV = pd.DataFrame(
        GROUPING_SUMMARY.groupby("environ").topology.count())
    TOPOS_BY_ENV["percentage"] = ((TOPOS_BY_ENV.topology /
                                   TOTAL_TOPOS) * 100)

    print(f"\nTotal topologies: {TOTAL_TOPOS}")
    print("\nTotal topologies by cluster:\n")
    print(TOPOS_BY_CLUSTER.to_string())
    print("\nTotal topologies by environment:\n")
    print(TOPOS_BY_ENV.to_string())

    print("\n-------------------")
    print("Stream grouping stats")

    JUST_GROUPINGS: pd.DataFrame = \
        (GROUPING_SUMMARY.drop(["topology", "cluster", "environ", "user"],
                               axis=1))

    print("\nPercentage of topologies with each grouping - Overall:\n")
    OVERALL: pd.Series = \
        (JUST_GROUPINGS.count() / TOTAL_TOPOS * 100)

    print(OVERALL.to_string())

    print("\nPercentage of topologies with each grouping - Per Environment:\n")
    ENVIRON: pd.Series = \
        (GROUPING_SUMMARY.groupby(["environ"]).count() / TOTAL_TOPOS * 100)

    print(ENVIRON.to_string())

    print("\nTopologies with only a single grouping type:\n")
    SINGLE_GROUPING: List[Dict[str, Union[int, float]]] = []
    for grouping in [g for g in list(JUST_GROUPINGS.columns) if "->" not in g]:
        grouping_only_count: int = \
            (JUST_GROUPINGS[JUST_GROUPINGS[grouping].notna() &
                            (JUST_GROUPINGS.isna().sum(axis=1) ==
                             len(JUST_GROUPINGS.columns) -1)]
             [grouping].count())
        SINGLE_GROUPING.append({"Grouping" : grouping,
                                "Frequency" : grouping_only_count,
                                "% of Total" :
                                (grouping_only_count/ TOTAL_TOPOS * 100)})

    print(pd.DataFrame(SINGLE_GROUPING).to_string())
