""" Script for displaying statistics for all topologies registered with a
Heron Tracker instance."""
import sys

import pandas as pd

from caladrius import logs
from caladrius.metrics.heron.topology import groupings

if __name__ == "__main__":

    logs.setup()

    tracker_url: str = sys.argv[1]

    GROUPING_SUMMARY: pd.DataFrame = groupings.summerise(tracker_url)

    print("-------------------")
    print("Heron Tracker Stats")
    print("-------------------")

    TOTAL_TOPOS: int = GROUPING_SUMMARY.topology.count()

    print(f"\nTotal topologies: {TOTAL_TOPOS}")

    print("\n-------------------")
    print("Stream grouping stats")

    print("\nOverall stream grouping summary (%):")
    OVERALL: pd.Series = \
        (GROUPING_SUMMARY.drop(["topology", "cluster", "environ", "user"],
                               axis=1).count() / TOTAL_TOPOS * 100)

    print(OVERALL.to_string())

    print("\nEnvironment stream grouping summary (%):")
    ENVIRON: pd.Series = \
        (GROUPING_SUMMARY.groupby(["environ"]).count() / TOTAL_TOPOS * 100)

    print(ENVIRON.to_string())
