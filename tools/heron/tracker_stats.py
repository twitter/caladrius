""" Script for displaying statistics for all topologies registered with a
Heron Tracker instance."""
import sys
import logging

from typing import List, Dict, Union, Any

import requests

import pandas as pd

from caladrius import logs
from caladrius.common.heron import tracker
from caladrius.metrics.heron.topology import groupings

LOG: logging.Logger = logging.getLogger(__name__)


def add_conf_info(tracker_url: str,
                  topologies: pd.DataFrame = None) -> pd.DataFrame:
    """ Adds information from the physical plan config dictionary to the
    topologies summary DataFrame.

    Arguments:
        tracker_url (str):  The URL for the Heron Tracker API
        topologies (pd.DataFrame):  The topologies summary from the heron
                                    tracker can be supplied, if not it will
                                    fetched fresh from the Tracker API.

    Returns:
        pandas.DataFrame:   The topologies summary DataFrame with config
        information added. This will return a new DataFrame and will not modify
        the supplied DataFrame
    """
    if topologies is None:
        topologies = tracker.get_topologies(tracker_url)

    output: List[Dict[str, Union[str, float]]] = []

    for (cluster, environ, user), data in topologies.groupby(["cluster",
                                                              "environ",
                                                              "user"]):
        for topology_id in data.topology:

            try:
                pplan: Dict[str, Any] = tracker.get_physical_plan(
                    tracker_url, cluster, environ, topology_id)
            except requests.HTTPError:
                # If we cannot fetch the plan, skip this topology
                continue

            config: Dict[str, str] = pplan["config"]
            row: Dict[str, Union[str, float]] = {}
            row["topology"] = topology_id
            row["cluster"] = cluster
            row["environ"] = environ
            row["user"] = user
            for key, value in config.items():

                # Some of the custom config values are large dictionaries or
                # list so we will skip them
                if isinstance(value, (dict, list)):
                    continue

                new_key: str = "_".join(key.split(".")[1:])

                try:
                    new_value: Union[str, float] = float(value)
                except ValueError:
                    new_value = value
                except TypeError:
                    LOG.error("Value of key: %s was not a string or number it",
                              " was a %s", key, str(type(value)))

                row[new_key] = new_value
            output.append(row)

    return pd.DataFrame(output)


if __name__ == "__main__":

    logs.setup()

    TRACKER_URL: str = sys.argv[1]

    TOPOLOGIES: pd.DataFrame = tracker.get_topologies(TRACKER_URL)

    # Overall topology counts
    TOTAL_TOPOS: int = TOPOLOGIES.topology.count()
    TOPOS_BY_CLUSTER: pd.DataFrame = pd.DataFrame(
        TOPOLOGIES.groupby("cluster").topology.count())
    TOPOS_BY_CLUSTER["percentage"] = ((TOPOS_BY_CLUSTER.topology /
                                       TOTAL_TOPOS) * 100)
    TOPOS_BY_CLUSTER = TOPOS_BY_CLUSTER.reset_index()
    TOPOS_BY_CLUSTER.rename(index=str,
                            columns={"topology": "topo_cluster_count"},
                            inplace=True)

    TOPOS_BY_ENV: pd.DataFrame = pd.DataFrame(
        TOPOLOGIES.groupby("environ").topology.count())
    TOPOS_BY_ENV["percentage"] = ((TOPOS_BY_ENV.topology /
                                   TOTAL_TOPOS) * 100)
    TOPOS_BY_ENV = TOPOS_BY_ENV.reset_index()

    # Add config options
    TOPO_CONF: pd.DataFrame = add_conf_info(TRACKER_URL, TOPOLOGIES)

    MG_TOTAL_TOPOS: int = TOPO_CONF.reliability_mode.count()

    # Message Guarantee stats

    # Overall
    MG_OVERALL: pd.Series = \
        (TOPO_CONF.groupby("reliability_mode").topology.count())
    MG_OVERALL = MG_OVERALL.reset_index()
    MG_OVERALL.rename(index=str, columns={"topology": "mg_overall_count"},
                      inplace=True)
    MG_OVERALL["percentage"] = (MG_OVERALL.mg_overall_count /
                                MG_TOTAL_TOPOS * 100)

    # By Cluster
    MG_BY_CLUSTER: pd.DataFrame = pd.DataFrame(
        TOPO_CONF.groupby(["cluster", "reliability_mode"]).topology.count())
    MG_BY_CLUSTER = MG_BY_CLUSTER.reset_index()
    MG_BY_CLUSTER.rename(index=str, columns={"topology": "mg_cluster_count"},
                         inplace=True)
    MG_BY_CLUSTER = MG_BY_CLUSTER.merge(
        (MG_BY_CLUSTER.groupby("cluster").mg_cluster_count.sum()
         .reset_index().rename(
            index=str, columns={"mg_cluster_count": "mg_cluster_total"})),
        on="cluster")

    MG_BY_CLUSTER["overall_percentage"] = ((MG_BY_CLUSTER.mg_cluster_count /
                                            MG_TOTAL_TOPOS) * 100)
    MG_BY_CLUSTER["cluster_percentage"] = ((MG_BY_CLUSTER.mg_cluster_count /
                                            MG_BY_CLUSTER.mg_cluster_total)
                                           * 100)
    # By Environment
    MG_BY_ENV: pd.DataFrame = pd.DataFrame(
        TOPO_CONF.groupby(["environ", "reliability_mode"]).topology.count())
    MG_BY_ENV = MG_BY_ENV.reset_index()
    MG_BY_ENV.rename(index=str, columns={"topology": "mg_environ_count"},
                     inplace=True)
    MG_BY_ENV = MG_BY_ENV.merge(
        (MG_BY_ENV.groupby("environ").mg_environ_count.sum()
         .reset_index().rename(
            index=str, columns={"mg_environ_count": "mg_environ_total"})),
        on="environ")

    MG_BY_ENV["overall_percentage"] = ((MG_BY_ENV.mg_environ_count /
                                        MG_TOTAL_TOPOS) * 100)
    MG_BY_ENV["cluster_percentage"] = ((MG_BY_ENV.mg_environ_count /
                                        MG_BY_ENV.mg_environ_total)
                                       * 100)

    # Grouping stats
    GROUPING_SUMMARY: pd.DataFrame = groupings.summarise(TRACKER_URL,
                                                         TOPOLOGIES)

    JUST_GROUPINGS: pd.DataFrame = \
        (GROUPING_SUMMARY.drop(["topology", "cluster", "environ", "user"],
                               axis=1))

    GROUPING_OVERALL: pd.Series = \
        (JUST_GROUPINGS.count() / TOTAL_TOPOS * 100)

    GROUPING_ENVIRON: pd.Series = \
        (GROUPING_SUMMARY.groupby(["environ"]).count() / TOTAL_TOPOS * 100)

    SINGLE_GROUPING: List[Dict[str, Union[int, float]]] = []
    for grouping in [g for g in list(JUST_GROUPINGS.columns) if "->" not in g]:
        grouping_only_count: int = \
            (JUST_GROUPINGS[JUST_GROUPINGS[grouping].notna() &
                            (JUST_GROUPINGS.isna().sum(axis=1) ==
                             len(JUST_GROUPINGS.columns) - 1)]
             [grouping].count())
        SINGLE_GROUPING.append({"Grouping": grouping,
                                "Frequency": grouping_only_count,
                                "% of Total":
                                (grouping_only_count / TOTAL_TOPOS * 100)})

    print("-------------------")
    print("Heron Tracker Stats")
    print("-------------------")

    print(f"\nTotal topologies: {TOTAL_TOPOS}")
    print("\nTotal topologies by cluster:\n")
    print(TOPOS_BY_CLUSTER.to_string())
    print("\nTotal topologies by environment:\n")
    print(TOPOS_BY_ENV.to_string())

    print("\n-------------------")
    print("Stream manager stats:\n")

    print(TOPO_CONF.stmgrs.describe().to_string())

    print("\n-------------------")
    print("Message guarantee stats:\n")

    print("\nPercentage of topologies with each message guarantee type - "
          "Overall\n")
    print(MG_OVERALL.to_string())

    print("\nPercentage of topologies with each message guarantee type - "
          "Cluster\n")
    print(MG_BY_CLUSTER.to_string())

    print("\nPercentage of topologies with each message guarantee type - "
          "Environment\n")
    print(MG_BY_ENV.to_string())

    print("\n-------------------")
    print("Stream grouping stats:\n")

    print("\nPercentage of topologies with each grouping - Overall:\n")
    print(GROUPING_OVERALL.to_string())

    print("\nPercentage of topologies with each grouping - Per Environment:\n")
    print(GROUPING_ENVIRON.to_string())

    print("\nTopologies with only a single grouping type:\n")
    print(pd.DataFrame(SINGLE_GROUPING).to_string())
