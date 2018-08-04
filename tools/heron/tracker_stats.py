# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" Script for displaying statistics for all topologies registered with a
Heron Tracker instance."""

import os
import sys
import logging
import argparse
import pickle

import datetime as dt

from typing import List, Dict, Union, Any

import requests

import pandas as pd

from caladrius import logs
from caladrius.common.heron import tracker
from caladrius.metrics.heron.topology import groupings

LOG: logging.Logger = logging.getLogger("caladrius.tools.heron.tracker_stats")


def summarise_groupings(tracker_url: str,
                        topologies: pd.DataFrame = None) -> pd.DataFrame:
    """ Summarises the stream grouping counts of all topologies registered with
    the supplied Tracker instance.

    Arguments:
        tracker_url (str):  The URL for the Heron Tracker API
        topologies (pd.DataFrame):  The topologies summary from the heron
                                    tracker can be supplied, if not it will
                                    fetched fresh from the trackerAPI.

    Returns:
        A DataFrame with columns for:
        topology: The topology ID
        cluster: The cluster the topology is running on
        environ: The environment the topology is running in
        user: The user that uploaded the topology
        A column for each type of stream grouping as well as combinations of
        stream grouping (incoming grouping)->(outgoing grouping) and their
        associate frequency count for each topology.
    """
    if topologies is None:
        topologies = tracker.get_topologies(tracker_url)
    output: pd.DataFrame = None

    for (cluster, environ), data in topologies.groupby(["cluster", "environ"]):
        for topology in data.topology:

            try:
                grouping_summary: Dict[str, int] = \
                    groupings.summary(tracker_url, topology, cluster, environ)
            except requests.HTTPError:
                LOG.warning("Unable to fetch grouping summary for topology: "
                            "%s, cluster: %s, environ: %s", topology, cluster,
                            environ)
            else:
                grouping_summary["topology"] = topology
                grouping_summary["cluster"] = cluster
                grouping_summary["environ"] = environ
                grouping_df: pd.DataFrame = pd.DataFrame([grouping_summary])

                if output is None:
                    output = grouping_df
                else:
                    output = output.append(grouping_df)

    output = output.merge(topologies, on=["topology","environ","cluster"])
    return output


def add_pplan_info(tracker_url: str,
                   topologies: pd.DataFrame = None) -> pd.DataFrame:
    """ Combines information from the topology summary DataFrame with
    information from the physical plan of each topology.

    Arguments:
        tracker_url (str):  The URL for the Heron Tracker API
        topologies (pd.DataFrame):  The topologies summary from the heron
                                    tracker can be supplied, if not it will
                                    fetched fresh from the Tracker API.

    Returns:
        pandas.DataFrame:   The topologies summary DataFrame with physical plan
        information added. This will return a new DataFrame and will not modify
        the supplied DataFrame
    """
    if topologies is None:
        topologies = tracker.get_topologies(tracker_url)

    output: List[Dict[str, Union[str, float, List[int]]]] = []

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

            # Add information from the configuration dictionary
            config: Dict[str, str] = pplan["config"]
            row: Dict[str, Union[str, float, List[int]]] = {}
            row["topology"] = topology_id
            row["cluster"] = cluster
            row["environ"] = environ
            row["user"] = user
            for key, value in config.items():

                # Some of the custom config values are large dictionaries or
                # lists so we will skip them
                if isinstance(value, (dict, list)):
                    continue

                # Replace "." with "_" in the key name so we can use namespace
                # calls on the DataFrame
                new_key: str = "_".join(key.split(".")[1:])

                # Try to convert any values that numeric so we can do summary
                # stats
                try:
                    new_value: Union[str, float] = float(value)
                except ValueError:
                    new_value = value
                except TypeError:
                    LOG.error("Value of key: %s was not a string or number it"
                              " was a %s", key, str(type(value)))

                row[new_key] = new_value

            # Add instances stats for this topology
            row["total_instances"] = len(pplan["instances"])
            row["instances_per_container_dist"] = \
                [len(pplan["stmgrs"][stmgr]["instance_ids"])
                 for stmgr in pplan["stmgrs"]]
            row["total_bolts"] = len(pplan["bolts"])
            row["total_spouts"] = len(pplan["spouts"])
            row["total_components"] = len(pplan["bolts"]) + len(pplan["spouts"])

            output.append(row)

    return pd.DataFrame(output)


def add_logical_plan_info(tracker_url: str,
                   topologies: pd.DataFrame = None) -> pd.DataFrame:
    """ Combines information from the topology summary DataFrame with
    information from the logical plan of each topology.

    Arguments:
        tracker_url (str):  The URL for the Heron Tracker API
        topologies (pd.DataFrame):  The topologies summary from the heron
                                    tracker can be supplied, if not it will
                                    fetched fresh from the Tracker API.

    Returns:
        pandas.DataFrame:   The topologies summary DataFrame with logical plan
        information added. This will return a new DataFrame and will not modify
        the supplied DataFrame
    """
    if topologies is None:
        topologies = tracker.get_topologies(tracker_url)
    output: List[Dict] = []

    for (cluster, environ, user), data in topologies.groupby(["cluster",
                                                              "environ",
                                                              "user"]):
        for topology_id in data.topology:

            try:
                logical_plan: Dict[str, Any] = tracker.get_logical_plan(
                    tracker_url, cluster, environ, topology_id)
            except requests.HTTPError:
                # If we cannot fetch the plan, skip this topology
                continue

            # there are two possible kinds of spouts:
            spout_single_output = 0
            spout_multiple_output = 0

            # there are six possible kinds of bolts:
            # two of which are sinks, and four are intermediate bolts
            # sink types
            bolt_single_in_zero_out = 0
            bolt_multiple_in_zero_out = 0

            # intermediate types:
            bolt_single_in_single_out = 0
            bolt_multiple_in_single_out = 0
            bolt_single_in_multiple_out = 0
            bolt_multiple_in_multiple_out = 0

            row: Dict = {}
            LOG.info("Topology ID: %s",  topology_id)
            for key in logical_plan["spouts"].keys():
                num_outputs = len(logical_plan["spouts"][key]["outputs"])
                if num_outputs == 1:
                    spout_single_output = spout_single_output + 1
                else:
                    spout_multiple_output = spout_multiple_output + 1
            for key in logical_plan["bolts"].keys():
                outputs = len(logical_plan["bolts"][key]["outputs"])
                inputs = len(logical_plan["bolts"][key]["inputs"])

                # sinks
                if outputs == 0:
                    if inputs == 1:
                        bolt_single_in_zero_out = bolt_single_in_zero_out + 1
                    elif inputs > 1:
                        bolt_multiple_in_zero_out = bolt_multiple_in_zero_out + 1
                elif outputs == 1:
                    if inputs == 1:
                        bolt_single_in_single_out = bolt_single_in_single_out + 1
                    elif inputs > 1:
                        bolt_multiple_in_single_out = bolt_multiple_in_single_out + 1
                elif outputs > 1:
                    if inputs == 1:
                        bolt_single_in_multiple_out = bolt_single_in_multiple_out + 1
                    elif inputs > 1:
                        bolt_multiple_in_multiple_out = bolt_multiple_in_multiple_out + 1

            row["topology"] = topology_id

            row["spout_single_output"] = spout_single_output
            row["spout_multiple_output"] = spout_multiple_output

            row["bolt_single_in_zero_out"] = bolt_single_in_zero_out
            row["bolt_multiple_in_zero_out"] = bolt_multiple_in_zero_out

            row["bolt_single_in_single_out"] = bolt_single_in_single_out
            row["bolt_multiple_in_single_out"] = bolt_multiple_in_single_out
            row["bolt_single_in_multiple_out"] = bolt_single_in_multiple_out
            row["bolt_multiple_in_multiple_out"] = bolt_multiple_in_multiple_out
            output.append(row)

    return pd.DataFrame(output)


def _get_mg_summary(topo_pplan: pd.DataFrame, groupby_term: str):

    mg_summary: pd.DataFrame = pd.DataFrame(
        topo_pplan.groupby([groupby_term,
                            "reliability_mode"]).topology.count())
    mg_summary = mg_summary.reset_index()
    mg_summary.rename(index=str,
                      columns={"topology": f"mg_{groupby_term}_count"},
                      inplace=True)
    mg_summary = mg_summary.merge(
        (mg_summary.groupby(groupby_term)[f"mg_{groupby_term}_count"].sum()
         .reset_index().rename(
             index=str,
             columns={f"mg_{groupby_term}_count":
                      f"mg_{groupby_term}_total"})),
        on=groupby_term)

    mg_summary["overall_percentage"] = \
        (mg_summary[f"mg_{groupby_term}_count"] /
         topo_pplan.reliability_mode.count()
         * 100)
    mg_summary[f"{groupby_term}_percentage"] = \
        (mg_summary[f"mg_{groupby_term}_count"] /
         mg_summary[f"mg_{groupby_term}_total"] * 100)

    return mg_summary


def _create_parser() -> argparse.ArgumentParser:

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=("This program provides various statistics about the "
                     "topologies running on a Heron Tracker instance"))

    parser.add_argument("-t", "--tracker", required=False, type=str,
                        help=("The URL to the Heron Tracker API"))

    parser.add_argument("-r", "--reload", required=False, action="store_true",
                        help=("If supplied then fresh information will be "
                              "pulled from the Heron Tracker API. Otherwise "
                              "cached data will be used."))

    parser.add_argument("-cd", "--cache_dir", required=False, type=str,
                        default="/tmp/caladrius/heron/tracker/stats",
                        help=("The temporary directory to store Tracker "
                              "information."))

    parser.add_argument("-o", "--output", required=False, type=str,
                        help=("Output file path for the statistic summary to "
                              "be printed to. If not supplied out will be "
                              "sent to standard out."))

    parser.add_argument("-q", "--quiet", required=False, action="store_true",
                        help=("Optional flag indicating if console log output "
                              "should be suppressed"))

    parser.add_argument("--debug", required=False, action="store_true",
                        help=("Optional flag indicating if debug level "
                              "information should be displayed"))

    return parser


def _check_tracker(tracker_str: str) -> str:

    if "http://" in tracker_str:
        return tracker_str

    return "http://" + tracker_str


if __name__ == "__main__":

    ARGS: argparse.Namespace = _create_parser().parse_args()

    if not ARGS.quiet:
        logs.setup(debug=ARGS.debug)

    NO_CACHE_DIR: bool = False
    if not os.path.exists(ARGS.cache_dir):
        LOG.info("No cached tracker information present")
        NO_CACHE_DIR = True
        os.makedirs(ARGS.cache_dir)
        LOG.info("Created cache directory at: %s", ARGS.cache_dir)
    elif len(os.listdir(ARGS.cache_dir)) < 1:
        LOG.info("No cached tracker information present")
        # If the directory exists but there is nothing in it then we need to
        # load the tracker data
        NO_CACHE_DIR = True

    CREATE_TIME_FILE: str = os.path.join(ARGS.cache_dir, "created.pkl")
    TOPO_FILE: str = os.path.join(ARGS.cache_dir, "topo.pkl")
    TOPO_PPLAN_FILE: str = os.path.join(ARGS.cache_dir, "topo_pplan.pkl")
    TOPO_LPLAN_FILE: str = os.path.join(ARGS.cache_dir, "topo_lplan.pkl")
    GROUPING_SUMMARY_FILE: str = os.path.join(ARGS.cache_dir,
                                              "grouping_summary.pkl")

    if ARGS.reload or NO_CACHE_DIR:

        if not ARGS.tracker:
            err_msg: str = ("In order to load information from the Tracker API"
                            " the tracker URL must be supplied via the "
                            "-t / --tracker argument")
            if ARGS.quiet:
                print(err_msg, file=sys.stderr)
            else:
                LOG.error(err_msg)
            # Exit with error code 2 (usually means cli syntax error)
            sys.exit(2)

        TRACKER_URL: str = _check_tracker(ARGS.tracker)

        LOG.info("Fetching new data from the Heron Tracker API at %s",
                 TRACKER_URL)

        # Get the list of topologies registered with the heron tracker
        TOPOLOGIES: pd.DataFrame = tracker.get_topologies(TRACKER_URL)
        LOG.info("Caching topology data at %s", TOPO_FILE)
        TOPOLOGIES.to_pickle(TOPO_FILE)

        # Add physical plan options
        TOPO_PPLAN: pd.DataFrame = add_pplan_info(TRACKER_URL, TOPOLOGIES)
        LOG.info("Caching topology physical plan data at %s",
                 TOPO_PPLAN_FILE)
        TOPO_PPLAN.to_pickle(TOPO_PPLAN_FILE)

        # Add logical plan information
        TOPO_LPLAN: pd.DataFrame = add_logical_plan_info(TRACKER_URL, TOPOLOGIES)
        LOG.info("Caching topology logical plan data at %s",
                 TOPO_LPLAN_FILE)
        TOPO_LPLAN.to_pickle(TOPO_LPLAN_FILE)
        # Get the stream grouping summary
        GROUPING_SUMMARY: pd.DataFrame = summarise_groupings(TRACKER_URL,
                                                             TOPOLOGIES)
        LOG.info("Caching stream grouping summary data at %s",
                 GROUPING_SUMMARY_FILE)
        GROUPING_SUMMARY.to_pickle(GROUPING_SUMMARY_FILE)

        # Save a time stamp so we know how old the data is
        NOW: dt.datetime = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

        with open(CREATE_TIME_FILE, 'wb') as time_file:
            pickle.dump(NOW, time_file)

    else:

        with open(CREATE_TIME_FILE, 'rb') as time_file:
            timestamp: dt.datetime = pickle.load(time_file)

        NOW = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        duration: dt.timedelta = NOW - timestamp

        LOG.info("Loading data that is %s hours old",
                 str(round(duration.total_seconds() / 3600, 2)))

        LOG.info("Loading topology data from %s", TOPO_FILE)
        TOPOLOGIES = pd.read_pickle(TOPO_FILE)

        LOG.info("Loading topology physical plan data from %s",
                 TOPO_PPLAN_FILE)
        TOPO_PPLAN = pd.read_pickle(TOPO_PPLAN_FILE)

        LOG.info("Loading topology logical plan data from %s",
                 TOPO_PPLAN_FILE)
        TOPO_LPLAN = pd.read_pickle(TOPO_LPLAN_FILE)

        LOG.info("Loading stream grouping summary data from %s",
                 GROUPING_SUMMARY_FILE)
        GROUPING_SUMMARY = pd.read_pickle(GROUPING_SUMMARY_FILE)

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

    MG_TOTAL_TOPOS: int = TOPO_PPLAN.reliability_mode.count()

    # Message Guarantee stats

    # Overall
    MG_OVERALL: pd.Series = \
        (TOPO_PPLAN.groupby("reliability_mode").topology.count())
    MG_OVERALL = MG_OVERALL.reset_index()
    MG_OVERALL.rename(index=str, columns={"topology": "mg_overall_count"},
                      inplace=True)
    MG_OVERALL["percentage"] = (MG_OVERALL.mg_overall_count /
                                MG_TOTAL_TOPOS * 100)

    # By Cluster
    MG_BY_CLUSTER: pd.DataFrame = _get_mg_summary(TOPO_PPLAN, "cluster")

    # By Environment
    MG_BY_ENV: pd.DataFrame = _get_mg_summary(TOPO_PPLAN, "environ")

    # Topology structure stats
    LINEAR_TOPOS = TOPO_LPLAN[(TOPO_LPLAN['spout_multiple_output'] == 0) &
                              (TOPO_LPLAN['bolt_multiple_in_zero_out'] == 0) &
                              (TOPO_LPLAN['bolt_multiple_in_multiple_out'] == 0) &
                              (TOPO_LPLAN['bolt_multiple_in_single_out'] == 0) &
                              (TOPO_LPLAN['bolt_single_in_multiple_out'] == 0)].reset_index().shape[0] \
                   / TOTAL_TOPOS * 100

    # We look for the most common type of operator
    SPOUT_MULTIPLE_OUTPUTS = \
        TOPO_LPLAN[(TOPO_LPLAN['spout_multiple_output'] > 0)].reset_index().shape[0] / TOTAL_TOPOS * 100
    SINK_MULTIPLE_INPUTS = \
        TOPO_LPLAN[(TOPO_LPLAN['bolt_multiple_in_zero_out'] > 0)].reset_index().shape[0] / TOTAL_TOPOS * 100
    BOLT_MULTIPLE_IN_OUT = \
        TOPO_LPLAN[(TOPO_LPLAN['bolt_multiple_in_multiple_out'] > 0)].reset_index().shape[0] / TOTAL_TOPOS * 100
    BOLT_MULTIPLE_IN_SINGLE_OUT = \
        TOPO_LPLAN[(TOPO_LPLAN['bolt_multiple_in_single_out'] > 0)].reset_index().shape[0] / TOTAL_TOPOS * 100
    BOLT_SINGLE_IN_MULTIPLE_OUT = \
        TOPO_LPLAN[(TOPO_LPLAN['bolt_single_in_multiple_out'] > 0)].reset_index().shape[0] / TOTAL_TOPOS * 100
    # We look for topologies that have both of the most common non-linear operator types:
    COMMON = TOPO_LPLAN[(TOPO_LPLAN['bolt_multiple_in_single_out'] > 0) &
                        (TOPO_LPLAN['bolt_multiple_in_zero_out'] > 0)].reset_index()
    # Grouping stats
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

    PERCENTILES: List[float] = [.10, .25, .5, .75, .95, .99]

    if ARGS.output:
        LOG.info("Saving output to file: %s", ARGS.output)
        OUT_FILE = open(ARGS.output, "w")
    else:
        LOG.info("Sending output to standard out")
        OUT_FILE = sys.stdout

    print("-------------------", file=OUT_FILE)
    print("Heron Tracker Stats", file=OUT_FILE)
    print("-------------------", file=OUT_FILE)

    print(f"\nTotal topologies: {TOTAL_TOPOS}", file=OUT_FILE)
    print("\nTotal topologies by cluster:\n", file=OUT_FILE)
    print(TOPOS_BY_CLUSTER.to_string(index=False), file=OUT_FILE)
    print("\nTotal topologies by environment:\n", file=OUT_FILE)
    print(TOPOS_BY_ENV.to_string(index=False), file=OUT_FILE)

    print("\n-------------------", file=OUT_FILE)
    print("Container stats:\n", file=OUT_FILE)

    print(TOPO_PPLAN.stmgrs.describe(percentiles=PERCENTILES).to_string(),
          file=OUT_FILE)

    print("\nTop 10 Largest topologies by container count:\n", file=OUT_FILE)
    print(TOPO_PPLAN.sort_values(by="stmgrs", ascending=False)
          [["topology", "cluster", "environ", "user", "total_instances",
            "stmgrs"]]
          .head(10).to_string(index=False), file=OUT_FILE)

    print("\n-------------------", file=OUT_FILE)
    print("Instance stats:\n", file=OUT_FILE)

    print("\nStatistics for total number of instances per topology:\n",
          file=OUT_FILE)
    print(TOPO_PPLAN.total_instances.describe(
        percentiles=PERCENTILES).to_string(), file=OUT_FILE)

    print("\nTop 10 Largest topologies by instance count:\n", file=OUT_FILE)
    print(TOPO_PPLAN.sort_values(by="total_instances", ascending=False)
          [["topology", "cluster", "environ", "user", "total_instances",
            "stmgrs"]].head(10).to_string(index=False), file=OUT_FILE)

    print("\nStatistics for instances per container:\n", file=OUT_FILE)
    output: List[int] = []
    for index, dist in TOPO_PPLAN.instances_per_container_dist.iteritems():
        output.extend(dist)
    print(pd.Series(output).describe(percentiles=PERCENTILES).to_string(),
          file=OUT_FILE)

    print("\n-------------------", file=OUT_FILE)
    print("Component stats:\n", file=OUT_FILE)

    print("\nStatistics for total number of components per topology:\n",
          file=OUT_FILE)
    print(TOPO_PPLAN.total_components.describe(
        percentiles=PERCENTILES).to_string(), file=OUT_FILE)

    print("\nTop 20 Largest topologies by component count:\n", file=OUT_FILE)
    print(TOPO_PPLAN.sort_values(by="total_components", ascending=False)
          [["topology", "cluster", "environ", "user", "total_components",
            "stmgrs", "total_spouts", "total_bolts", "total_instances"]]
          .head(20).to_string(index=False), file=OUT_FILE)

    print("\n-------------------", file=OUT_FILE)
    print("Topology structure stats:\n", file=OUT_FILE)
    print("\nPercentage of topologies with linear structure: ",
          LINEAR_TOPOS, file=OUT_FILE)
    print("\nPercentage of topologies with spouts with multiple outputs: ",
          SPOUT_MULTIPLE_OUTPUTS, file=OUT_FILE)
    print("\nPercentage of topologies with sinks with multiple inputs: ",
          SINK_MULTIPLE_INPUTS, file=OUT_FILE)
    print("\nPercentage of topologies with bolts with multiple inputs"
          " and outputs: ",
          BOLT_MULTIPLE_IN_OUT, file=OUT_FILE)
    print("\nPercentage of topologies with bolts with multiple inputs"
          " and a single output: ",
          BOLT_MULTIPLE_IN_SINGLE_OUT, file=OUT_FILE)
    print("\nPercentage of topologies with bolts with single input "
          "and multiple outputs: ",
          BOLT_SINGLE_IN_MULTIPLE_OUT, file=OUT_FILE)
    print("\nPercentage of topologies with both intermediate bolts with"
          " multiple inputs and sinks with multiple inputs: ",
          COMMON.shape[0]/TOTAL_TOPOS * 100, file=OUT_FILE)

    print("\n-------------------", file=OUT_FILE)
    print("Message guarantee stats:\n", file=OUT_FILE)

    print("\nTopologies with each message guarantee type - Overall\n",
          file=OUT_FILE)
    print(MG_OVERALL.to_string(), file=OUT_FILE)

    print("\nTopologies with each message guarantee type by Cluster\n",
          file=OUT_FILE)
    print(MG_BY_CLUSTER.set_index(["cluster",
                                   "reliability_mode"]).to_string(),
          file=OUT_FILE)

    print("\nTopologies with each message guarantee type by Environment\n",
          file=OUT_FILE)
    print(MG_BY_ENV.set_index(["environ", "reliability_mode"]).to_string(),
          file=OUT_FILE)

    print("\n-------------------", file=OUT_FILE)
    print("Stream grouping stats:\n", file=OUT_FILE)

    print("\nPercentage of topologies with each grouping - Overall:\n",
          file=OUT_FILE)
    print(GROUPING_OVERALL.to_string(), file=OUT_FILE)

    print("\nPercentage of topologies with each grouping - Per Environment:\n",
          file=OUT_FILE)
    print(GROUPING_ENVIRON.drop(["topology", "cluster", "user"],
                                axis=1).to_string(), file=OUT_FILE)

    print("\nTopologies with only a single grouping type:\n", file=OUT_FILE)
    print(pd.DataFrame(SINGLE_GROUPING).to_string(index=False), file=OUT_FILE)
