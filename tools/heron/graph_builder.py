# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" Command line program for constructing physical graphs of heron topologies
within the caladrius graph database. """

import logging
import argparse
import sys

import datetime as dt

from typing import Dict, cast, Any, Type

from caladrius import logs
from caladrius import loader
from caladrius.config.keys import ConfKeys
from caladrius.graph.builder.heron import builder
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.common.heron import tracker
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.common.timestamp import get_window_dt_from_now

LOG: logging.Logger = \
    logging.getLogger("caladrius.tools.heron.graph_builder")


def create_parser() -> argparse.ArgumentParser:
    """ Helper function for creating the command line arguments parser. """

    parser = argparse.ArgumentParser(
        description=("Builds a physical graph representation of the specified"
                     "heron topology "))
    parser.add_argument("-cfg", "--config", required=True,
                        help="Path to the configuration file containing the "
                             "graph and metrics database strings and the url "
                             "to the Heron Tracker.")
    parser.add_argument("-t", "--topology", required=True,
                        help="The topology identification string")
    parser.add_argument("-z", "--zone", required=True,
                        help="The zone the topology is run within")
    parser.add_argument("-e", "--environment", required=True,
                        help=("The environment the topology is run within. "
                              "eg TEST, PROD etc."))
    parser.add_argument("-r", "--reference", required=True,
                        help=("The topology reference to be applied to all "
                              "elements of the physical graph."))
    parser.add_argument("-p", "--populate", required=False,
                        action="store_true",
                        help=("Flag indicating if the physical graph should "
                              "be populated with metrics. This requires some "
                              "custom caladrius metric classes to be included "
                              "in the topology."))
    parser.add_argument("-d", "--duration", type=int, required=False,
                        help=("The time in second from now backwards over "
                              "which metrics should be gathered."))
    parser.add_argument("--debug", required=False, action="store_true",
                        help=("Optional flag indicating if debug logging "
                              "output should be shown"))
    parser.add_argument("-q", "--quiet", required=False, action="store_true",
                        help=("Optional flag indicating if log output should "
                              "be suppressed."))
    return parser


if __name__ == "__main__":

    ARGS: argparse.Namespace = create_parser().parse_args()

    if not ARGS.quiet:
        logs.setup(debug=ARGS.debug)

    if ARGS.populate and not ARGS.duration:
        MSG: str = ("Populate flag was supplied but duration argument "
                    "(-d/--duration)was not. Please supply a metrics "
                    "gathering window duration in integer seconds.")

        if ARGS.quiet:
            print(MSG)
        else:
            LOG.error(MSG)

        sys.exit(2)

    try:
        CONFIG: Dict[str, Any] = loader.load_config(ARGS.config)
    except FileNotFoundError:
        MSG2: str = f"The config file: {ARGS.config} was not found. Aborting"

        if ARGS.quiet:
            print(MSG2)
        else:
            LOG.error(MSG2)

        sys.exit(1)

    TIMER_START = dt.datetime.now()

    TRACKER_URL: str = cast(str, CONFIG[ConfKeys.HERON_TRACKER_URL.value])

    LPLAN: Dict[str, Any] = tracker.get_logical_plan(TRACKER_URL, ARGS.zone,
                                                     ARGS.environment,
                                                     ARGS.topology)

    PPLAN: Dict[str, Any] = tracker.get_physical_plan(TRACKER_URL,
                                                      ARGS.zone,
                                                      ARGS.environment,
                                                      ARGS.topology)

    GRAPH_CLIENT: GremlinClient = GremlinClient(CONFIG["graph.client.config"])

    builder.create_physical_graph(GRAPH_CLIENT,
                                  ARGS.topology, ARGS.reference,
                                  LPLAN, PPLAN)

    if ARGS.populate and ARGS.duration:

        METRIC_CLIENT_CLASS: Type = \
            loader.get_class(CONFIG["heron.metrics.client"])

        METRICS_CLIENT: HeronMetricsClient = METRIC_CLIENT_CLASS(
            CONFIG["heron.metrics.client.config"])

        START, END = get_window_dt_from_now(seconds=ARGS.duration)

        try:
            builder.populate_physical_graph(GRAPH_CLIENT, METRICS_CLIENT,
                                            ARGS.topology, ARGS.reference,
                                            START, END)
        except KeyError as kerr:
            err_msg: str = ("Caladrius metrics not present in metrics "
                            "database. Cannot continue with graph metrics "
                            "population.")

            if ARGS.quiet:
                print(err_msg)
            else:
                LOG.error(err_msg)

    LOG.info("Graph building completed in %d seconds",
             (dt.datetime.now() - TIMER_START).total_seconds())
