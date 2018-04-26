""" Command line program for constructing physical graphs of topologies within
the caladrius graph database. """

import logging
import argparse
import sys

from caladrius.logs import set_up_logging
from caladrius.graph.builder.heron import builder
from caladrius.graph.client.gremlin.client import GremlinClient
from caladrius.common.heron import tracker
from caladrius.metrics.heron.cuckoo.client import HeronCuckooClient
from caladrius.common.time.timestamp import get_window_dt_from_now

LOG: logging.Logger = logging.getLogger(__name__)

#pylint: disable=invalid-name


# Setup the command line parser
parser = argparse.ArgumentParser(
    description=("Builds a physical graph representation of the specified"
                 " topology "))
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
parser.add_argument("-p", "--populate", required=False, action="store_true",
                    help=("Flag indicating if the physical graph should be "
                          "populated with metrics. This requires some custom "
                          "caladrius metric classes to be included in the "
                          "topology."))
parser.add_argument("-d", "--duration", type=int, required=False,
                    help=("The time in second from now backwards over which "
                          "metrics should be gathered."))
parser.add_argument("--debug", required=False, action="store_true",
                    help=("Optional flag indicating if debug logging output "
                          "should be shown"))
parser.add_argument("-q", "--quiet", required=False, action="store_true",
                    help=("Optional flag indicating if log output should be "
                          "suppressed."))
ARGS = parser.parse_args()

if not ARGS.quiet:
    set_up_logging(debug=ARGS.debug)

if ARGS.populate and not ARGS.duration:
    msg: str = ("Populate flag was supplied but duration argument "
                "(-d/--duration)was not. Please supply a metrics gathering "
                "window duration in integer seconds.")

    if ARGS.quiet:
        print(msg)
    else:
        LOG.error(msg)

    sys.exit(2)

# TODO: Move these to config file and load from there
CONFIG = {"heron.tracker.url" :
          "http://heron-tracker-new.prod.heron.service.smf1.twitter.com",
          "gremlin.server.url" : "localhost:8182",
          "cuckoo.database.url": 'https://cuckoo-prod-smf1.twitter.biz'}

logical_plan = tracker.get_logical_plan(CONFIG["heron.tracer.url"], ARGS.zone,
                                        ARGS.environment,
                                        ARGS.topology)

physical_plan = tracker.get_physical_plan(CONFIG["heron.tracer.url"],
                                          ARGS.zone,
                                          ARGS.environment,
                                          ARGS.topology)


graph_client = GremlinClient(CONFIG)

builder.create_physical_graph(graph_client,
                              ARGS.topology, ARGS.reference,
                              logical_plan, physical_plan)

if ARGS.populate and ARGS.duration:

    metrics_client = HeronCuckooClient(CONFIG, "Infra-Caladrius")

    start, end = get_window_dt_from_now(seconds=ARGS.duration)

    try:
        builder.populate_physical_graph(graph_client, metrics_client,
                                        ARGS.topology, ARGS.reference,
                                        start, end)
    except KeyError:
        msg: str = ("Caladrius metrics not present in metrics database. Cannot"
                    " continue with graph metrics population.")

        if ARGS.quiet:
            print(msg)
        else:
            LOG.error(msg)
