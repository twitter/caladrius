import logging
import sys

from caladrius.logs import get_top_level_logger
from caladrius.graph.builder.heron.builder import HeronGraphBuilder

LOG: logging.Logger = get_top_level_logger(debug=True)

CONFIG: dict = {
    "heron.tracker.url" :
    'http://heron-tracker-new.prod.heron.service.smf1.twitter.com',
    "gremlin.server.url" : 'localhost:8182'
    }

HGB: HeronGraphBuilder = HeronGraphBuilder(CONFIG)

HGB.build_topology_graph(sys.argv[1], sys.argv[2], "smf1", "test")
