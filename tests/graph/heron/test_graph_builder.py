import logging

from caladrius.logs import get_top_level_logger
from caladrius.graph.heron.heron_graph_builder import HeronGraphBuilder

#LOG: logging.Logger = get_top_level_logger(debug=True)

CONFIG: dict = {
    "heron.tracker.url" :
    'http://heron-tracker-new.prod.heron.service.smf1.twitter.com',
    "caladrius.graph.db.url" : 'localhost:8182'
    }

HGB: HeronGraphBuilder = HeronGraphBuilder(CONFIG)

HGB.build_topology_graph("ossWordCount3", "test1", "smf1", "test")
