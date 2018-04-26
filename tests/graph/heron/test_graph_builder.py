import logging
import sys

from caladrius.logs import set_up_logging
from caladrius.graph.builder.heron import builder
from caladrius.graph.client.gremlin.client import GremlinClient
from caladrius.common.heron import tracker
from caladrius.metrics.heron.cuckoo.client import HeronCuckooClient
from caladrius.common.time.timestamp import get_window_dt_from_now

LOG: logging.Logger = logging.getLogger(__name__)

#pylint: disable=invalid-name

set_up_logging(debug=True)

tracker_url = 'http://heron-tracker-new.prod.heron.service.smf1.twitter.com'

logical_plan = tracker.get_logical_plan(tracker_url, "smf1", "test",
                                        sys.argv[1])

physical_plan = tracker.get_physical_plan(tracker_url, "smf1", "test",
                                          sys.argv[1])

CONFIG = {"gremlin.server.url" : "localhost:8182",
          "cuckoo.database.url": 'https://cuckoo-prod-smf1.twitter.biz'}

graph_client = GremlinClient(CONFIG)

builder.create_physical_graph(graph_client, sys.argv[1], sys.argv[2],
                              logical_plan, physical_plan)

metrics_client = HeronCuckooClient(CONFIG, "Infra-Caladrius")

start, end = get_window_dt_from_now(seconds=int(sys.argv[3]))

builder.populate_physical_graph(graph_client, metrics_client, sys.argv[1],
                                sys.argv[2], start, end)
