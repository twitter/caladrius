# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module defines an abstract base class that is used to compare performance of a proposed
 packing plan with the current one"""

from abc import abstractmethod
import datetime as dt
import json
from typing import Any

from caladrius.common.heron import tracker
from caladrius.metrics.client import MetricsClient
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.model.topology.heron.abs_queueing_models import QueueingModels
from caladrius.model.topology.heron.helpers import *
from caladrius.performance_prediction.util import util

LOG: logging.Logger = logging.getLogger(__name__)


class Predictor(object):
    """ Abstract base class for performance predictors """
    def __init__(self, topology_id: str, cluster: str, environ: str,
                 start: [dt.datetime], end: [dt.datetime], tracker_url: str, metrics_client: MetricsClient,
                 graph_client: GremlinClient, queue: QueueingModels, **kwargs: Any) -> None:

        self.metrics_client: MetricsClient = metrics_client
        self.graph_client: GremlinClient = graph_client
        self.topology_id = topology_id
        self.queue = queue
        self.cluster = cluster
        self.environ = environ
        self.start = start
        self.end = end
        self.kwargs = kwargs

        current_packing_plan = json.loads(tracker.get_packing_plan(
            tracker_url, cluster, environ, topology_id))
        del current_packing_plan["id"]
        util.validate_packing_plan(current_packing_plan)

        # summarize the plans
        self.current_plan: pd.DataFrame = \
            pd.DataFrame.from_records(util.summarize_packing_plans(current_packing_plan),
                                      index=util.InstanceInfo._fields).transpose().reset_index().\
                                      rename(columns={'index':'instance'})

    @abstractmethod
    def create_new_plan(self) -> Dict[str, Any]:
        """ Predicts the performance of the new packing plan by 1) finding
        out the performance of the current plan, 2) finding out where the
        new plan is different 3) analysing how that might impact the new
        plan's performance.

        """
        pass
