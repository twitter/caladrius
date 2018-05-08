""" This module contains classes and methods for modelling traffic based on
summaries of historic spout emission metrics. """

import logging

import datetime as dt

from typing import List, Dict, Union

import pandas as pd

from gremlin_python.process.graph_traversal import GraphTraversalSource

from caladrius.model.traffic.model import TrafficModel
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)

class BasicSatsTrafficModel(TrafficModel):

    def __init__(self, config: dict, metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:
        super().__init__(config, metrics_client, graph_client,
                         "basic_stats_traffic_model")

    def predict_traffic(self, topology_id: str, **kwargs: int) -> dict:

        hours: int = kwargs.get("hours", 3)

        end: dt.datetime = dt.datetime.now(dt.timezone.utc)
        start: dt.datetime = end - dt.timedelta(hours=hours)

        spout_comps: List[str] = (self.graph_client.graph_traversal.V()
                                  .has("topology_id", topology_id)
                                  .hasLabel("spout").values("component")
                                  .dedup().toList())

        emit_counts: pd.DataFrame = self.metrics_client.get_emit_counts(
            topology_id, start, end, **kwargs)

        spout_emit_counts: pd.DataFrame = emit_counts[
            emit_counts["component"].isin(spout_comps)]["emit_count"]

        output: Dict[Union[str, int], Dict[str, Union[str, float]]] = {}

        output["details"] = {"start": start.isoformat(),
                              "end" : end.isoformat()}

        overall: Dict[str, float] = {}

        overall["mean"] = spout_emit_counts.mean()
        overall["median"] = spout_emit_counts.mean()
        overall["max"] = spout_emit_counts.max()
        overall["min"] = spout_emit_counts.min()
        for quantile in [10, 90, 95, 99]:
            overall[f"{quantile}-quantile"] = \
                spout_emit_counts.quantile(quantile/100)

        output["overall"] = overall

        return output
