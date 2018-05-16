""" This module contains classes and methods for modelling traffic based on
summaries of historic spout emission metrics. """

import logging

import datetime as dt

from typing import List, Dict, Any, Union, cast

import pandas as pd

from caladrius.model.traffic.base import TrafficModel
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)

SUMMARY_DICT = Dict[str, float]

class StatsSummaryTrafficModel(TrafficModel):
    """ This model provides summary statistics for the spout instances emit
    metrics (traffic)."""

    name: str = "stats_summary_traffic_model"

    description: str = ("Provides summary traffic statistics for the "
                        "specified topology. Statistics are based on emit "
                        "count metrics from the topologies spout instances.")

    def __init__(self, config: dict, metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:

        super().__init__(config, metrics_client, graph_client)

        self.metrics_client: HeronMetricsClient
        self.default_source_hours: int = \
            config["stats.summary.model.default.source.hours"]

    def predict_traffic(self, topology_id: str,
                        **kwargs: Union[str, int, float]) -> Dict[str, Any]:
        """ This method will provide a summary of the emit counts from the
        spout instances of the specified topology. It will summarise the emit
        metrics over the number of hours defined by the source_hours keyword
        argument and provide summary statistics (mean, median, min, max and
        quantiles) over all instances and for each individual instance.

        Arguments:
            topology_id (str):  The topology ID string
            **source_hours (int):   Optional keyword argument for the number of
                                    hours (backwards from now) of metrics data
                                    to summarise.
        Returns:
            A dictionary with top level keys for "overall" statistics and
            "instances=" with summary statistics for each instance. The summary
            dictionaries have keys for each of the provided statistics linking
            to a float value for that statistic.
        """

        if "source_hours" not in kwargs:
            LOG.warning("source_hours parameter (indicating how many hours of "
                        "historical data to summarise) was not provided, "
                        "using default value of %d hours",
                        self.default_source_hours)
            source_hours: int = self.default_source_hours

        source_hours = cast(int, kwargs["source_hours"])

        LOG.info("Predicting traffic for topology %s using statistics summary "
                 "model", topology_id)

        end: dt.datetime = dt.datetime.now(dt.timezone.utc)
        start: dt.datetime = end - dt.timedelta(hours=source_hours)

        spout_comps: List[str] = (self.graph_client.graph_traversal.V()
                                  .has("topology_id", topology_id)
                                  .hasLabel("spout").values("component")
                                  .dedup().toList())

        emit_counts: pd.DataFrame = self.metrics_client.get_emit_counts(
            topology_id, start, end, **kwargs)

        spout_emit_counts: pd.DataFrame = emit_counts[
            emit_counts["component"].isin(spout_comps)]

        output: Dict[str, Any] = {}

        output["details"] = {"start": start.isoformat(),
                             "end" : end.isoformat(),
                             "source_hours" : source_hours}

        overall: Dict[str, float] = {}

        quantiles: List[int] = [10, 90, 95, 99]

        overall["mean"] = float(spout_emit_counts.emit_count.mean())
        overall["median"] = float(spout_emit_counts.emit_count.median())
        overall["max"] = float(spout_emit_counts.emit_count.max())
        overall["min"] = float(spout_emit_counts.emit_count.min())
        for quantile in quantiles:
            overall[f"{quantile}-quantile"] = \
                float(spout_emit_counts.emit_count.quantile(quantile/100))

        output["overall"] = overall

        instances: Dict[int, SUMMARY_DICT] = {}
        for task_id, task_data in spout_emit_counts.groupby("task"):
            instance: SUMMARY_DICT = {}
            instance["mean"] = float(task_data.emit_count.mean())
            instance["median"] = float(task_data.emit_count.median())
            instance["max"] = float(task_data.emit_count.max())
            instance["min"] = float(task_data.emit_count.min())
            for quantile in quantiles:
                instance[f"{quantile}-quantile"] = \
                    float(task_data.emit_count.quantile(quantile/100))
            instances[task_id] = instance

        output["instances"] = instances

        return output
