# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains classes and methods for modelling traffic based on
summaries of historic spout emission metrics. """

import logging

import datetime as dt

from typing import List, Dict, Any, Union, cast, DefaultDict
from collections import defaultdict

import pandas as pd

from caladrius.common.timestamp import calculate_ts_period
from caladrius.model.traffic.heron.base import HeronTrafficModel
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)

SUMMARY_DICT = Dict[str, float]


class StatsSummaryTrafficModel(HeronTrafficModel):
    """ This model provides summary statistics for the spout instances emit
    metrics (traffic)."""

    name: str = "stats_summary"

    description: str = ("Provides summary traffic statistics for the "
                        "specified topology. Statistics are based on emit "
                        "count metrics from the topologies spout instances.")

    def __init__(self, config: dict, metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:

        super().__init__(config, metrics_client, graph_client)

        self.metrics_client: HeronMetricsClient

        if "stats.summary.model.default.source.hours" in config:

            self.default_source_hours: float = \
                config["stats.summary.model.default.source.hours"]

        else:
            LOG.warning("Default source hours were not supplied via "
                        "configuration file. Setting to 3 hours.")
            self.default_source_hours = 3

        if "stats.summary.model.quantiles" in config:
            self.quantiles: List[int] = config["stats.summary.model.quantiles"]
        else:
            LOG.warning("Quantile values were not set via configuration file "
                        " using; 10, 90, 95, 99 as defaults.")
            self.quantiles = [10, 90, 95, 99]

    def predict_traffic(self, topology_id: str, cluster: str, environ: str,
                        **kwargs: Union[str, int, float]) -> Dict[str, Any]:
        """ This method will provide a summary of the emit counts from the
        spout instances of the specified topology. It will summarise the emit
        metrics over the number of hours defined by the source_hours keyword
        argument and provide summary statistics (mean, median, min, max and
        quantiles) over all instances of each component and for each individual
        instance.

        Arguments:
            topology_id (str):  The topology ID string
            **source_hours (int):   Optional keyword argument for the number of
                                    hours (backwards from now) of metrics data
                                    to summarise.
            **metric_sample_period (float): This is an optional argument
                                            specifying the period (seconds) of
                                            the metrics returned by the metrics
                                            client.
        Returns:
            dict:   A dictionary with top level keys for "components" which
            links to summary statistics for each spout instance and "instances"
            with summary statistics for each individual instance of each spout
            component.

            The dictionary has the form:
                ["components"]
                    [statistic_name]
                        [component_name]
                            [output_stream_name] = emit_count

                ["instances"]
                    [statistic_name]
                        [task_id]
                            [output_stream_name] = emit_count

            All dictionary keys are stings to allow easy conversion to JSON.
        """

        if "source_hours" not in kwargs:
            LOG.warning("source_hours parameter (indicating how many hours of "
                        "historical data to summarise) was not provided, "
                        "using default value of %d hours",
                        self.default_source_hours)
            source_hours: float = self.default_source_hours
        else:
            source_hours = cast(float, float(kwargs["source_hours"]))

        LOG.info("Predicting traffic for topology %s using statistics summary "
                 "model", topology_id)

        end: dt.datetime = dt.datetime.now(dt.timezone.utc)
        start: dt.datetime = end - dt.timedelta(hours=source_hours)

        spout_comps: List[str] = (self.graph_client.graph_traversal.V()
                                  .has("topology_id", topology_id)
                                  .hasLabel("spout").values("component")
                                  .dedup().toList())

        emit_counts: pd.DataFrame = self.metrics_client.get_emit_counts(
            topology_id, cluster, environ, start, end, **kwargs)

        spout_emit_counts: pd.DataFrame = emit_counts[
            emit_counts["component"].isin(spout_comps)]

        if "metrics_sample_period" in kwargs:
            time_period_sec: float = \
                cast(float, float(kwargs["metrics_sample_period"]))
        else:
            # TODO: This method needs to be made robust, interleaved unique
            # timestamps will return entirely the wrong period!!!
            # TODO: Maybe introduce aggregation time bucket into metric client
            # methods to create known time period.
            # TODO: Or just accept the timer frequency of the metrics as an
            # argument? For TMaster it is always 1 min and for others this can
            # be supplied in the kwargs passed to the metrics get method so it
            # will be available.
            time_period_sec: float = \
                    calculate_ts_period(spout_emit_counts.timestamp)
            LOG.info("Emit count data was calculated to have a period of %f "
                     "seconds", time_period_sec)

        output: Dict[str, Any] = {}

        output["details"] = {"start": start.isoformat(),
                             "end": end.isoformat(),
                             "source_hours": source_hours,
                             "original_metric_frequency_secs":
                             time_period_sec}

        components: DefaultDict[str, DefaultDict[str, SUMMARY_DICT]] = \
            defaultdict(lambda: defaultdict(dict))

        for (comp, stream), comp_data in \
                spout_emit_counts.groupby(["component", "stream"]):

            LOG.debug("Processing component: %s stream: %s", comp, stream)

            components["mean"][comp][stream] = \
                (float(comp_data.emit_count.mean()) / time_period_sec)

            components["median"][comp][stream] = \
                (float(comp_data.emit_count.median()) / time_period_sec)

            components["max"][comp][stream] = \
                (float(comp_data.emit_count.max()) / time_period_sec)

            components["min"][comp][stream] = \
                (float(comp_data.emit_count.min()) / time_period_sec)

            for quantile in self.quantiles:
                components[f"{quantile}-quantile"][comp][stream] = \
                    (float(comp_data.emit_count.quantile(quantile/100)) /
                     time_period_sec)

        output["components"] = components

        instances: DefaultDict[str, DefaultDict[str, SUMMARY_DICT]] = \
            defaultdict(lambda: defaultdict(dict))

        for (task_id, stream), task_data in \
                spout_emit_counts.groupby(["task", "stream"]):

            LOG.debug("Processing instance: %d stream: %s", task_id, stream)

            instances["mean"][str(task_id)][stream] = \
                (float(task_data.emit_count.mean()) / time_period_sec)

            instances["median"][str(task_id)][stream] = \
                (float(task_data.emit_count.median()) / time_period_sec)

            instances["max"][str(task_id)][stream] = \
                (float(task_data.emit_count.max()) / time_period_sec)

            instances["min"][str(task_id)][stream] = \
                (float(task_data.emit_count.min()) / time_period_sec)

            for quantile in self.quantiles:
                instances[f"{quantile}-quantile"][str(task_id)][stream] = \
                    (float(task_data.emit_count.quantile(quantile/100)) /
                     time_period_sec)

        output["instances"] = instances

        return output
