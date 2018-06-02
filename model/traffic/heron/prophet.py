# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods and classes for forecasting the traffic from
spout instances in a Heron topology using Facebook's Prophet times series
prediction package: https://facebook.github.io/prophet/"""

import logging

import datetime as dt

from typing import Any, Dict, DefaultDict, Union, cast, List
from functools import lru_cache
from collections import defaultdict
from copy import deepcopy

import pandas as pd

from fbprophet import Prophet

from caladrius.common.heron import tracker
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.traffic.heron.base import HeronTrafficModel
from caladrius.graph.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)


def get_spout_emissions(metric_client: HeronMetricsClient, tracker_url: str,
                        topology_id: str, cluster: str, environ: str,
                        start: dt.datetime, end: dt.datetime) -> pd.DataFrame:

    emit_counts: pd.DataFrame = metric_client.get_emit_counts(
            topology_id, cluster, environ, start, end)

    lplan: Dict[str, Any] = tracker.get_logical_plan(tracker_url, cluster,
                                                     environ, topology_id)

    spout_emits: pd.DataFrame = \
        emit_counts[emit_counts.component.isin(lplan["spouts"].keys())]

    return spout_emits


@lru_cache()
def build_component_models(
        metric_client: HeronMetricsClient, tracker_url: str, topology_id: str,
        cluster: str, environ: str, start: dt.datetime, end: dt.datetime
        ) -> DefaultDict[str, Dict[str, Prophet]]:

    LOG.info("Creating traffic models for spout components of topology %s",
             topology_id)

    spout_instance_emits: pd.DataFrame = get_spout_emissions(
        metric_client, tracker_url, topology_id, cluster, environ, start, end)

    spout_comp_emits: pd.DataFrame = \
        (spout_instance_emits.groupby(["component", "stream", "timestamp"])
         .mean()["emit_count"].reset_index())

    output: DefaultDict[str, Dict[str, Prophet]] = defaultdict(dict)

    for (spout_comp, stream), data in spout_comp_emits.groupby(["component",
                                                                "stream"]):

        LOG.info("Creating traffic model for spout %s stream %s", spout_comp,
                 stream)

        df: pd.DataFrame = (data[["timestamp", "emit_count"]]
                            .rename(index=str, columns={"timestamp": "ds",
                                                        "emit_count": "y"}))
        model: Prophet = Prophet()
        model.fit(df)

        output[spout_comp][stream] = model

    return output


@lru_cache()
def build_instance_models(
        metric_client: HeronMetricsClient, tracker_url: str, topology_id: str,
        cluster: str, environ: str, start: dt.datetime, end: dt.datetime
        ) -> DefaultDict[str, DefaultDict[int, Dict[str, Prophet]]]:

    spout_emits: pd.DataFrame = get_spout_emissions(
        metric_client, tracker_url, topology_id, cluster, environ, start, end)

    spout_groups: pd.core.groupby.DataFrameGroupBy = \
        (spout_emits[["component", "task", "stream",  "timestamp",
                      "emit_count"]]
         .groupby(["component", "task", "stream"]))

    output: DefaultDict[str, DefaultDict[int, Dict[str, Prophet]]] = \
        defaultdict(lambda: defaultdict(dict))

    for (spout_comp, task, stream), data in spout_groups:
        df: pd.DataFrame = (data[["timestamp", "emit_count"]]
                            .rename(index=str, columns={"timestamp": "ds",
                                                        "emit_count": "y"}))

        model: Prophet = Prophet()
        model.fit(df)

        output[spout_comp][task][stream] = model

    return output


def predict_per_instance(metric_client: HeronMetricsClient, tracker_url: str,
                         topology_id: str, cluster: str, environ: str,
                         start: dt.datetime, end: dt.datetime,
                         future_mins: int) -> pd.DataFrame:

    models: DefaultDict[str, DefaultDict[int, Dict[str, Prophet]]] = \
        build_instance_models(metric_client, tracker_url, topology_id, cluster,
                              environ, start, end)

    output: pd.DataFrame = None

    for spout_comp, task_dict in models.items():
        for task, stream_models in task_dict.items():
            for stream, model in stream_models.items():

                future: pd.DataFrame = model.make_future_dataframe(
                    periods=future_mins, freq='T')

                forecast: pd.DataFrame = model.predict(future)

                future_only: pd.DataFrame = deepcopy(
                    forecast[forecast.ds > (model.start + model.t_scale)])

                future_only["stream"] = stream
                future_only["task"] = task
                future_only["component"] = spout_comp

                if output is None:
                    output = future_only
                else:
                    output = output.append(future_only)

    return output


class ProphetTrafficModel(HeronTrafficModel):
    """ This model class provides traffic forecasting for Heron topologies
    using the Facebook Prophet time series forecasting package:
    https://facebook.github.io/prophet/
    """

    name: str = "prophet"

    description: str = ("This model uses the Facebook Prophet time series "
                        "prediction package to forecast the spout traffic for "
                        "a given amount of time into the future.")

    def __init__(self, config: dict, metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:

        super().__init__(config, metrics_client, graph_client)

        self.metrics_client: HeronMetricsClient

        if "heron.tracker.url" in config:

            self.tracker_url: str = \
                config["heron.tracker.url"]

        else:
            tracker_msg: str = (
                "The Heron Tracker URL was not supplied in the configuration "
                "dictionary given to the Prophet traffic model. Please check "
                "the keys under the 'heron.traffic.model.config' in the main "
                "configuration file")
            LOG.error(tracker_msg)
            raise RuntimeError(tracker_msg)

        if "prophet.model.default.source.hours" in config:

            self.default_source_hours: int = \
                config["prophet.model.default.source.hours"]

        else:
            self.default_source_hours = 24
            LOG.warning("Default source hours were not supplied via "
                        "configuration file. Setting to %d hours.",
                        self.default_source_hours)

        if "prophet.model.default.future.mins" in config:

            self.default_future_mins: int = \
                config["prophet.model.default.future.mins"]

        else:
            self.default_future_minuets = 30
            LOG.warning("Default future minutes parameter was not supplied "
                        "via configuration file. Setting to %d minutes.",
                        self.default_future_mins)

        if "prophet.model.quantiles" in config:
            self.quantiles: List[int] = config["prophet.model.quantiles"]
        else:
            LOG.warning("Quantile values were not set via configuration file "
                        " using: 10, 90, 95, 99 as defaults.")
            self.quantiles = [10, 90, 95, 99]

    def predict_traffic(self, topology_id: str, cluster: str, environ: str,
                        **kwargs: Union[str, int, float]) -> Dict[str, Any]:

        if "source_hours" not in kwargs:
            LOG.warning("source_hours parameter (indicating how many hours of "
                        "historical data to summarise) was not provided, "
                        "using default value of %d hours",
                        self.default_source_hours)
            source_hours: int = self.default_source_hours
        else:
            source_hours = cast(int, kwargs["source_hours"])

        source_end: dt.datetime = \
            dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        source_start: dt.datetime = (source_end -
                                     dt.timedelta(hours=source_hours))

        if "future_mins" not in kwargs:
            LOG.warning("future_mins parameter (indicating how many minutes "
                        "into the future traffic should be predicted was not "
                        "provided, using default value of %d minutes",
                        self.default_future_mins)
            future_mins: int = self.default_future_mins
        else:
            future_mins = cast(int, kwargs["future_mins"])

        LOG.info("Predicting traffic over the next %d minutes for topology %s "
                 "using a Prophet model trained on metrics from a %f hour "
                 "period from %s to %s", future_mins, topology_id,
                 (source_end-source_start).total_seconds() / 3600,
                 source_start.isoformat(), source_end.isoformat())

        if "metrics_sample_period" in kwargs:
            time_period_sec: float = cast(float,
                                          kwargs["metrics_sample_period"])
        else:
            sample_period_err: str = \
                ("Inferring metric sample period is not yet supported. Please "
                 "supply the period which the metrics timeseries is reported "
                 "in.")
            LOG.error(sample_period_err)
            raise NotImplementedError(sample_period_err)

        traffic: pd.DataFrame = predict_per_instance(
            self.metrics_client, self.tracker_url, topology_id, cluster,
            environ, source_start, source_end, future_mins)

        traffic_by_task: pd.core.groupby.DataFrameGroupBy = \
            traffic.groupby(["task", "stream"])

        output: Dict[str, Any] = {}

        instances: DefaultDict[str, DefaultDict[str, Dict[str, float]]] = \
            defaultdict(lambda: defaultdict(dict))

        for (task_id, stream), data in traffic_by_task:

            instances["mean"][str(task_id)][stream] = \
                (float(data.yhat.mean()) / time_period_sec)

            instances["median"][str(task_id)][stream] = \
                (float(data.yhat.median()) / time_period_sec)

            instances["max"][str(task_id)][stream] = \
                (float(data.yhat.max()) / time_period_sec)

            instances["min"][str(task_id)][stream] = \
                (float(data.yhat.min()) / time_period_sec)

            for quantile in self.quantiles:
                instances[f"{quantile}-quantile"][str(task_id)][stream] = \
                    (float(data.yhat.quantile(quantile/100)) /
                     time_period_sec)

        output["instances"] = instances

        return output
