# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods and classes for forecasting the traffic from
spout instances in a Heron topology using Facebook's Prophet times series
prediction package: https://facebook.github.io/prophet/"""

import logging

import datetime as dt

from typing import Any, Dict, DefaultDict, Union, cast, List, Optional
from collections import defaultdict

import pandas as pd

from fbprophet import Prophet

from caladrius.common.heron import tracker
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.traffic.heron.base import HeronTrafficModel
from caladrius.graph.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)

INSTANCE_MODELS = DefaultDict[str, DefaultDict[int, Dict[str, Prophet]]]
COMPONENT_MODELS = DefaultDict[str, Dict[str, Prophet]]


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


def build_component_models(
        metric_client: HeronMetricsClient, tracker_url: str, topology_id: str,
        cluster: str, environ: str, start: dt.datetime = None, end: dt.datetime = None,
        spout_emits: Optional[pd.DataFrame]=None) -> DefaultDict[str, Dict[str, Prophet]]:

    LOG.info("Creating traffic models for spout components of topology %s",
             topology_id)

    if start and end and spout_emits is None:
        spout_emits = get_spout_emissions(metric_client, tracker_url,
                                          topology_id, cluster, environ, start,
                                          end)
    elif spout_emits is None and ((not start and end) or (start and not end)):
        err: str = ("Either start and end datetime instances or the spout "
                    "emits should be provided")
        LOG.error(err)
        raise RuntimeError(err)

    spout_comp_emits: pd.DataFrame = \
        (spout_emits.groupby(["component", "stream", "timestamp"])
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


def predict_per_component(metric_client: HeronMetricsClient, tracker_url: str,
                          topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime,
                          future_mins: int) -> pd.DataFrame:

    models: DefaultDict[str, Dict[str, Prophet]] = \
        build_component_models(metric_client, tracker_url, topology_id,
                               cluster, environ, start, end)

    return run_per_component(models, future_mins)


def run_per_component(models: COMPONENT_MODELS, future_mins: int) -> pd.DataFrame:
    output: pd.DataFrame = None

    for spout_comp, stream_models in models.items():
        for stream, model in stream_models.items():

            future: pd.DataFrame = model.make_future_dataframe(
                periods=future_mins, freq='T', include_history=False)

            forecast: pd.DataFrame = model.predict(future)

            forecast["stream"] = stream
            forecast["component"] = spout_comp

            if output is None:
                output = forecast
            else:
                output = output.append(forecast)

    return output


def build_instance_models(
        metric_client: HeronMetricsClient, tracker_url: str, topology_id: str,
        cluster: str, environ: str, start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
        spout_emits: Optional[pd.DataFrame] = None) -> INSTANCE_MODELS:

    if start and end and spout_emits is None:
        spout_emits = get_spout_emissions(metric_client, tracker_url,
                                          topology_id, cluster, environ, start,
                                          end)
    elif spout_emits is None and ((not start and end) or (start and not end)):
        err: str = ("Both start and end datetimes should be provided if spout "
                    "emits DataFrame is not")
        LOG.error(err)
        raise RuntimeError(err)
    elif spout_emits is None and not start and not end:
        err = ("Either start and end datetimes or the spout emits should be "
               "provided")
        LOG.error(err)
        raise RuntimeError(err)
    else:
        spout_emits = cast(pd.DataFrame, spout_emits)

    spout_groups: pd.core.groupby.DataFrameGroupBy = \
        (spout_emits[["component", "task", "stream", "timestamp",
                      "emit_count"]]
         .groupby(["component", "task", "stream"]))

    output: INSTANCE_MODELS = defaultdict(lambda: defaultdict(dict))

    for (spout_comp, task, stream), data in spout_groups:
        df: pd.DataFrame = (data[["timestamp", "emit_count"]]
                            .rename(index=str, columns={"timestamp": "ds",
                                                        "emit_count": "y"}))

        model: Prophet = Prophet()
        model.fit(df)

        output[spout_comp][task][stream] = model

    return output


def run_per_instance_models(models: INSTANCE_MODELS,
                            future_mins: int) -> pd.DataFrame:

    output: pd.DataFrame = None

    for spout_comp, task_dict in models.items():
        for task, stream_models in task_dict.items():
            for stream, model in stream_models.items():

                future: pd.DataFrame = model.make_future_dataframe(
                    periods=future_mins, freq='T', include_history=False)

                forecast: pd.DataFrame = model.predict(future)

                forecast["stream"] = stream
                forecast["task"] = task
                forecast["component"] = spout_comp

                if output is None:
                    output = forecast
                else:
                    output = output.append(forecast)

    return output


def predict_per_instance(metric_client: HeronMetricsClient, tracker_url: str,
                         topology_id: str, cluster: str, environ: str,
                         start: dt.datetime, end: dt.datetime,
                         future_mins: int) -> pd.DataFrame:

    models = build_instance_models(metric_client, tracker_url, topology_id,
                                   cluster, environ, start, end)

    return run_per_instance_models(models, future_mins)



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

            self.default_source_hours: float = \
                config["prophet.model.default.source.hours"]

        else:
            self.default_source_hours = 24
            LOG.warning("Default source hours were not supplied via "
                        "configuration file. Setting to %d hours.",
                        self.default_source_hours)

        if "prophet.model.default.future.mins" in config:
            self.default_future_minutes: int = \
                config["prophet.model.default.future.mins"]

        else:
            self.default_future_minutes = 30
            LOG.warning("Default future minutes parameter was not supplied "
                        "via configuration file. Setting to %d minutes.",
                        self.default_future_minutes)

        if "prophet.model.default.metrics_sample_period" in config:
            self.default_metrics_sample_period: int = \
                config["prophet.model.default.metrics_sample_period"]

        else:
            self.default_metrics_sample_period = 60
            LOG.warning("Default metrics sample period parameter was not supplied "
                        "via configuration file. Setting to %d minutes.",
                        self.default_metrics_sample_period)


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
            source_hours: float = self.default_source_hours
        else:
            source_hours = cast(float, float(kwargs["source_hours"]))

        source_end: dt.datetime = \
            dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        source_start: dt.datetime = (source_end -
                                     dt.timedelta(hours=source_hours))

        if "future_mins" not in kwargs:
            LOG.warning("future_mins parameter (indicating how many minutes "
                        "into the future traffic should be predicted was not "
                        "provided, using default value of %d minutes",
                        self.default_future_minutes)
            future_mins: int = self.default_future_minutes
        else:
            future_mins = cast(int, int(kwargs["future_mins"]))

        LOG.info("Predicting traffic over the next %d minutes for topology %s "
                 "using a Prophet model trained on metrics from a %f hour "
                 "period from %s to %s", future_mins, topology_id,
                 (source_end-source_start).total_seconds() / 3600,
                 source_start.isoformat(), source_end.isoformat())

        if "metrics_sample_period" in kwargs:
            time_period_sec: float = \
                cast(float, float(kwargs["metrics_sample_period"]))
        else:
            LOG.warning("metrics_sample_period (indicating the period of time the metrics client retrieves metrics for)"
                        " was not supplied. Using default value of %d seconds.", self.default_metrics_sample_period)
            time_period_sec: int = self.default_metrics_sample_period

        output: Dict[str, Any] = {}

        # Per component predictions

        component_traffic: pd.DataFrame = predict_per_component(
            self.metrics_client, self.tracker_url, topology_id,
            cluster, environ, source_start, source_end, future_mins)

        traffic_by_component: pd.core.groupby.DataFrameGroupBy = \
            component_traffic.groupby(["component", "stream"])

        components: DefaultDict[str, DefaultDict[str, Dict[str, float]]] = \
            defaultdict(lambda: defaultdict(dict))

        for (spout_component, stream), data in traffic_by_component:

            components["mean"][spout_component][stream] = \
                (float(data.yhat.mean()) / time_period_sec)

            components["median"][spout_component][stream] = \
                (float(data.yhat.median()) / time_period_sec)

            components["max"][spout_component][stream] = \
                (float(data.yhat.max()) / time_period_sec)

            components["min"][spout_component][stream] = \
                (float(data.yhat.min()) / time_period_sec)

            for quantile in self.quantiles:
                components[f"{quantile}-quantile"][spout_component][stream] = \
                    (float(data.yhat.quantile(quantile/100)) /
                     time_period_sec)

        output["components"] = components

        # Per instance predictions

        instance_traffic: pd.DataFrame = predict_per_instance(
            self.metrics_client, self.tracker_url, topology_id, cluster,
            environ, source_start, source_end, future_mins)

        traffic_by_task: pd.core.groupby.DataFrameGroupBy = \
            instance_traffic.groupby(["task", "stream"])

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
