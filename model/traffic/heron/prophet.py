import logging

import datetime as dt

from typing import Any, Dict, DefaultDict
from functools import lru_cache
from collections import defaultdict

import pandas as pd

from fbprophet import Prophet

from common.heron import tracker
from metrics.heron.client import HeronMetricsClient

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
def build_instance_models(metric_client: HeronMetricsClient, tracker_url: str,
                          topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime
                          ) -> Dict[str, Dict[int, Prophet]]:

    spout_emits: pd.DataFrame = get_spout_emissions(
        metric_client, tracker_url, topology_id, cluster, environ, start, end)

    spout_groups: pd.core.groupby.DataFrameGroupBy = \
        (spout_emits[["component", "task", "timestamp", "emit_count"]]
         .groupby(["component", "task"]))

    output: DefaultDict[str, Dict[int, Prophet]] = defaultdict(dict)

    for (spout_comp, task), data in spout_groups:
        df: pd.DataFrame = (data[["timestamp", "emit_count"]]
                            .rename(index=str, columns={"timestamp": "ds",
                                                        "emit_count": "y"}))

        model: Prophet = Prophet()
        model.fit(df)

        output[spout_comp][task] = model

    return dict(output)


def predict_per_instance(metric_client: HeronMetricsClient, tracker_url: str,
                         topology_id: str, cluster: str, environ: str,
                         start: dt.datetime, end: dt.datetime,
                         future_mins: int) -> pd.DataFrame:

    models: Dict[str, Dict[int, Prophet]] = build_instance_models(
        metric_client, tracker_url, topology_id, cluster, environ, start, end)

    output: pd.DataFrame = None

    for spout_comp, instance_models in models.items():
        for task, model in instance_models.items():

            future: pd.DataFrame = model.make_future_dataframe(
                periods=future_mins, freq='T')

            forecast: pd.DataFrame = model.predict(future)

            future_only: pd.DataFrame = forecast[forecast.ds >
                                                 (model.start + model.t_scale)]

            future_only["task"] = task
            future_only["component"] = spout_comp

            if output is None:
                output = future_only
            else:
                output = output.append(future_only)

    return output
