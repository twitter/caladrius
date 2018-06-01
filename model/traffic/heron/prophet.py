import logging

import datetime as dt

from typing import Any, Dict

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


def predict(spout_emits: pd.DataFrame, future_mins: int) -> pd.DataFrame:

    spout_groups: pd.DataFrameGroupBy = \
        (spout_emits[["component", "task", "timestamp", "emit_count"]]
         .groupby(["component", "task"]))

    output: pd.DataFrame = None

    for (spout_comp, task), data in spout_groups:
        df: pd.DataFrame = (data[["timestamp", "emit_count"]]
                            .rename(index=str, columns={"timestamp": "ds",
                                                        "emit_count": "y"}))

        # TODO: Move model build to separate LRU Cached method based on source
        # time period variable
        model: Prophet = Prophet()
        model.fit(df)
        future: pd.DataFrame = model.make_future_dataframe(periods=future_mins,
                                                           freq='T')
        forecast: pd.DataFrame = model.predict(future)

        future_only: pd.DataFrame = forecast[forecast.ds > df.ds.max()]

        future_only["task"] = task
        future_only["component"] = spout_comp

        if output is None:
            output = future_only
        else:
            output.append(future_only)

        return output
