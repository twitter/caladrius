""" This module contains helper methods for use in validating the modelling of
Heron Topologies. """

import logging

import datetime as dt

from typing import Dict, Any, DefaultDict, Union, cast
from collections import defaultdict

import requests

import pandas as pd

from caladrius.common.heron import tracker
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.traffic.heron.stats_summary import \
    StatsSummaryTrafficModel

LOG: logging.Logger = logging.getLogger(__name__)

def get_spout_state(metrics_client: HeronMetricsClient, topology_id: str,
                    tracker_url: str,
                    start: dt.datetime, end: dt.datetime,
                    summary_method: str = "median",
                    **kwargs: Union[str, int, float]
                   ) -> Dict[int, Dict[str, float]]:

    LOG.info("Getting spout emission state dictionary for topology %s over a"
             "period of %d seconds from %s to %s", topology_id,
             (end-start).total_seconds(), start.isoformat(), end.isoformat())

    lplan: Dict[str, Any] = tracker.get_logical_plan(
        tracker_url, cast(str, kwargs["cluster"]),
        cast(str, kwargs["environ"]), topology_id)

    emit_counts: pd.DataFrame = metrics_client.get_emit_counts(
        topology_id, start, end, **kwargs)

    spout_groups: pd.core.groupby.DataFrameGroupBy = \
        (emit_counts[emit_counts["component"].isin(lplan["spouts"])]
         .groupby(["task", "stream"]))

    if summary_method == "median":

        spout_emits: pd.Series = spout_groups.emit_count.median()

    elif summary_method == "mean":

        spout_emits = spout_groups.emit_count.mean()

    else:
        msg: str = f"Unknown summary method: {summary_method}"
        LOG.error(msg)
        raise RuntimeError(msg)

    output: DefaultDict[int, Dict[str, float]] = defaultdict(dict)

    for (task_id, stream), emit_count in spout_emits.iteritems():

        output[task_id][stream] = emit_count

    return output

def traffic_summary_to_spout_state(topology_id: str, cluster: str,
                                   environ: str, source_hours: int=3,
                                   base_url: str = "localhost:5000"):

    traffic_summary_url: str = \
        f"http://{base_url}/model/traffic/heron/{topology_id}"

    response: requests.Response = requests.get(
        traffic_summary_url, params={"cluster" : cluster, "environ" : environ,
                                     "source_hours" : source_hours})

    response.raise_for_status()

    response_dict: Dict[str, Any] = response.json()

    traffic_stats: Dict[str, Any] = \
        response_dict["results"][StatsSummaryTrafficModel.name]

    output = defaultdict(lambda: defaultdict(dict))

    for instance_str, stream_dict in traffic_stats["instances"].items():
        task_id: int = int(instance_str)
        for stream, summary in stream_dict.items():
            for stat_name, value in summary.items():
                output[stat_name][task_id][stream] = value

    return output
