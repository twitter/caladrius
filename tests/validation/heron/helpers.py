""" This module contains helper methods for use in validating the modelling of
Heron Topologies. """

import logging

import datetime as dt

from typing import Dict, Any, DefaultDict, Union, cast
from collections import defaultdict

import pandas as pd

from caladrius.common.heron import tracker
from caladrius.metrics.heron.client import HeronMetricsClient

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
