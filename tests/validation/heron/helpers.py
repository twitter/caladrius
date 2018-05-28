# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains helper methods for use in validating the modelling of
Heron Topologies. """

import logging

import datetime as dt

from typing import Dict, Any, DefaultDict, Union
from collections import defaultdict

import pandas as pd

from caladrius.common.heron import tracker
from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)


def get_spout_state(metrics_client: HeronMetricsClient, topology_id: str,
                    cluster: str, environ: str, tracker_url: str,
                    start: dt.datetime, end: dt.datetime,
                    metrics_sample_period: float,
                    summary_method: str = "median",
                    **kwargs: Union[str, int, float]
                    ) -> Dict[int, Dict[str, float]]:
    """ Helper script that will fetch the median or mean spout emission rates
    and format them into the dictionary structure expected by the topology
    performance prediction methods.

    Arguments:
        metrics_client (HeronMetricsClient):    The client for the metrics
                                                database.
        topology_id (str):  The topology identification string.
        cluster (str):  The cluster that that the topology is running on.
        environ (str): The environment that the topology is running in.
        tracker_url (str):  The URL for the Heron Tracker API>
        start (datetime):   The UTC datetime for the start of the metrics
                            gathering period.
        end (datetime): The UTC datetime for the start of the metrics
                        gathering period.
        metrics_sample_period (float):  The period that metrics are sampled
                                        into. eg 60 secs (1 min), 300 secs
                                        (5 mins).
        summary_method (str):   The method to use to summerise the emit counts.
                                Either "mean" to "median". Defaults to median.
        **kwargs:   Any additional keyword arguments required by the metrics
                    client.

    Returns:
        Dict[int, Dict[str, float]]:    A dictionary mapping from task ID to a
        dict that maps from output stream name to an emission rate in tuples
        per second.
    """

    LOG.info("Getting spout emission state dictionary for topology %s over a"
             "period of %d seconds from %s to %s", topology_id,
             (end-start).total_seconds(), start.isoformat(), end.isoformat())

    lplan: Dict[str, Any] = tracker.get_logical_plan(
        tracker_url, cluster, environ, topology_id)

    emit_counts: pd.DataFrame = metrics_client.get_emit_counts(
        topology_id, cluster, environ, start, end, **kwargs)

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

        output[task_id][stream] = emit_count / metrics_sample_period

    return dict(output)
