# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

import datetime as dt

from typing import Union

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from caladrius.metrics.heron.client import HeronMetricsClient


def plot_emit_complete_latency(metrics_client: HeronMetricsClient,
                               topology_id: str, cluster: str, environ: str,
                               start: dt.datetime, end: dt.datetime,
                               **kwargs: Union[str, int, float]):

    emit_counts: pd.DataFrame = metrics_client.get_emit_counts(
        topology_id, cluster, environ, start, end, **kwargs)

    complete_latencies: pd.DataFrame = metrics_client.get_complete_latencies(
        topology_id, cluster, environ, start, end, **kwargs)

    spouts: np.ndarray = complete_latencies.component.unique()

    emit_counts = emit_counts[emit_counts.component.isin(spouts)]

    combined: pd.DataFrame = emit_counts.merge(
        complete_latencies, on=["task", "timestamp"])[["task", "timestamp",
                                                       "emit_count",
                                                       "latency_ms"]]

    for (task, stream), data in combined.groupby(["task", "stream"]):
        fig, ax1 = plt.subplots()

        color = 'tab:red'
        ax1.set_xlabel('timestamp')
        ax1.set_ylabel('latency (ms)', color=color)
        ax1.plot(data.timestamp, data.latency_ms, color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        # instantiate a second axes that shares the same x-axis
        ax2 = ax1.twinx()

        color = 'tab:blue'
        # we already handled the x-label with ax1
        ax2.set_ylabel('count', color=color)
        ax2.plot(data.timestamp, data.emit_count, color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()  # otherwise the right y-label is slightly clipped
        plt.show()
