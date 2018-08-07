# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for calculating the input output (I/O) ratio
for the instances of a given topology. """

import logging

import datetime as dt

from typing import Dict, Union, List, Tuple

import pandas as pd
import numpy as np

from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient

LOG: logging.Logger = logging.getLogger(__name__)


def get_in_out_components(graph_client: GremlinClient,
                          topology_id: str) -> List[str]:
    """ Gets a list of components that have both incoming and outgoing streams.

    Arguments:
        graph_client (GremlinClient):   The client instance for the graph
                                        database.
        topology_id (str):  The topology identification string.

    Returns:
        A list of component name strings.
    """

    in_out_comps: List[str] = (graph_client.graph_traversal.V()
                               .hasLabel("bolt").has("topology_id",
                                                     topology_id)
                               .inE("logically_connected")
                               .inV().as_("in_out")
                               .outE("logically_connected")
                               .select("in_out").by("component")
                               .dedup().toList())
    return in_out_comps


def lstsq_io_ratios(metrics_client: HeronMetricsClient,
                    graph_client: GremlinClient, topology_id: str,
                    cluster: str, environ: str,
                    start: dt.datetime, end: dt.datetime, bucket_length: int,
                    **kwargs: Union[str, int, float]) -> pd.DataFrame:
    """ This method will calculate the input/output ratio for each instance in
    the supplied topology using data aggregated from the defined period. The
    method uses least squares regression to calculate a coefficient for each
    input stream into a instance such that the total output amount for a given
    output stream is sum of all input stream arrival amounts times their
    coefficient.

    *NOTE*: This method assumes that there is an (approximately) linear
    relationship between the inputs and outputs of a given component.

    Arguments:
        metrics_client (HeronMetricsClient):    The client instance for the
                                                metrics database.
        graph_client (GremlinClient):   The client instance for the graph
                                        database.
        topology_id (str):  The topology identification string.
        start (dt.datetime):    The UTC datetime object for the start of the
                                metric gathering period.
        end (dt.datetime):  The UTC datetime object for the end of the metric
                            gathering period.
        bucket_length (int):    The length in seconds that the metrics should
                                be aggregated into. *NOTE*: For the least
                                squares regression to work the number of
                                buckets must exceed the highest number of input
                                streams into the component of the topology.
        **kwargs:   Additional keyword arguments that will be passed to the
                    metrics client object. Consult the documentation for the
                    specific metrics client beings used.
    Returns:
        pandas.DataFrame:   A DataFrame with the following columns:

        * task: Task ID integer.
        * output_stream: The output stream name.
        * input_stream: The input stream name.
        * source_component: The name of the source component for the input
          stream.
        * coefficient: The value of the input amount coefficient for this
          output stream, inputs stream source component combination.
    """

    LOG.info("Calculating instance input/output ratios using least squares "
             "regression for topology %s over a %d second window between %s "
             "and %s", topology_id, (end-start).total_seconds(),
             start.isoformat(), end.isoformat())

    emit_counts: pd.DataFrame = metrics_client.get_emit_counts(
        topology_id, cluster, environ, start, end, **kwargs)

    arrived_tuples: pd.DataFrame = metrics_client.get_tuple_arrivals_at_stmgr(
        topology_id, cluster, environ, start, end, **kwargs)

    execute_counts: pd.DataFrame = metrics_client.get_execute_counts(
        topology_id, cluster, environ, start, end, **kwargs)

    arrived_tuples = arrived_tuples.merge(execute_counts, on=["task", "component", "container", "timestamp"])

    arrived_tuples.drop("execute_count", axis=1, inplace=True)
    # Limit the count DataFrames to only those component with both incoming and
    # outgoing streams
    in_out_comps: List[str] = get_in_out_components(graph_client, topology_id)

    emit_counts = emit_counts[emit_counts["component"].isin(in_out_comps)]
    emit_counts.rename(index=str, columns={"stream": "outgoing_stream"},
                       inplace=True)

    arrived_tuples = arrived_tuples[arrived_tuples["component"]
                                    .isin(in_out_comps)]
    arrived_tuples.rename(index=str, columns={"stream": "incoming_stream"},
                          inplace=True)
    # Re-sample the counts into equal length time buckets and group by task id,
    # time bucket and stream. This aligns the two DataFrames with timestamps of
    # equal length and start point so they can be merged later
    emit_counts_ts: pd.DataFrame = \
        (emit_counts.set_index(["task", "timestamp"])
         .groupby([pd.Grouper(level="task"),
                   pd.Grouper(freq=f"{bucket_length}S", level='timestamp'),
                   "component", "outgoing_stream"])
         ["emit_count"]
         .sum().reset_index())

    arrived_tuples_ts: pd.DataFrame = \
        (arrived_tuples.set_index(["task", "timestamp"])
         .groupby([pd.Grouper(level="task"),
                   pd.Grouper(freq=f"{bucket_length}S", level='timestamp'),
                   "component", "incoming_stream", "source_component"])
         ["num-tuples"]
         .sum().reset_index())

    rows: List[Dict[str, Union[str, float]]] = []

    # Now we loop through each component and munge the data until we have an
    # output total for each output stream for each task on the same row (one
    # row per time bucket) as the input total for each input stream
    component: str
    in_data: pd.DataFrame
    for component, in_data in arrived_tuples_ts.groupby(["component"]):
        in_stream_counts: pd.DataFrame = \
            (in_data.set_index(["task", "timestamp", "incoming_stream",
                                "source_component"])
             ["num-tuples"].unstack(level=["incoming_stream",
                                           "source_component"])
             .reset_index())

        out_stream_counts: pd.DataFrame = \
            emit_counts_ts[emit_counts_ts.component == component]

        merged: pd.DataFrame = out_stream_counts.merge(in_stream_counts,
                                                       on=["task",
                                                           "timestamp"])
        task: int
        out_stream: str
        data: pd.DataFrame
        for (task, out_stream), data in merged.groupby(["task",
                                                        "outgoing_stream"]):

            LOG.debug("Processing instance %d output stream %s", task,
                      out_stream)

            # Get a series of the output counts for this output stream, these
            # are the dependent variables (b) of the least squares regression
            # a x = b
            output_counts: pd.DataFrame = data.emit_count

            # If this instance's component has output stream registered that
            # nothing else subscribes too then the emit count will be zero and
            # we can skip this output stream
            if output_counts.sum() <= 0.0:
                LOG.debug("No emissions from instance %d on stream %s, "
                          "skipping this stream...", task, out_stream)
                continue

            # Get just the input stream counts for each time bucket. This is
            # the coefficients matrix (a) of the least squares regression
            # a x = b
            cols: List[Tuple[str, str]] = data.columns[5:]
            input_counts: pd.DataFrame = data[cols]

            coeffs: List[float]
            coeffs, _, _, _ = np.linalg.lstsq(input_counts, output_counts,
                                              rcond=None)
            i: int
            in_stream: str
            source: str
            for i, (in_stream, source) in enumerate(cols):
                row: Dict[str, Union[str, float]] = {
                    "task": task,
                    "output_stream": out_stream,
                    "input_stream": in_stream,
                    "source_component": source,
                    "coefficient": coeffs[i]}
                rows.append(row)
    result = pd.DataFrame(rows)

    if result.empty:
        raise Exception("lstsq_io_ratios returns an empty dataframe")

    return result
