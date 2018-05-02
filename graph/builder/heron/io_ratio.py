""" This module contains methods for calculating the input output (I/O) ratio
for the instances of a given topology. """

import logging

import datetime as dt

from typing import Any, Dict, Union, List, DefaultDict, Tuple
from collections import defaultdict

import pandas as pd
import numpy as np

from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

def get_in_out_components(logical_plan: Dict[str, Any]) -> List[str]:
    """ Gets a list of components that have both incoming and outgoing streams.

    Arguments:
        logical_plan (dict):    The logical plan dictionary returned by the
                                Heron Tracker API.

    Returns:
        A list of component name strings.
    """

    return [comp_name for comp_name, comp_data in logical_plan["bolts"].items()
            if ("inputs" in comp_data) and ("outputs" in comp_data)]

def lstsq_io_ratios(metrics_client: HeronMetricsClient,
                    topology_id: str, start: dt.datetime, end: dt.datetime,
                    logical_plan: Dict[str, Any], bucket_length: int,
                    metric_kwargs: Dict[str, Union[str, int, float]] = None
                   ) -> DefaultDict[int, Dict[str,
                                              List[Dict[str,
                                                        Union[str, float]]]]]:
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
        topology_id (str):  The topology identification string.
        start (dt.datetime):    The UTC datetime object for the start of the
                                metric gathering period.
        end (dt.datetime):  The UTC datetime object for the end of the metric
                            gathering period.
        logical_plan (dict):    The logical plan for the topology, returned by
                                the Heron Tracker API.
        bucket_length (int):    The length in seconds that the metrics should
                                be aggregated into. *NOTE*: For the least
                                squares regression to work the number of
                                buckets must exceed the highest number of input
                                streams into the component of the topology.
        metric_kwargs (dict):   Optional dictionary containing additional
                                keyword arguments needed by the metrics client.

    Returns:
        A dictionary with the following structure:

        [task ID integer] -> [output stream name string] -> [List of input
        stream dictionaries]

        Each input stream dictionary contains keys for "stream" : input stream
        name, "source" : source component name, "coefficient" : the stream
        coefficient as a float.
    """

    LOG.info("Calculating instance input/output ratios using least squares "
             "regression for topology %s over a %d second window between %s "
             "and %s", topology_id, (end-start).total_seconds(),
             start.isoformat(), end.isoformat())

    emit_counts: pd.DataFrame = metrics_client.get_emit_counts(
        topology_id, start, end, **metric_kwargs)

    execute_counts: pd.DataFrame = metrics_client.get_execute_counts(
        topology_id, start, end, **metric_kwargs)

    # Limit the count DataFrames to only those component with both incoming and
    # outgoing streams
    in_out_comps: List[str] = get_in_out_components(logical_plan)

    emit_counts = emit_counts[emit_counts["component"].isin(in_out_comps)]
    emit_counts.rename(index=str, columns={"stream" : "outgoing_stream"},
                       inplace=True)

    execute_counts = execute_counts[execute_counts["component"]
                                    .isin(in_out_comps)]
    execute_counts.rename(index=str, columns={"stream" : "incoming_stream"},
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

    execute_counts_ts: pd.DataFrame = \
    (execute_counts.set_index(["task", "timestamp"])
     .groupby([pd.Grouper(level="task"),
               pd.Grouper(freq=f"{bucket_length}S", level='timestamp'),
               "component", "incoming_stream", "source_component"])
     ["execute_count"]
     .sum().reset_index())


    output: DefaultDict[int, Dict[str, List[Dict[str, Union[str, float]]]]] = \
            defaultdict(dict)

    # Now we loop through each component and munge the data until we have an
    # output total for each output stream for each task on the same row (one
    # row per time bucket) as the input total for each input stream
    component: str
    in_data: pd.DataFrame
    for component, in_data in execute_counts_ts.groupby(["component"]):
        in_stream_counts: pd.DataFrame = \
            (in_data.set_index(["task", "timestamp", "incoming_stream",
                                "source_component"])
             .execute_count.unstack(level=["incoming_stream",
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
            out_stream_coeffs: List[Dict[str, Union[str, float]]] = []
            i: int
            in_stream: str
            source: str
            for i, (in_stream, source) in enumerate(cols):
                out_stream_coeffs.append({"stream" : in_stream,
                                          "source" : source,
                                          "coefficient" : coeffs[i]})
            output[task][out_stream] = out_stream_coeffs

    return output
