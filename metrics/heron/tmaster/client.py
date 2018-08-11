# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains classes and methods for extracting metrics from the
Heron Topology Master instance. """

import logging
import warnings

import datetime as dt

from typing import Dict, List, Any, Callable, Union, Tuple, Optional

import pandas as pd

from requests.exceptions import HTTPError

from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.common.heron import tracker
from caladrius.config.keys import ConfKeys

LOG: logging.Logger = logging.getLogger(__name__)

# pylint: disable=too-many-locals, too-many-arguments

# Type definitions
ROW_DICT = Dict[str, Union[str, int, float, dt.datetime, None]]

# The TMaster metrics are aggregated into minute long periods by default
DEFAULT_METRIC_PERIOD: int = 60


def time_check(start: dt.datetime, end: dt.datetime,
               time_limit_hrs: float) -> None:
    """ Checks the time period, defined by the supplied start and end points,
    against the period defined from now back by the supplied time limit in
    hours. If the time check passes then nothing will be returned.

    Arguments:
        start (datetime):    The start of the time period. Should be UTC.
        end (datetime):  The end of the time period. Should be UTC.
        time_limit_hrs (float): The number of hours back from now that define
                                the allowed time period.

    Raises:
        RuntimeError:   If the supplied time period is not within the defined
                        limit or if the end time is before the start time.
        RuntimeWarning: If the supplied time period crosses the limits of the
                        metrics storage period.
    """

    if end < start:
        msg: str = (f"The supplied end time ({end.isoformat}) is before the "
                    f"supplied start time ({start.isoformat}). No data will "
                    f"be returned.")
        LOG.error(msg)
        raise RuntimeError(msg)

    now: dt.datetime = dt.datetime.now(dt.timezone.utc)
    limit: dt.datetime = now - dt.timedelta(hours=time_limit_hrs)

    if start < limit and end < limit:
        limit_msg: str = (f"The defined time period ({start.isoformat()} to "
                          f"{end.isoformat()}) is outside of the "
                          f"{time_limit_hrs} hours of data stored by the "
                          f"Topology Master. No data will be returned.")
        LOG.error(limit_msg)
        raise RuntimeError(limit_msg)

    if start < limit and end > limit:
        truncated_duration: float = round(((end - limit).total_seconds() /
                                           3600), 2)
        truncated_msg: str = (f"The start ({start.isoformat()}) of the "
                              f"supplied time window is beyond the "
                              f"{time_limit_hrs} hours stored by the Topology "
                              f"Master. Results will be limited to "
                              f"{truncated_duration} hours from "
                              f"{limit.isoformat()} to {end.isoformat()}")
        LOG.warning(truncated_msg)
        warnings.warn(truncated_msg, RuntimeWarning)


def instance_timelines_to_dataframe(
        instance_timelines: dict, stream: Optional[str], measurement_name: str,
        conversion_func: Callable[[str], Union[str, int, float]] = None,
        source_component: str = None) -> pd.DataFrame:
    """ Converts the timeline dictionaries of a *single metric* into a single
    combined DataFrame for all instances. All timestamps are converted to UTC
    Python datetime objects and the returned DataFrame (for each instance) is
    sorted by ascending date.

    Arguments:
        instance_timelines (dict):  A dictionary of instance metric timelines,
                                    where each key is an instance name linking
                                    to a dictionary of <timestamp> :
                                    <measurement> pairs.
        stream (str):   The stream name that these metrics are related to.
        measurement_name (str): The name of the measurements being processed.
                                This will be used as the measurement column
                                heading.
        conversion_func (function): An optional function for converting the
                                    measurement in the timeline. If not
                                    supplied the measurement will be left as a
                                    string.

    Returns:
        pandas.DataFrame: A DataFrame containing the timelines of all instances
        in the supplied dictionary.
    """

    output: List[ROW_DICT] = []

    instance_name: str
    timeline: Dict[str, str]
    for instance_name, timeline in instance_timelines.items():

        details = tracker.parse_instance_name(instance_name)
        instance_list: List[ROW_DICT] = []

        timestamp_str: str
        measurement_str: str
        for timestamp_str, measurement_str in timeline.items():

            timestamp: dt.datetime = \
                    dt.datetime.utcfromtimestamp(int(timestamp_str))

            if "nan" in measurement_str:
                measurement: Union[str, int, float, None] = None
            else:
                if conversion_func:
                    measurement = conversion_func(measurement_str)
                else:
                    measurement = measurement_str

            row: ROW_DICT = {
                "timestamp": timestamp,
                "container": details["container"],
                "task": details["task_id"],
                "component": details["component"],
                measurement_name: measurement}

            if stream:
                row["stream"] = stream

            if source_component:
                row["source_component"] = source_component

            instance_list.append(row)

        # Because the original dict returned by the tracker is
        # unsorted we need to sort the rows by ascending time
        instance_list.sort(
            key=lambda instance: instance["timestamp"])

        output.extend(instance_list)

    return pd.DataFrame(output)


def str_nano_to_float_milli(nano_str: str) -> float:
    """ Converts a string of a nano measurement into a millisecond float value.
    """

    return float(nano_str) / 1000000.0


class HeronTMasterClient(HeronMetricsClient):
    """ Class for extracting metrics from the Heron Topology Master metrics
    store. """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.tracker_url = config[ConfKeys.HERON_TRACKER_URL.value]
        self.time_limit_hrs = \
            config.get(ConfKeys.HERON_TMASTER_METRICS_MAX_HOURS.value, 3)

        LOG.info("Created Topology Master metrics client using Heron Tracker "
                 "at: %s", self.tracker_url)

    def __hash__(self) -> int:

        return hash(self.tracker_url)

    def __eq__(self, other: object) -> bool:

        if not isinstance(other, HeronTMasterClient):
            return False

        if self.tracker_url == other.tracker_url:
            return True

        return False

    def _query_setup(self, topology_id: str, cluster: str, environ: str,
                     start: dt.datetime, end: dt.datetime,
                     ) -> Tuple[Dict[str, Any], int, int]:
        """ Helper method for setting up each of the query methods with the
        required variables."""

        time_check(start, end, self.time_limit_hrs)

        start_time: int = int(round(start.timestamp()))
        end_time: int = int(round(end.timestamp()))

        logical_plan: Dict[str, Any] = tracker.get_logical_plan(
            self.tracker_url, cluster, environ, topology_id)

        return logical_plan, start_time, end_time

    def get_component_service_times(self, topology_id: str, cluster: str,
                                    environ: str, component_name: str,
                                    start: int, end: int, logical_plan:
                                    Dict[str, Any]=None) -> pd.DataFrame:
        """ Gets the service times, as a timeseries, for every instance of the
        specified component of the specified topology. The start and end times
        define the window over which to gather the metrics. The window duration
        should be less then 3 hours as this is the limit of what the Topology
        master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            cluster (str):  The cluster the topology is running in.
            environ (str):  The environment the topology is running in (eg.
                            prod, devel, test, etc).
            component_name (str):   The name of the component whose metrics are
                                    required.
            start (int):    Start time for the time period the query is run
                            against. This should be a UTC POSIX time integer
                            (seconds since epoch).
            end (int):  End time for the time period the query is run against.
                        This should be a UTC POSIX time integer (seconds since
                        epoch).
            logical_plan (dict):    Optional dictionary logical plan returned
                                    by the Heron Tracker API. If not supplied
                                    this method will call the API to get the
                                    logical plan.

        Returns:
            pandas.DataFrame: A DataFrame containing the service time
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp: The UTC timestamp for the metric time period,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from,
            * stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * execute-latency-ms: The average execute latency measurement in
              milliseconds for that metric time period.
        """

        LOG.info("Getting service time metrics for component %s of topology "
                 "%s", component_name, topology_id)

        if not logical_plan:
            LOG.debug("Logical plan not supplied, fetching from Heron Tracker")
            logical_plan = tracker.get_logical_plan(self.tracker_url, cluster,
                                                    environ, topology_id)

        incoming_streams: List[Tuple[str, str]] = \
            tracker.incoming_sources_and_streams(logical_plan, component_name)

        metrics: List[str] = ["__execute-latency/" + source + "/" + stream
                              for source, stream in incoming_streams]

        results: Dict[str, Any] = tracker.get_metrics_timeline(
            self.tracker_url, cluster, environ, topology_id, component_name,
            start, end, metrics)

        output: pd.DataFrame = None

        for stream_metric, instance_timelines in results["timeline"].items():
            metric_list: List[str] = stream_metric.split("/")
            incoming_source: str = metric_list[1]
            incoming_stream: str = metric_list[2]

            instance_tls_df: pd.DataFrame = instance_timelines_to_dataframe(
                instance_timelines, incoming_stream, "latency_ms",
                str_nano_to_float_milli, incoming_source)

            if output is None:
                output = instance_tls_df
            else:
                output = output.append(instance_tls_df, ignore_index=True)

        return output

    def get_service_times(self, topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime,
                          **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the service times, as a timeseries, for every instance of the
        of all the bolt components of the specified topology. The start and end
        times define the window over which to gather the metrics. The window
        duration should be less than 3 hours as this is the limit of what the
        Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            start (datetime):    utc datetime instance for the start of the
                                    metrics gathering period.
            end (datetime):  utc datetime instance for the end of the
                                metrics gathering period.
            **cluster (str):  The cluster the topology is running in.
            **environ (str):  The environment the topology is running in (eg.
                              prod, devel, test, etc).

        Returns:
            pandas.DataFrame: A DataFrame containing the service time
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp:The UTC timestamp for the metric time period,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from,
            * stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * latency_ms: The average execute latency measurement in
              milliseconds for that metric time period.
        """

        LOG.info("Getting service times for topology %s over a %d second "
                 "period from %s to %s", topology_id,
                 (end-start).total_seconds(), start.isoformat(),
                 end.isoformat())

        logical_plan, start_time, end_time = self._query_setup(
            topology_id, cluster, environ, start, end)

        output: pd.DataFrame = None

        bolts: Dict[str, Any] = logical_plan["bolts"]
        bolt_component: str
        for bolt_component in bolts:

            try:
                bolt_service_times: pd.DataFrame = \
                        self.get_component_service_times(topology_id,
                                                         cluster, environ,
                                                         bolt_component,
                                                         start_time, end_time,
                                                         logical_plan)
            except HTTPError as http_error:
                LOG.warning("Fetching execute latencies  for component %s "
                            "failed with status code %s", bolt_component,
                            str(http_error.response.status_code))
            else:
                if output is None:
                    output = bolt_service_times
                else:
                    output = output.append(bolt_service_times,
                                           ignore_index=True)

        return output

    def get_component_emission_counts(self, topology_id: str, cluster: str,
                                      environ: str, component_name: str,
                                      start: int, end: int,
                                      logical_plan: Dict[str, Any] = None
                                      ) -> pd.DataFrame:
        """ Gets the emit counts, as a timeseries, for every instance of the
        specified component of the specified topology. The start and end times
        define the window over which to gather the metrics. The window duration
        should be less then 3 hours as this is the limit of what the Topology
        master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            cluster (str):  The cluster the topology is running in.
            environ (str):  The environment the topology is running in (eg.
                            prod, devel, test, etc).
            component_name (str):   The name of the component whose metrics are
                                    required.
            start (int):    Start time for the time period the query is run
                            against. This should be a UTC POSIX time integer
                            (seconds since epoch).
            end (int):  End time for the time period the query is run against.
                        This should be a UTC POSIX time integer (seconds since
                        epoch).
            logical_plan (dict):    Optional dictionary logical plan returned
                                    by the Heron Tracker API. If not supplied
                                    this method will call the API to get the
                                    logical plan.

        Returns:
            pandas.DataFrame: A DataFrame containing the emit count
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp:The UTC timestamp for the metric time period,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from,
            * stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * emit_count: The emit count in that metric time period.
        """

        LOG.info("Getting emit count metrics for component %s of topology "
                 "%s", component_name, topology_id)

        if not logical_plan:
            LOG.debug("Logical plan not supplied, fetching from Heron Tracker")
            logical_plan = tracker.get_logical_plan(self.tracker_url, cluster,
                                                    environ, topology_id)

        outgoing_streams: List[str] = tracker.get_outgoing_streams(
            logical_plan, component_name)

        metrics: List[str] = ["__emit-count/" + stream
                              for stream in outgoing_streams]

        results: Dict[str, Any] = tracker.get_metrics_timeline(
            self.tracker_url, cluster, environ, topology_id, component_name,
            start, end, metrics)

        output: pd.DataFrame = None

        for stream_metric, instance_timelines in results["timeline"].items():
            outgoing_stream: str = stream_metric.split("/")[-1]

            instance_tls_df: pd.DataFrame = instance_timelines_to_dataframe(
                instance_timelines, outgoing_stream, "emit_count",
                lambda m: int(float(m)))

            if output is None:
                output = instance_tls_df
            else:
                output = output.append(instance_tls_df, ignore_index=True)

        return output

    def get_emit_counts(self, topology_id: str, cluster: str, environ: str,
                        start: dt.datetime, end: dt.datetime,
                        **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the emit counts, as a timeseries, for every instance of each
        of the components of the specified topology. The start and end times
        define the window over which to gather the metrics. The window duration
        should be less than 3 hours as this is the limit of what the Topology
        master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            start (datetime):    utc datetime instance for the start of the
                                    metrics gathering period.
            end (datetime):  utc datetime instance for the end of the
                                metrics gathering period.
            **cluster (str):  The cluster the topology is running in.
            **environ (str):  The environment the topology is running in (eg.
                              prod, devel, test, etc).

        Returns:
            pandas.DataFrame:   A DataFrame containing the emit count
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp: The UTC timestamp for the metric,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from,
            * stream: The name of the outing stream from which the tuples that
              lead to this metric came from,
            * emit_count: The emit count during the metric time period.
        """
        LOG.info("Getting emit counts for topology %s over a %d second "
                 "period from %s to %s", topology_id,
                 (end-start).total_seconds(), start.isoformat(),
                 end.isoformat())

        logical_plan, start_time, end_time = self._query_setup(
            topology_id, cluster, environ, start, end)

        output: pd.DataFrame = None

        components: List[str] = (list(logical_plan["spouts"].keys()) +
                                 list(logical_plan["bolts"].keys()))

        for component in components:

            try:
                comp_emit_counts: pd.DataFrame = \
                        self.get_component_emission_counts(
                            topology_id, cluster, environ, component,
                            start_time, end_time, logical_plan)
            except HTTPError as http_error:
                LOG.warning("Fetching emit counts for component %s failed with"
                            " status code %s", component,
                            str(http_error.response.status_code))

            if output is None:
                output = comp_emit_counts
            else:
                output = output.append(comp_emit_counts, ignore_index=True)

        return output

    def get_component_execute_counts(self, topology_id: str, cluster: str,
                                     environ: str, component_name: str,
                                     start: int, end: int,
                                     logical_plan: Dict[str, Any] = None
                                     ) -> pd.DataFrame:
        """ Gets the execute counts, as a timeseries, for every instance of the
        specified component of the specified topology. The start and end times
        define the window over which to gather the metrics. The window duration
        should be less then 3 hours as this is the limit of what the Topology
        master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            cluster (str):  The cluster the topology is running in.
            environ (str):  The environment the topology is running in (eg.
                            prod, devel, test, etc).
            component_name (str):   The name of the component whose metrics are
                                    required.
            start (int):    Start time for the time period the query is run
                            against. This should be a UTC POSIX time integer
                            (seconds since epoch).
            end (int):  End time for the time period the query is run against.
                        This should be a UTC POSIX time integer (seconds since
                        epoch).
            logical_plan (dict):    Optional dictionary logical plan returned
                                    by the Heron Tracker API. If not supplied
                                    this method will call the API to get the
                                    logical plan.

        Returns:
            pandas.DataFrame: A DataFrame containing the emit count
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp: The UTC timestamp for the metric time period,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from.
            * stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * execute_count: The execute count in that metric time period.
        """

        LOG.info("Getting execute count metrics for component %s of topology "
                 "%s", component_name, topology_id)

        if not logical_plan:
            LOG.debug("Logical plan not supplied, fetching from Heron Tracker")
            logical_plan = tracker.get_logical_plan(self.tracker_url, cluster,
                                                    environ, topology_id)

        incoming_streams: List[Tuple[str, str]] = \
            tracker.incoming_sources_and_streams(logical_plan, component_name)

        metrics: List[str] = ["__execute-count/" + source + "/" + stream
                              for source, stream in incoming_streams]

        results: Dict[str, Any] = tracker.get_metrics_timeline(
            self.tracker_url, cluster, environ, topology_id, component_name,
            start, end, metrics)

        output: pd.DataFrame = None

        for stream_metric, instance_timelines in results["timeline"].items():
            metric_list: List[str] = stream_metric.split("/")
            incoming_source: str = metric_list[1]
            incoming_stream: str = metric_list[2]

            instance_tls_df: pd.DataFrame = instance_timelines_to_dataframe(
                instance_timelines, incoming_stream, "execute_count",
                lambda m: int(float(m)), incoming_source)

            if output is None:
                output = instance_tls_df
            else:
                output = output.append(instance_tls_df, ignore_index=True)

        return output

    def get_execute_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the execute counts, as a timeseries, for every instance of
        each of the components of the specified topology. The start and end
        times define the window over which to gather the metrics. The window
        duration should be less than 3 hours as this is the limit of what the
        Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            start (datetime):    UTC datetime instance for the start of the
                                    metrics gathering period.
            end (datetime):  UTC datetime instance for the end of the
                                metrics gathering period.
            **cluster (str):  The cluster the topology is running in.
            **environ (str):  The environment the topology is running in (eg.
                              prod, devel, test, etc).

        Returns:
            pandas.DataFrame:   A DataFrame containing the service time
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp: The UTC timestamp for the metric,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from.
            * stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * source_component: The name of the component the stream's source
              instance belongs to,
            * execute_count: The execute count during the metric time period.
        """
        LOG.info("Getting execute counts for topology %s over a %d second "
                 "period from %s to %s", topology_id,
                 (end-start).total_seconds(), start.isoformat(),
                 end.isoformat())

        logical_plan, start_time, end_time = self._query_setup(
            topology_id, cluster, environ, start, end)

        output: pd.DataFrame = None

        for component in logical_plan["bolts"].keys():

            try:
                comp_execute_counts: pd.DataFrame = \
                        self.get_component_execute_counts(topology_id, cluster,
                                                          environ, component,
                                                          start_time, end_time,
                                                          logical_plan)
            except HTTPError as http_error:
                LOG.warning("Fetching execute counts for component %s failed "
                            "with status code %s", component,
                            str(http_error.response.status_code))

            if output is None:
                output = comp_execute_counts
            else:
                output = output.append(comp_execute_counts, ignore_index=True)

        return output

    def get_spout_complete_latencies(self, topology_id: str, cluster: str,
                                     environ: str, component_name: str,
                                     start: int, end: int,
                                     logical_plan: Dict[str, Any] = None
                                     ) -> pd.DataFrame:
        """ Gets the complete latency, as a timeseries, for every instance of
        the specified component of the specified topology. The start and end
        times define the window over which to gather the metrics. The window
        duration should be less then 3 hours as this is the limit of what the
        Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            cluster (str):  The cluster the topology is running in.
            environ (str):  The environment the topology is running in (eg.
                            prod, devel, test, etc).
            component_name (str):   The name of the spout component whose
                                    metrics are required.
            start (int):    Start time for the time period the query is run
                            against. This should be a UTC POSIX time integer
                            (seconds since epoch).
            end (int):  End time for the time period the query is run against.
                        This should be a UTC POSIX time integer (seconds since
                        epoch).
            logical_plan (dict):    Optional dictionary logical plan returned
                                    by the Heron Tracker API. If not supplied
                                    this method will call the API to get the
                                    logical plan.

        Returns:
            pandas.DataFrame:   A DataFrame containing the complete latency
            measurements as a timeseries. Each row represents a measurement
            (averaged over one minute) with the following columns:

            * timestamp:  The UTC timestamp for the metric,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container:  The ID for the container this metric comes from,
            * stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * latency_ms: The average execute latency measurement in
              milliseconds for that metric time period.
        """

        LOG.info("Getting complete latency metrics for component %s of "
                 "topology %s", component_name, topology_id)

        if not logical_plan:
            LOG.debug("Logical plan not supplied, fetching from Heron Tracker")
            logical_plan = tracker.get_logical_plan(self.tracker_url, cluster,
                                                    environ, topology_id)

        outgoing_streams: List[str] = \
            tracker.get_outgoing_streams(logical_plan, component_name)

        metrics: List[str] = ["__complete-latency/" + stream
                              for stream in outgoing_streams]

        results: Dict[str, Any] = tracker.get_metrics_timeline(
            self.tracker_url, cluster, environ, topology_id, component_name,
            start, end, metrics)

        output: pd.DataFrame = None

        for stream_metric, instance_timelines in results["timeline"].items():
            metric_list: List[str] = stream_metric.split("/")
            outgoing_stream: str = metric_list[1]

            instance_tls_df: pd.DataFrame = instance_timelines_to_dataframe(
                instance_timelines, outgoing_stream, "latency_ms",
                str_nano_to_float_milli)

            if output is None:
                output = instance_tls_df
            else:
                output = output.append(instance_tls_df, ignore_index=True)

        return output

    def get_complete_latencies(self, topology_id: str, cluster: str,
                               environ: str, start: dt.datetime,
                               end: dt.datetime,
                               **kwargs: Union[str, int, float]
                               ) -> pd.DataFrame:
        """ Gets the complete latencies, as a timeseries, for every instance of
        the of all the spout components of the specified topology. The start
        and end times define the window over which to gather the metrics. The
        window duration should be less than 3 hours as this is the limit of
        what the Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            cluster (str):  The cluster the topology is running in.
            environ (str):  The environment the topology is running in (eg.
                            prod, devel, test, etc).
            start (datetime):    utc datetime instance for the start of the
                                    metrics gathering period.
            end (datetime):  utc datetime instance for the end of the
                                metrics gathering period.

        Returns:
            pandas.DataFrame: A DataFrame containing the service time
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp:  The UTC timestamp for the metric,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container:  The ID for the container this metric comes from,
              stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * latency_ms: The average execute latency measurement in
              milliseconds for that metric time period.

        Raises:
            RuntimeWarning: If the specified topology has a reliability mode
                            that does not enable complete latency.
        """
        LOG.info("Getting complete latencies for topology %s over a %d second "
                 "period from %s to %s", topology_id,
                 (end-start).total_seconds(), start.isoformat(),
                 end.isoformat())

        logical_plan, start_time, end_time = self._query_setup(
            topology_id, cluster, environ, start, end)

        # First we need to check that the supplied topology will actually have
        # complete latencies. Only ATLEAST_ONCE and EXACTLY_ONCE will have
        # complete latency values as acking is disabled for ATMOST_ONCE.
        physical_plan: Dict[str, Any] = tracker.get_physical_plan(
            self.tracker_url, cluster, environ, topology_id)
        if (physical_plan["config"]
                ["topology.reliability.mode"] == "ATMOST_ONCE"):
            rm_msg: str = (f"Topology {topology_id} reliability mode is set "
                           f"to ATMOST_ONCE. Complete latency is not "
                           f"available for these types of topologies")
            LOG.warning(rm_msg)
            warnings.warn(rm_msg, RuntimeWarning)
            return pd.DataFrame()

        output: pd.DataFrame = None

        spouts: Dict[str, Any] = logical_plan["spouts"]
        for spout_component in spouts:

            try:
                spout_complete_latencies: pd.DataFrame = \
                        self.get_spout_complete_latencies(topology_id,
                                                          cluster, environ,
                                                          spout_component,
                                                          start_time, end_time,
                                                          logical_plan)
            except HTTPError as http_error:
                LOG.warning("Fetching execute latencies  for component %s "
                            "failed with status code %s", spout_component,
                            str(http_error.response.status_code))

            if output is None:
                output = spout_complete_latencies
            else:
                output = output.append(spout_complete_latencies,
                                       ignore_index=True)

        return output

    def get_calculated_arrival_rates(self, topology_id: str, cluster: str, environ: str,
                                     start: dt.datetime, end: dt.datetime,
                                     **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the arrival rates, as a timeseries, for every instance of each
        of the bolt components of the specified topology. The start and end
        times define the window over which to gather the metrics. The window
        duration should be less than 3 hours as this is the limit of what the
        Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            start (datetime):    utc datetime instance for the start of the
                                    metrics gathering period.
            end (datetime):  utc datetime instance for the end of the
                                metrics gathering period.
            **cluster (str):  The cluster the topology is running in.
            **environ (str):  The environment the topology is running in (eg.
                              prod, devel, test, etc).

        Returns:
            pandas.DataFrame:   A DataFrame containing the arrival rate
            measurements as a timeseries. Each row represents a measurement
            (aggregated over one minute) with the following columns:

            * timestamp: The UTC timestamp for the metric,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from,
            * arrival_count: The number of arrivals (across all streams) at
              each instance.
            * arrival_rate_tps: The arrival rate at each instance (across all
              streams) in units of tuples per second.
        """
        LOG.info("Getting arrival rates for topology %s over a %d second "
                 "period from %s to %s", topology_id,
                 (end-start).total_seconds(), start.isoformat(),
                 end.isoformat())

        execute_counts: pd.DataFrame = self.get_execute_counts(
            topology_id, cluster, environ, start, end)

        arrivals: pd.DataFrame = \
            (execute_counts.groupby(["task", "component", "timestamp"])
             .sum().reset_index()
             .rename(index=str, columns={"execute_count": "arrival_count"}))

        arrivals["arrival_rate_tps"] = (arrivals["arrival_count"] /
                                        DEFAULT_METRIC_PERIOD)

        return arrivals

    def get_receive_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = ("The custom caladrius receive-count metrics is not yet "
                    "available via the TMaster metrics database")
        LOG.error(msg)
        raise NotImplementedError(msg)

    def get_incoming_queue_sizes(self, topology_id: str, cluster: str, environ: str,
                                 start: [dt.datetime] = None, end: [dt.datetime] = None,
                                 **kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = "Unimplemented"
        LOG.error(msg)
        raise NotImplementedError(msg)

    def get_cpu_load(self, topology_id: str, cluster: str, environ: str,
                     start: [dt.datetime] = None, end: [dt.datetime] = None,
                     ** kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = "Unimplemented"
        LOG.error(msg)
        raise NotImplementedError(msg)

    def get_gc_time(self, topology_id: str, cluster: str, environ: str,
                    start: [dt.datetime] = None, end: [dt.datetime] = None,
                    ** kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = "Unimplemented"
        LOG.error(msg)
        raise NotImplementedError(msg)

    def get_num_packets_received(self, topology_id: str, cluster: str, environ: str,
                    start: [dt.datetime] = None, end: [dt.datetime] = None,
                    ** kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = "Unimplemented"
        LOG.error(msg)
        raise NotImplementedError(msg)

    def get_packet_arrival_rate(self, topology_id: str, cluster: str, environ: str,
                                start: [dt.datetime] = None, end: [dt.datetime] = None,
                                **kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = "Unimplemented"
        LOG.error(msg)
        raise NotImplementedError(msg)

    def get_tuple_arrivals_at_stmgr(self, topology_id: str, cluster: str, environ: str,
                                    start: [dt.datetime] = None, end: [dt.datetime] = None,
                                    **kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = "Unimplemented"
        LOG.error(msg)
        raise NotImplementedError(msg)
