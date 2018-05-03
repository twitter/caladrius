""" This module contains classes and methods for extracting metrics from the
Heron Topology Master instance. """

import logging

import datetime as dt

from typing import Dict, List, Any, Callable, Union, Tuple, cast

import pandas as pd

from requests.exceptions import HTTPError

from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.common.heron import tracker
from caladrius.config.keys import ConfKeys

LOG: logging.Logger = logging.getLogger(__name__)

# pylint: disable=too-many-locals, too-many-arguments

def instance_timelines_to_dataframe(
        instance_timelines: dict, stream: str, measurement_name: str,
        conversion_func: Callable[[str], Any] = None,
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
        A pandas DataFrame containing the timelines of all instances in the
        supplied dictionary.
    """

    row_dict = Dict[str, Union[str, int, float, dt.datetime]]

    output: List[row_dict] = []

    for instance_name, timeline in instance_timelines.items():

        details = tracker.parse_instance_name(instance_name)
        instance_list: List[row_dict] = []

        for timestamp_str, measurement_str in timeline.items():

            timestamp: dt.datetime = \
                    dt.datetime.utcfromtimestamp(int(timestamp_str))

            if "nan" in measurement_str:
                measurement: Any = None
            else:
                measurement = conversion_func(measurement_str)

            row: row_dict = {
                "timestamp" : timestamp,
                "container" : details["container"],
                "task" : details["task_id"],
                "component" : details["component"],
                "stream" : stream,
                measurement_name : measurement}

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

        LOG.info("Created Topology Master metrics client using Heron Tracker "
                 "at: %s", self.tracker_url)


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
            A pandas DataFrame containing the service time measurements as a
            timeseries. Each row represents a measurement (aggregated over one
            minuet) with the following columns:
            timestamp:  The UTC timestamp for the metric.
            component: The component this metric comes from.
            task:   The instance ID number for the instance that the metric
                    comes from.
            container:  The ID for the container this metric comes from.
            stream: The name of the incoming stream from which the tuples
                    that lead to this metric came from.
            execute-latency-ms: The average execute latency measurement in
                                milliseconds for that metric time period.
        """

        LOG.info("Getting service time metrics for component %s of topology "
                 "%s between %s and %s", component_name, topology_id,
                 dt.datetime.utcfromtimestamp(start).isoformat(),
                 dt.datetime.utcfromtimestamp(end).isoformat())

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

    def get_service_times(self, topology_id: str, start: dt.datetime,
                          end: dt.datetime,
                          **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the service times, as a timeseries, for every instance of the
        of all the bolt components of the specified topology. The start and end
        times define the window over which to gather the metrics. The window
        duration should be less than 3 hours as this is the limit of what the
        Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            start (dt.datetime):    utc datetime instance for the start of the
                                    metrics gathering period.
            end (dt.datetime):  utc datetime instance for the end of the
                                metrics gathering period.
            **cluster (str):  The cluster the topology is running in.
            **environ (str):  The environment the topology is running in (eg.
                              prod, devel, test, etc).

        Returns:
            A pandas DataFrame containing the service time measurements as a
            timeseries. Each row represents a measurement (aggregated over one
            minuet) with the following columns:
            timestamp:  The UTC timestamp for the metric.
            component: The component this metric comes from.
            task:   The instance ID number for the instance that the metric
                    comes from.
            container:  The ID for the container this metric comes from.
            stream: The name of the incoming stream from which the tuples
                    that lead to this metric came from.
            latency_ms: The average execute latency measurement in milliseconds
                        for that metric time period.
        """

        cluster: str = cast(str, kwargs["cluster"])
        environ: str = cast(str, kwargs["environ"])

        logical_plan: Dict[str, Any] = tracker.get_logical_plan(
            self.tracker_url, cluster, environ, topology_id)

        if (end-start) > dt.timedelta(hours=3):
            LOG.warning("The specified metrics gathering window is greater "
                        "than the 3 hours that the TMaster stores. Results "
                        "will be limited to the last 3 hours of data.")

        start_time: int = int(round(start.timestamp()))
        end_time: int = int(round(end.timestamp()))

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

            if output is None:
                output = bolt_service_times
            else:
                output = output.append(bolt_service_times, ignore_index=True)

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
            A pandas DataFrame containing the emit count measurements as a
            timeseries. Each row represents a measurement (aggregated over one
            minuet) with the following columns:
                timestamp:  The UTC timestamp for the metric time period.
                component: The component this metric comes from.
                task:   The instance ID number for the instance that the metric
                        comes from.
                container:  The ID for the container this metric comes from.
                stream: The name of the incoming stream from which the tuples
                        that lead to this metric came from.
                emit_count: The emit count in that metric time period
        """

        LOG.info("Getting emission metrics for component %s of topology "
                 "%s between %s and %s", component_name, topology_id,
                 dt.datetime.utcfromtimestamp(start).isoformat(),
                 dt.datetime.utcfromtimestamp(end).isoformat())

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

    def get_emit_counts(self, topology_id: str, start: dt.datetime,
                        end: dt.datetime,
                        **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the emit counts, as a timeseries, for every instance of each
        of the components of the specified topology. The start and end times
        define the window over which to gather the metrics. The window duration
        should be less than 3 hours as this is the limit of what the Topology
        master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            start (dt.datetime):    utc datetime instance for the start of the
                                    metrics gathering period.
            end (dt.datetime):  utc datetime instance for the end of the
                                metrics gathering period.
            **cluster (str):  The cluster the topology is running in.
            **environ (str):  The environment the topology is running in (eg.
                              prod, devel, test, etc).

        Returns:
            A pandas DataFrame containing the service time measurements as a
            timeseries. Each row represents a measurement (aggregated over one
            minuet) with the following columns:
            timestamp:  The UTC timestamp for the metric.
            component: The component this metric comes from.
            task:   The instance ID number for the instance that the metric
                    comes from.
            container:  The ID for the container this metric comes from.
            stream: The name of the outing stream from which the tuples that
                     lead to this metric came from.
            emit_count: The emit count during the metric time period.
        """

        cluster: str = cast(str, kwargs["cluster"])
        environ: str = cast(str, kwargs["environ"])

        logical_plan: Dict[str, Any] = tracker.get_logical_plan(
            self.tracker_url, cluster, environ, topology_id)

        output: pd.DataFrame = None

        components: List[str] = (list(logical_plan["spouts"].keys()) +
                                 list(logical_plan["bolts"].keys()))

        if (end-start) > dt.timedelta(hours=3):
            LOG.warning("The specified metrics gathering window is greater "
                        "than the 3 hours that the TMaster stores. Results "
                        "will be limited to the last 3 hours of data.")

        start_time: int = int(round(start.timestamp()))
        end_time: int = int(round(end.timestamp()))

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
            A pandas DataFrame containing the emit count measurements as a
            timeseries. Each row represents a measurement (aggregated over one
            minuet) with the following columns:
                timestamp:  The UTC timestamp for the metric time period.
                component: The component this metric comes from.
                task:   The instance ID number for the instance that the metric
                        comes from.
                container:  The ID for the container this metric comes from.
                stream: The name of the incoming stream from which the tuples
                        that lead to this metric came from.
                execute_count: The execute count in that metric time period
        """

        LOG.info("Getting execute count metrics for component %s of topology "
                 "%s between %s and %s", component_name, topology_id,
                 dt.datetime.utcfromtimestamp(start).isoformat(),
                 dt.datetime.utcfromtimestamp(end).isoformat())

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

    def get_execute_counts(self, topology_id: str, start: dt.datetime,
                           end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the execute counts, as a timeseries, for every instance of
        each of the components of the specified topology. The start and end
        times define the window over which to gather the metrics. The window
        duration should be less than 3 hours as this is the limit of what the
        Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            start (dt.datetime):    UTC datetime instance for the start of the
                                    metrics gathering period.
            end (dt.datetime):  UTC datetime instance for the end of the
                                metrics gathering period.
            **cluster (str):  The cluster the topology is running in.
            **environ (str):  The environment the topology is running in (eg.
                              prod, devel, test, etc).

        Returns:
            A pandas DataFrame containing the service time measurements as a
            timeseries. Each row represents a measurement (aggregated over one
            minuet) with the following columns:
            timestamp:  The UTC timestamp for the metric.
            component: The component this metric comes from.
            task:   The instance ID number for the instance that the metric
                    comes from.
            container:  The ID for the container this metric comes from.
            stream: The name of the incoming stream from which the tuples
                    that lead to this metric came from.
            execute_count: The execute count during the metric time period.
        """

        cluster: str = cast(str, kwargs["cluster"])
        environ: str = cast(str, kwargs["environ"])

        logical_plan: Dict[str, Any] = tracker.get_logical_plan(
            self.tracker_url, cluster, environ, topology_id)

        if (end-start) > dt.timedelta(hours=3):
            LOG.warning("The specified metrics gathering window is greater "
                        "than the 3 hours that the TMaster stores. Results "
                        "will be limited to the last 3 hours of data.")

        start_time: int = int(round(start.timestamp()))
        end_time: int = int(round(end.timestamp()))

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

    def get_receive_counts(self, topology_id: str, start: dt.datetime,
                           end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        msg: str = ("The custom caladrius receive-count metrics is not yet "
                    "available via the TMaster metrics database")
        LOG.error(msg)
        raise NotImplementedError(msg)
