""" This module contains classes and methods for extracting metrics from the
Heron Topology Master instance. """

import logging

import datetime as dt

from typing import Dict, List, Any

import requests

import pandas as pd

from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.common.heron import tracker

LOG: logging.Logger = logging.getLogger(__name__)

def get_incoming_streams(logical_plan: Dict[str, Any], component_name: str):
    """ Gets a list of input stream names for the supplied component in the
    supplied logical plan. """

    return [input_stream["stream_name"]
            for input_stream
            in logical_plan["bolts"][component_name]["inputs"]]


class HeronTMasterClient(HeronMetricsClient):
    """ Class for extracting metrics from the Heron Topology Master metrics
    store. """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.tracker_url = config["heron.tracker.url"]

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
                latency_ms: The execute latency measurement in milliseconds.
        """

        LOG.info("Getting service time metrics for component %s of topology "
                 "%s between %s and %s", component_name, topology_id,
                 dt.datetime.utcfromtimestamp(start).isoformat(),
                 dt.datetime.utcfromtimestamp(end).isoformat())

        if not logical_plan:
            LOG.debug("Logical plan not supplied, fetching from Heron Tracker")
            logical_plan = tracker.get_logical_plan(self.tracker_url, cluster,
                                                    environ, topology_id)

        incoming_streams: List[str] = get_incoming_streams(logical_plan,
                                                           component_name)

        metrics: List[str] = ["__execute-latency/" + stream
                              for stream in incoming_streams]

        results: Dict[str, Any] = tracker.get_metrics_timeline(
            self.tracker_url, cluster, environ, topology_id, component_name,
            start, end, metrics)

        output: List[Dict[str, Any]] = []

        for stream_metric, instance_timelines in results["timeline"].items():
            incoming_stream: str = stream_metric.split("/")[-1]
            for instance_name, timeline in instance_timelines.items():

                details = tracker.parse_instance_name(instance_name)
                instance_list: List[Dict[str, Any]] = []

                for timestamp_str, measurement_str in timeline.items():

                    timestamp: dt.datetime = dt.datetime.utcfromtimestamp(
                                    int(timestamp_str))

                    if "nan" in measurement_str:
                        measurement: float = None
                    else:
                        measurement = float(measurement_str) / 1000

                    instance_list.append({
                        "timestamp" : timestamp,
                        "container" : details["container"],
                        "task" : details["task_id"],
                        "component" : details["component"],
                        "stream" : incoming_stream,
                        "latency_ms" : measurement})

                # Because the original dict returned by the tracker is
                # unsorted we need to sort the rows by ascending time
                instance_list.sort(
                    key=lambda instance: instance["timestamp"])

                output.extend(instance_list)

        return pd.DataFrame(output)

    def get_service_times(self, topology_id: str, cluster: str, environ: str,
                          start: int, end: int):
        """ Gets the service times, as a timeseries, for every instance of the
        of all the bolt components of the specified topology. The start and end
        times define the window over which to gather the metrics. The window
        duration should be less than 3 hours as this is the limit of what the
        Topology master stores.

        Arguments:
            topology_id (str):    The topology identification string.
            cluster (str):  The cluster the topology is running in.
            environ (str):  The environment the topology is running in (eg.
                            prod, devel, test, etc).
            start (int):    Start time for the time period the query is run
                            against. This should be a UTC POSIX time integer
                            (seconds since epoch).
            end (int):  End time for the time period the query is run against.
                        This should be a UTC POSIX time integer (seconds since
                        epoch).

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
                latency_ms: The execute latency measurement in milliseconds.
        """

        logical_plan: Dict[str, Any] = tracker.get_logical_plan(
            self.tracker_url, cluster, environ, topology_id)

        output: pd.DataFrame = None

        for bolt_component in logical_plan["bolts"]:

            bolt_service_times: pd.DataFrame = \
                    self.get_component_service_times(topology_id, cluster,
                                                     environ, bolt_component,
                                                     start, end, logical_plan)

            if output is None:
                output = bolt_service_times
            else:
                output = output.append(bolt_service_times, ignore_index=True)

        return output
