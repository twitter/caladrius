# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains classes and methods for extracting metrics from an
InfluxDB server"""

import re
import logging
import warnings

import datetime as dt

from typing import Union, List, DefaultDict, Dict, Optional, Any
from functools import lru_cache
from collections import defaultdict

import pandas as pd

from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet

from caladrius.common.heron import tracker
from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

INFLUX_TIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%fZ"

INSTANCE_NAME_RE_STR: str = r"container_(?P<container>\d+)_.*_(?P<task>\d+)"
INSTANCE_NAME_RE: re.Pattern = re.compile(INSTANCE_NAME_RE_STR)


@lru_cache(maxsize=128, typed=False)
def create_db_name(
        prefix: str, topology: str, cluster: str, environ: str) -> str:
    """ Function for forming the InfluxDB database name from the supplied
    details. This function will cache results to speed up name creation.

    Arguments:
        prefix (str):   The string prepended to all database names for this
                        Heron setup.
        topology (str): The topology ID string.
        cluster (str):  The cluster name.
        environ (str):  The environment that the topology is running in.

    Returns:
        The database name string formed form the supplied details.
    """

    return f"{prefix}-{cluster}-{environ}-{topology}"


def convert_rfc339_to_datetime(time_str: str) -> dt.datetime:
    """ Converts an InfluxDB RFC3339 timestamp string into a naive Python
    datetime object.

    Arguments:
        time_str (str): An RFC3339 format timestamp.

    Returns:
        datetime.datetime:  A naive datetime object.
    """

    return dt.datetime.strptime(time_str, INFLUX_TIME_FORMAT)


def convert_datetime_to_rfc3339(dt_obj: dt.datetime) -> str:
    """ Converts a Python datetime object into an InfluxDB RFC3339 timestamp
    string. The datetime object is assumed to be naive (no timezone).

    Arguments:
        dt_obj (datetime.datetime): A naive datetime object.

    Returns:
        str: An RFC3339 format timestamp.
    """

    return dt_obj.strftime(INFLUX_TIME_FORMAT)


class HeronInfluxDBClient(HeronMetricsClient):

    """ Class for extracting Heron metrics from a InfluxDB server """

    def __init__(self, config: dict) -> None:
        super().__init__(config)

        try:
            self.host: str = config["influx.host"]
            self.port: int = int(config["influx.port"])
            self.database_prefix: str = config["influx.database.prefix"]
            self.tracker_url: str = config["heron.tracker.url"]
        except KeyError as kerr:
            msg: str = f"Required configuration keys were missing: {kerr.args}"
            LOG.error(msg)
            raise KeyError(msg)
        else:
            self.username: Optional[str] = None
            self.password: Optional[str] = None

        if ("influx.user" in config) and ("influx.password" in config):

            self.username = config["influx.user"]
            self.password = config["influx.password"]

            LOG.info("Creating InfluxDB client for user: %s, on host: %s",
                     config["influx.user"], self.host)
            self.client: InfluxDBClient = InfluxDBClient(
                host=self.host, port=self.port, username=self.username,
                password=self.password)

        elif "influx.user" in config and "influx.password" not in config:

            pw_msg: str = (f"Password for InfluxDB user: "
                           f"{config['influx.user']} was not provided")
            LOG.error(pw_msg)
            raise KeyError(pw_msg)

        elif not ("influx.user" in config) and ("influx.password" in config):

            user_msg: str = "InfluxDB user information was not provided"
            LOG.error(user_msg)
            raise KeyError(user_msg)

        else:
            LOG.info("Creating InfluxDB client for sever on host: %s",
                     self.host)
            self.client: InfluxDBClient = InfluxDBClient(host=self.host,
                                                         port=self.port)

        self.metric_name_cache: DefaultDict[str,
                                            DefaultDict[str, List[str]]] = \
            defaultdict(lambda: defaultdict(list))

    def __hash__(self) -> int:

        if self.username and self.password:
            return hash(self.host + str(self.port) + self.database_prefix +
                        self.username + self.password)

        return hash(self.host + str(self.port) + self.database_prefix)

    def __eq__(self, other: object) -> bool:

        if not isinstance(other, HeronInfluxDBClient):
            return False

        if other.username and other.password:
            other_hash: int = hash(other.host + str(other.port) +
                                   other.database_prefix + other.username +
                                   other.password)
        else:
            other_hash = hash(other.host + str(other.port) +
                              other.database_prefix)

        if self.__hash__() == other_hash:
            return True

        return False

    @lru_cache(maxsize=128, typed=False)
    def get_all_measurement_names(self, topology_id: str, cluster: str,
                                  environ: str) -> List[str]:
        """ Gets a list of the measurement names present in the configured
        InfluxDB database for the topology with the supplied credentials.

        Arguments:
            topology (str): The topology ID string.
            cluster (str):  The cluster name.
            environ (str):  The environment that the topology is running in.

        Returns:
            List[str]:  A list of measurement name strings.
        """

        self.client.switch_database(create_db_name(self.database_prefix,
                                    topology_id, cluster, environ))

        return [measurement.get("name") for measurement in
                self.client.get_list_measurements()]

    def get_metric_measurement_names(
            self, database: str, metric_name: str, metric_regex: str,
            force: bool=False) -> List[str]:
        """ Gets a list of measurement names from the supplied database that
        are related to the supplied metric using the supplied regex string.
        Metric name search as cached and so repeated calls will not query the
        database unless the force flag is used.

        Arguments:
            database (str): The name of the influx database to be searched.
            metric_name (str):  The name of the metric whose measurement names
                                are required (this is used for cache keys and
                                logging).
            metric_regex (str): The regex to be used to search for
                                measurement names.
            force (bool):   Flag indicating if the cache should be bypassed.
                            Defaults to false.

        Returns:
            List[str]:  A list of measurement name strings from the specified
                        database.

        Raises:
            RuntimeError:   If the specified database has no measurements
                            matching the supplied metric regex.
        """

        # Check to see if we have already queried Influx for the metric
        # measurement names, if not query them and cache the results.
        if database not in self.metric_name_cache[metric_name] or force:
            LOG.info("Finding measurement names for metric: %s from "
                     "database: %s", metric_name, database)

            # Find all the measurements for each bolt component
            measurement_query: str = (f"SHOW MEASUREMENTS ON \"{database}\" "
                                      f"WITH MEASUREMENT =~ {metric_regex}")

            measurement_names: List[str] = \
                [point["name"] for point in
                 self.client.query(measurement_query).get_points()]

            if not measurement_names:
                msg: str = (f"No measurements found in database: {database} "
                            f"for metric: {metric_name}")
                LOG.error(msg)
                raise RuntimeError(msg)
            else:
                LOG.info("Found %d measurement names for metric: %s",
                         len(measurement_names), metric_name)
                self.metric_name_cache[metric_name][database] = \
                    measurement_names

        else:
            LOG.info("Using cached measurement names for metric: %s from "
                     "database: %s", metric_name, database)

        return self.metric_name_cache[metric_name][database]


    def get_service_times(self, topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime,
                          **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the service times of each of the bolt
        instances in the specified topology

        Arguments:
            topology (str): The topology ID string.
            cluster (str):  The cluster name.
            environ (str):  The environment that the topology is running in.
            start (datetime.datetime):  UTC datetime instance for the start of
                                        the metrics gathering period.
            end (datetime.datetime):    UTC datetime instance for the end of
                                        the metrics gathering period.

        Returns:
            pandas.DataFrame:   A DataFrame containing the service time
            measurements as a timeseries. Each row represents a measurement
            with the following columns:

            * timestamp: The UTC timestamp for the metric,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from.
            * stream: The name of the incoming stream from which the tuples
              that lead to this metric came from,
            * source_component: The name of the component the stream's source
              instance belongs to,
            * execute_latency: The average execute latency during the metric
                               sample period.
        """

        start_time: str = convert_datetime_to_rfc3339(start)
        end_time: str = convert_datetime_to_rfc3339(end)

        database: str = create_db_name(self.database_prefix, topology_id,
                                       cluster, environ)

        LOG.info("Fetching service times for topology: %s on cluster: %s in "
                 "environment: %s for a %s second time period between %s and "
                 "%s", topology_id, cluster, environ,
                 (end-start).total_seconds(), start_time, end_time)

        self.client.switch_database(database)

        metric_name: str = "execute-latency"
        metric_regex: str = "/execute\-latency\/+.*\/+.*/"

        measurement_names: List[str] = self.get_metric_measurement_names(
            database, metric_name, metric_regex)

        output: List[Dict[str, Union[str, int, dt.datetime]]] = []

        for measurement_name in measurement_names:

            _, source_component, stream = measurement_name.split("/")

            query_str: str = (f"SELECT Component, Instance, value "
                              f"FROM \"{measurement_name}\" "
                              f"WHERE time >= '{start_time}' "
                              f"AND time <= '{end_time}'")

            LOG.debug("Querying %s measurements with influx QL statement: %s",
                      metric_name, query_str)

            results: ResultSet = self.client.query(query_str)

            for point in results.get_points():

                instance: Optional[re.Match] = re.search(
                    INSTANCE_NAME_RE, point["Instance"])

                if instance:
                    instance_dict: Dict[str, str] = instance.groupdict()
                else:
                    LOG.warning("Could not parse instance name: %s",
                                point["Instance"])
                    continue

                row: Dict[str, Union[str, int, dt.datetime]] = {
                    "time": convert_rfc339_to_datetime(point["time"]),
                    "component": point["Component"],
                    "task": int(instance_dict["task"]),
                    "container": int(instance_dict["container"]),
                    "stream": stream,
                    "source_component": source_component,
                    "execute_latency": float(point["value"])}

                output.append(row)

        return pd.DataFrame(output)

    def get_emit_counts(self, topology_id: str, cluster: str, environ: str,
                        start: dt.datetime, end: dt.datetime,
                        **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the emit count of each of the instances in
        the specified topology.

        Arguments:
            topology (str): The topology ID string.
            cluster (str):  The cluster name.
            environ (str):  The environment that the topology is running in.
            start (datetime.datetime):  UTC datetime instance for the start of
                                        the metrics gathering period.
            end (datetime.datetime):    UTC datetime instance for the end of
                                        the metrics gathering period.

        Returns:
            pandas.DataFrame:   A DataFrame containing the emit count
            measurements as a timeseries. Each row represents a measurement
            with the following columns:

            * timestamp: The UTC timestamp for the metric,
            * component: The component this metric comes from,
            * task: The instance ID number for the instance that the metric
              comes from,
            * container: The ID for the container this metric comes from,
            * stream: The name of the outgoing stream from which the tuples
              that lead to this metric came from,
            * emit_count: The emit count during the metric time period.
        """

        start_time: str = convert_datetime_to_rfc3339(start)
        end_time: str = convert_datetime_to_rfc3339(end)

        database: str = create_db_name(self.database_prefix, topology_id,
                                       cluster, environ)

        LOG.info("Fetching emit counts for topology: %s on cluster: %s in "
                 "environment: %s for a %s second time period between %s and "
                 "%s", topology_id, cluster, environ,
                 (end-start).total_seconds(), start_time, end_time)

        self.client.switch_database(database)

        metric_name: str = "emit-count"
        metric_regex: str = "/emit\-count\/+.*/"

        measurement_names: List[str] = self.get_metric_measurement_names(
            database, metric_name, metric_regex)

        output: List[Dict[str, Union[str, int, dt.datetime]]] = []

        for measurement_name in measurement_names:

            _, stream = measurement_name.split("/")

            query_str: str = (f"SELECT Component, Instance, value "
                              f"FROM \"{measurement_name}\" "
                              f"WHERE time >= '{start_time}' "
                              f"AND time <= '{end_time}'")

            LOG.debug("Querying %s measurements with influx QL statement: %s",
                      metric_name, query_str)

            results: ResultSet = self.client.query(query_str)

            for point in results.get_points():

                instance: Optional[re.Match] = re.search(
                    INSTANCE_NAME_RE, point["Instance"])

                if instance:
                    instance_dict: Dict[str, str] = instance.groupdict()
                else:
                    LOG.warning("Could not parse instance name: %s",
                                point["Instance"])
                    continue

                row: Dict[str, Union[str, int, dt.datetime]] = {
                    "timestamp": convert_rfc339_to_datetime(point["time"]),
                    "component": point["Component"],
                    "task": int(instance_dict["task"]),
                    "container": int(instance_dict["container"]),
                    "stream": stream,
                    "emit_count": int(point["value"])}

                output.append(row)

        return pd.DataFrame(output)

    def get_execute_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the service times of each of the bolt
        instances in the specified topology

        Arguments:
            topology (str): The topology ID string.
            cluster (str):  The cluster name.
            environ (str):  The environment that the topology is running in.
            start (datetime.datetime):  UTC datetime instance for the start of
                                        the metrics gathering period.
            end (datetime.datetime):    UTC datetime instance for the end of
                                        the metrics gathering period.

        Returns:
            pandas.DataFrame:   A DataFrame containing the service time
            measurements as a timeseries. Each row represents a measurement
            with the following columns:

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

        start_time: str = convert_datetime_to_rfc3339(start)
        end_time: str = convert_datetime_to_rfc3339(end)

        database: str = create_db_name(self.database_prefix, topology_id,
                                       cluster, environ)

        LOG.info("Fetching execute counts for topology: %s on cluster: %s in "
                 "environment: %s for a %s second time period between %s and "
                 "%s", topology_id, cluster, environ,
                 (end-start).total_seconds(), start_time, end_time)

        self.client.switch_database(database)

        metric_name: str = "execute-count"
        metric_regex: str = "/execute\-count\/+.*\/+.*/"

        measurement_names: List[str] = self.get_metric_measurement_names(
            database, metric_name, metric_regex)

        output: List[Dict[str, Union[str, int, dt.datetime]]] = []

        for measurement_name in measurement_names:

            _, source_component, stream = measurement_name.split("/")

            query_str: str = (f"SELECT Component, Instance, value "
                              f"FROM \"{measurement_name}\" "
                              f"WHERE time >= '{start_time}' "
                              f"AND time <= '{end_time}'")

            LOG.debug("Querying %s measurements with influx QL statement: %s",
                      metric_name, query_str)

            results: ResultSet = self.client.query(query_str)

            for point in results.get_points():

                instance: Optional[re.Match] = re.search(
                    INSTANCE_NAME_RE, point["Instance"])

                if instance:
                    instance_dict: Dict[str, str] = instance.groupdict()
                else:
                    LOG.warning("Could not parse instance name: %s",
                                point["Instance"])
                    continue

                row: Dict[str, Union[str, int, dt.datetime]] = {
                    "timestamp": convert_rfc339_to_datetime(point["time"]),
                    "component": point["Component"],
                    "task": int(instance_dict["task"]),
                    "container": int(instance_dict["container"]),
                    "stream": stream,
                    "source_component": source_component,
                    "execute_count": int(point["value"])}

                output.append(row)

        return pd.DataFrame(output)

    def get_complete_latencies(self, topology_id: str, cluster: str,
                               environ: str, start: dt.datetime,
                               end: dt.datetime,
                               **kwargs: Union[str, int, float]
                               ) -> pd.DataFrame:
        """ Gets the complete latencies, as a timeseries, for every instance of
        the of all the spout components of the specified topology. The start
        and end times define the window over which to gather the metrics.

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
            with the following columns:

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
            ConnectionError: If the physical plan cannot be extracted from the
                             Heron Tracker API.
        """

        # First we need to check that the supplied topology will actually have
        # complete latencies. Only ATLEAST_ONCE and EXACTLY_ONCE will have
        # complete latency values as acking is disabled for ATMOST_ONCE.
        try:
            physical_plan: Dict[str, Any] = tracker.get_physical_plan(
                self.tracker_url, cluster, environ, topology_id)
        except ConnectionError as conn_err:
            conn_msg: str = (f"Unable to connect to Heron Tracker API at: "
                             f"{self.tracker_url}. Cannot retrieve physical "
                             f"plan for topology: {topology_id}")
            LOG.error(conn_msg)
            raise ConnectionError(conn_msg)

        if (physical_plan["config"]
                ["topology.reliability.mode"] == "ATMOST_ONCE"):
            rm_msg: str = (f"Topology {topology_id} reliability mode is set "
                           f"to ATMOST_ONCE. Complete latency is not "
                           f"available for these types of topologies")
            LOG.warning(rm_msg)
            warnings.warn(rm_msg, RuntimeWarning)
            return pd.DataFrame()

        start_time: str = convert_datetime_to_rfc3339(start)
        end_time: str = convert_datetime_to_rfc3339(end)

        database: str = create_db_name(self.database_prefix, topology_id,
                                       cluster, environ)

        LOG.info("Fetching complete latencies for topology: %s on cluster: %s "
                 "in environment: %s for a %s second time period between %s "
                 "and %s", topology_id, cluster, environ,
                 (end-start).total_seconds(), start_time, end_time)

        self.client.switch_database(database)

        metric_name: str = "complete-latency"
        metric_regex: str = "/complete\-latency\/+.*/"

        measurement_names: List[str] = self.get_metric_measurement_names(
            database, metric_name, metric_regex)

        output: List[Dict[str, Union[str, int, dt.datetime]]] = []

        for measurement_name in measurement_names:

            _, stream = measurement_name.split("/")

            query_str: str = (f"SELECT Component, Instance, value "
                              f"FROM \"{measurement_name}\" "
                              f"WHERE time >= '{start_time}' "
                              f"AND time <= '{end_time}'")

            LOG.debug("Querying %s measurements with influx QL statement: %s",
                      metric_name, query_str)

            results: ResultSet = self.client.query(query_str)

            for point in results.get_points():

                instance: Optional[re.Match] = re.search(
                    INSTANCE_NAME_RE, point["Instance"])

                if instance:
                    instance_dict: Dict[str, str] = instance.groupdict()
                else:
                    LOG.warning("Could not parse instance name: %s",
                                point["Instance"])
                    continue

                row: Dict[str, Union[str, int, dt.datetime]] = {
                    "timestamp": convert_rfc339_to_datetime(point["time"]),
                    "component": point["Component"],
                    "task": int(instance_dict["task"]),
                    "container": int(instance_dict["container"]),
                    "stream": stream,
                    "latency_ms": float(point["value"])}

                output.append(row)

        return pd.DataFrame(output)

    def get_arrival_rates(self, topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime,
                          **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the arrival rates, in units of tuples per
        second, for each of the instances in the specified topology"""
        pass

    def get_receive_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the receive counts of each of the bolt
        instances in the specified topology"""
        msg: str = ("The custom Caladrius receive-count metrics is not yet "
                    "available via the Influx metrics database")
        LOG.error(msg)
        raise NotImplementedError(msg)
