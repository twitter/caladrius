# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains classes and methods for extracting metrics from an
InfluxDB server"""

import re
import logging

import datetime as dt

from typing import Union, List, DefaultDict, Dict, Optional
from functools import lru_cache
from collections import defaultdict

import pandas as pd

from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet

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
        except KeyError as kerr:
            msg: str = f"Required configuration keys were missing: {kerr.args}"
            LOG.error(msg)
            raise KeyError(msg)

        if ("influx.user" in config) and ("influx.password" in config):

            self.username: str = config["influx.user"]
            self.password: str = config["influx.password"]

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
    def get_measurement_names(self, topology_id: str, cluster: str,
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

        metric: str = "execute-latency"

        # Check to see if we have already queried Influx for the metric
        # measurement names, if not query them and cache the results.
        if database not in self.metric_name_cache[metric]:
            LOG.debug("Finding measurement names for metric %s from "
                      "database: %s", metric, database)
            # Find all the measurements for each bolt component
            measurement_query: str = ("SHOW MEASUREMENTS WITH MEASUREMENT =~ "
                                      "/execute\-latency\/+.*\/+.*/")

            measurement_names: List[str] = \
                [point["name"] for point in
                 self.client.query(measurement_query).get_points()]

            if not measurement_names:
                LOG.warning("No measurements found in database: %s for metric "
                            "%s", database, metric)
            else:
                LOG.debug("Found %d measurement names for %s metric",
                          len(measurement_names), metric)

            self.metric_name_cache[metric][database] = \
                measurement_names
        else:
            LOG.debug("Using cached measurement names for %s metric", metric)

        output: List[Dict[str, Union[str, int, dt.datetime]]] = []

        for measurement_name in self.metric_name_cache[metric][database]:

            _, source_component, stream = measurement_name.split("/")

            query_str: str = (f"SELECT Component, Instance, value "
                              f"FROM \"{measurement_name}\" "
                              f"WHERE time >= '{start_time}' "
                              f"AND time <= '{end_time}'")

            LOG.debug("Querying %s measurements with influx QL statement: %s",
                      metric, query_str)

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
                    "execute_latency": point["value"]}

                output.append(row)

        return pd.DataFrame(output)

    def get_receive_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the receive counts of each of the bolt
        instances in the specified topology"""
        pass

    def get_emit_counts(self, topology_id: str, cluster: str, environ: str,
                        start: dt.datetime, end: dt.datetime,
                        **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the emit count of each of the instances in
        the specified topology"""
        pass

    def get_execute_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the emit count of each of the instances in
        the specified topology"""
        pass

    def get_complete_latencies(self, topology_id: str, cluster: str,
                               environ: str, start: dt.datetime,
                               end: dt.datetime,
                               **kwargs: Union[str, int, float]
                               ) -> pd.DataFrame:
        """ Gets a time series of the complete latencies of each of the spout
        instances in the specified topology"""
        pass

    def get_arrival_rates(self, topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime,
                          **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the arrival rates, in units of tuples per
        second, for each of the instances in the specified topology"""
        pass
