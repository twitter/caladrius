# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains classes and methods for extracting metrics from an
InfluxDB server"""

import logging

import datetime as dt

import pandas as pd

from typing import Union

from influxdb import InfluxDBClient

from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)


class HeronInfluxDBClient(HeronMetricsClient):

    """ Class for extracting Heron metrics from a InfluxDB server """

    def __init__(self, config: dict) -> None:
        super().__init__(config)

        try:
            self.host: str = config["influx.host"]
            self.port: int = int(config["influx.port"])
            self.database: str = config["influx.database"]
        except KeyError as kerr:
            msg: str = f"Required configuration keys were missing: {kerr.args}"
            LOG.error(msg)
            raise KeyError(msg)

        if ("influx.user" in config) and ("influx.password" in config):

            self.username: str = config["influx.user"]
            self.password: str = config["influx.password"]

            LOG.info("Creating InfluxDB client for user: %s, database: %s on "
                     "host: %s", config["influx.user"], self.database,
                     self.host)
            self.client: InfluxDBClient = InfluxDBClient(
                host=self.host, port=self.port, database=self.database,
                username=self.username, password=self.password)

        elif ("influx.user" in config) and not ("influx.password" in config):

            pw_msg: str = (f"Password for InfluxDB user: "
                           f"{config['influx.user']} was not provided")
            LOG.error(pw_msg)
            raise KeyError(pw_msg)

        elif not ("influx.user" in config) and ("influx.password" in config):

            user_msg: str = "InfluxDB user information was not provided"
            LOG.error(user_msg)
            raise KeyError(user_msg)

        else:
            LOG.info("Creating InfluxDB client for database: %s on host: %s",
                     self.database, self.host)
            self.client: InfluxDBClient = InfluxDBClient(
                host=self.host, port=self.port, database=self.database)

    def __hash__(self) -> int:

        if self.username and self.password:
            return hash(self.host + str(self.port) + self.database +
                        self.username + self.password)
        else:
            return hash(self.host + str(self.port) + self.database)

    def __eq__(self, other: object) -> bool:

        if not isinstance(other, HeronInfluxDBClient):
            return False

        if other.username and other.password:
            other_hash: int = hash(other.host + str(other.port) +
                                   other.database + other.username +
                                   other.password)
        else:
            other_hash = hash(other.host + str(other.port) + other.database)

        if self.__hash__() == other_hash:
            return True

        return False

    def get_service_times(self, topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime,
                          **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets a time series of the service times of each of the bolt
        instances in the specified topology"""
        pass

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
