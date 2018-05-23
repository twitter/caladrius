""" This module contains abstract base classes for the heron metrics clients
"""
import datetime as dt

from abc import abstractmethod
from typing import Union

from pandas import DataFrame

from caladrius.metrics.client import MetricsClient


class HeronMetricsClient(MetricsClient):
    """ Abstract base class for all Heron metric client classes. """

    @abstractmethod
    def get_service_times(self, topology_id: str, start: dt.datetime,
                          end: dt.datetime, **kwargs: Union[str, int, float]
                          ) -> DataFrame:
        """ Gets a time series of the service times of each of the bolt
        instances in the specified topology"""
        pass

    @abstractmethod
    def get_receive_counts(self, topology_id: str, start: dt.datetime,
                           end: dt.datetime, **kwargs: Union[str, int, float]
                           ) -> DataFrame:
        """ Gets a time series of the receive counts of each of the bolt
        instances in the specified topology"""
        pass

    @abstractmethod
    def get_emit_counts(self, topology_id: str, start: dt.datetime,
                        end: dt.datetime, **kwargs: Union[str, int, float]
                        ) -> DataFrame:
        """ Gets a time series of the emit count of each of the instances in
        the specified topology"""
        pass

    @abstractmethod
    def get_execute_counts(self, topology_id: str, start: dt.datetime,
                           end: dt.datetime, **kwargs: Union[str, int, float]
                           ) -> DataFrame:
        """ Gets a time series of the emit count of each of the instances in
        the specified topology"""
        pass

    @abstractmethod
    def get_complete_latencies(self, topology_id: str, start: dt.datetime,
                               end: dt.datetime,
                               **kwargs: Union[str, int, float]
                               ) -> DataFrame:
        """ Gets a time series of the complete latencies of each of the spout
        instances in the specified topology"""
        pass
