""" This module contains abstract base classes for the heron metrics clients
"""
from abc import abstractmethod

from caladrius.metrics.client import MetricsClient

class HeronMetricsClient(MetricsClient):
    """ Abstract base class for all Heron metric client classes. """

    @abstractmethod
    def __init__(self, config: dict) -> None:
        super().__init__(config)

    @abstractmethod
    def get_service_times(self, topology_id: str, start, end, **kwargs):
        """ Gets a time series of the service times of each of the bolt
        instances in the specified topology"""
        pass
