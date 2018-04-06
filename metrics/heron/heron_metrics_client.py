""" This module contains abstract base classes for the heron metrics clients
"""
from abc import abstractmethod

from caladrius.metrics.metrics_client import MetricsClient

class HeronMetrics(MetricsClient):
    """ Abstract base class for all Heron metric client classes. """

    @abstractmethod
    def __init__(self, config) -> None:
        super().__init__(config)
