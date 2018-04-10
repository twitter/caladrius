""" This module contains abstract base classes for the heron metrics clients
"""
from caladrius.metrics.metrics_client import MetricsClient

class HeronMetricsClient(MetricsClient):
    """ Abstract base class for all Heron metric client classes. """
