""" Abstract base class from which all Caladrius models inherit """

from abc import ABC, abstractmethod
from typing import Any

from caladrius.metrics.client import MetricsClient
from caladrius.graph.gremlin.client import GremlinClient

class Model(ABC):
    """ Abstract base class for all caladrius model classes"""

    @abstractmethod
    def __init__(self, config: dict, metrics_client: MetricsClient,
                 graph_client: GremlinClient, name: str, description: str,
                 **kwargs: Any) -> None:

        self.name: str = name
        self.description: str = description
        self.config: dict = config
        self.metrics_client: MetricsClient = metrics_client
        self.graph_client: GremlinClient = graph_client
