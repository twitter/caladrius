""" Abstract base class from which all Caladrius models inherit """

from abc import ABC, abstractmethod
from typing import Any

from caladrius.metrics.client import MetricsClient
from caladrius.graph.client import GraphClient

class Model(ABC):

    @abstractmethod
    def __init__(self, config: dict, metrics_client: MetricsClient,
                 graph_client: GraphClient, name: str, **kwargs: Any) -> None:
        self.name: str = name
        self.config: dict = config
        self.metrics_client: MetricsClient = metrics_client
        self.graph_client: GraphClient = graph_client
