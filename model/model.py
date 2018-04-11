""" Abstract base class from which all Caladrius models inherit """

from abc import ABC, abstractmethod

from caladrius.metrics.metrics_client import MetricsClient
from caladrius.graph.graph_client import GraphClient

class Model(ABC):

    @abstractmethod
    def __init__(self, name: str, config: dict, metrics: MetricsClient,
                 graph: GraphClient) -> None:
        self.name: str = name
        self.config: dict = config
        self.metrics: MetricsClient = metrics
        self.graph: GraphClient = graph
