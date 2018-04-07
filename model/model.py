""" Abstract base class from which all Caladrius models inherit """

from abc import ABC

class Model(ABC):

    def __init__(self, name: str, config: Config, metrics: MetricsClient,
                 graph: GraphClient) -> None:
        self.name: str = name
        self.config: Config = config
        self.metrics: MetricsClient = metrics
        self.graph: GraphClient = graph
