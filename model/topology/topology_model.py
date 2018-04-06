""" This module defines abstract base classes for the topology performance
modelling """

from abc import ABC, abstractmethod
from typing import Any

from caladrius.metrics.metrics_client import MetricsClient
from caladrius.graph.graph_client import GraphClient

class TopologyModel(ABC):
    """ Abstract base class for all topology performance modelling classes """

    @abstractmethod
    def __init__(self, metrics: MetricsClient, graph: GraphClient) -> None:
        self.metrics = metrics
        self.graph = graph

    @abstractmethod
    def predict_performance(self, topology_id: str,
                            proposed_plan: Any) -> dict:
        """ Predicts the performance of the specified topology when configured
        according to the proposed physical plan.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose performance will be predicted.
            proposed_plan:  A data structure containing the proposed physical
                            plan.

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            performance prediction.
        """
        pass
