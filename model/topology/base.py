""" This module defines abstract base classes for the topology performance
modelling """

from abc import abstractmethod
from typing import Any

from caladrius.model.base import Model

class TopologyModel(Model):
    """ Abstract base class for all topology performance modelling classes """

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
