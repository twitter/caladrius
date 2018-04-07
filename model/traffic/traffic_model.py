""" This module defines abstract base classes for the traffic modelling """

from abc import ABC, abstractmethod

from caladrius.model.model import Model

class TrafficModel(Model):
    """ Abstract base class for all traffic modelling classes """

    @abstractmethod
    def predict_traffic(self, topology_id: str, duration: int) -> dict:
        """ Predicts the expected traffic arriving at the specified topology
        over the period defined by the duration argument.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose traffic will be predicted.
            duration (int): The number of minuets over which to summarise the
                            traffic predictions. For example duration = 120
                            would equate to "predict the traffic over the next
                            two hours".

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            traffic prediction.
        """
        pass
