# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module defines abstract base classes for the traffic modelling """

from abc import abstractmethod
from typing import Any, Dict

from caladrius.model.base import Model


class TrafficModel(Model):
    """ Abstract base class for all traffic modelling classes """

    @abstractmethod
    def predict_traffic(self, topology_id: str,
                        **kwargs: Any) -> Dict[str, Any]:
        """ Predicts the expected traffic arriving at the specified topology
        over the period defined by the duration argument.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose traffic will be predicted.

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            traffic prediction.
        """
        pass
