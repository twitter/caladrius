# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module defines abstract base classes for the Heron topology
performance model classes"""

from abc import abstractmethod
from typing import Any, Dict

from caladrius.model.base import Model


class HeronTopologyModel(Model):
    """ Abstract base class for all Heron topology performance modelling
    classes """

    @abstractmethod
    def predict_current_performance(self, topology_id: str, cluster: str,
                                    environ: str,
                                    spout_traffic: Dict[int, Dict[str, float]],
                                    **kwargs: Any) -> Dict[str, Any]:
        """ Predicts the performance of the specified topology as it is
        currently configured with the supplied traffic level.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose performance will be predicted.
            cluster (str): The cluster the topology is running on.
            environ (str): The environment the topology is running in.
            spout_traffic (dict):   A dictionary which gives the output of each
                                    spout instance onto each output stream.
            **kwargs:   Any additional keyword arguments required by the model
                        implementation.

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            performance prediction.
        """
        pass

    @abstractmethod
    def predict_proposed_performance(
            self, topology_id: str, cluster: str, environ: str,
            spout_traffic: Dict[int, Dict[str, float]],
            proposed_plan: Any, **kwargs: Any) -> Dict[str, Any]:
        """ Predicts the performance of the specified topology when configured
        according to the proposed physical plan.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose performance will be predicted.
            cluster (str): The cluster the topology is running on.
            environ (str): The environment the topology is running in.
            spout_traffic (dict):   A dictionary which gives the output of each
                                    spout instance onto each output stream.
            proposed_plan:  A data structure containing the proposed physical
                            plan.
            **kwargs:   Any additional keyword arguments required by the model
                        implementation.

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            performance prediction.
        """
        pass
