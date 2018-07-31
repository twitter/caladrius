# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module defines abstract base classes for the Heron topology
performance model classes"""

from abc import abstractmethod
import datetime as dt
from typing import Any, Dict

from caladrius.model.base import Model
from caladrius.traffic_provider.trafficprovider import TrafficProvider


class HeronTopologyModel(Model):
    """ Abstract base class for all Heron topology performance modelling
    classes """

    @abstractmethod
    def find_current_instance_waiting_times(self, topology_id: str, cluster: str,
                                            environ: str, traffic_source: TrafficProvider, start: dt.datetime,
                                            end: dt.datetime, **kwargs: Any) -> list:
        """ Applies queueing theory concepts to find the end to end latency of the
         specified topology.
        Arguments:
            topology_id (str): The identification string for the topology
            whose performance will be predicted.
            cluster (str): The cluster the topology is running on.
            environ (str): The environment the topology is running in.
            traffic_source (TrafficProvider): This object provides the current traffic
            rates for use by the GGC Queue
            start (datatime):  The time starting from where we look at performance metrics.
            end (datetime): The time until we look at job performance metrics for prediction.
            **kwargs: Any additional keyword arguments required by the model
            implementation.
        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            performance prediction.


        """
        pass

    @abstractmethod
    def predict_current_performance(self, topology_id: str, cluster: str,
                                    environ: str, spout_traffic: Dict[int, Dict[str, float]],
                                    **kwargs: Any) -> Dict[str, Any]:
        """ Predicts the performance of the specified topology as it is
        currently configured with the supplied traffic level.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose performance will be predicted.
            cluster (str): The cluster the topology is running on.
            environ (str): The environment the topology is running in.
            spout_traffic (dict):  A dictionary which gives the output of each
                                    spout instance onto each output stream.
            **kwargs:   Any additional keyword arguments required by the model
                        implementation.

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            performance prediction.
        """
        pass

    @abstractmethod
    def predict_packing_plan(self, topology_id: str, cluster: str, environ: str,
                             start: dt.datetime, end:dt.datetime, traffic_provider: TrafficProvider,
                             **kwargs: Any) -> Dict[str, Any]:

        """ Given the current performance of the topology, the
        function proposes a new packing plan for the topology.
        Arguments:
            topology_id (str):  The identification string for the topology
                                whose performance will be predicted.
            cluster (str): The cluster the topology is running on.
            environ (str): The environment the topology is running in.
            start (datetime): The time starting from where we look at performance metrics.
            end (datetime): The time until we look at job performance metrics for prediction.
            traffic_provider (TrafficProvider):   A TrafficProvider instance that can
            give us the arrival rate of tuples at each of the instances
            **kwargs:   Any additional keyword arguments required by the model
                        implementation.

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            new packing plan for the job.
        """
        pass
