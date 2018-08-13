# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module models different kinds of queues and performs relevant calculations for it."""

from abc import abstractmethod
import datetime as dt
import pandas as pd
from typing import List

from caladrius.metrics.client import MetricsClient
from caladrius.graph.gremlin.client import GremlinClient


class QueueingModels:
    """ Abstract base class for different queueing theory models """
    def __init__(self, graph_client: GremlinClient, metrics_client: MetricsClient, paths: List,
                 topology_id: str, cluster: str, environ: str,
                 start: dt.datetime, end: dt.datetime, kwargs: dict):
        self.metrics_client: MetricsClient = metrics_client
        self.graph_client: GremlinClient = graph_client
        self.topology_id = topology_id
        self.paths = paths
        self.cluster = cluster
        self.environ = environ
        self.start = start
        self.end = end
        self.kwargs = kwargs
        self.service_rate: pd.DataFrame
        self.arrival_rate: pd.DataFrame

    @abstractmethod
    def average_waiting_time(self) -> pd.DataFrame:
        """ Predicts the amounts of time a tuple would face while
        waiting to be processed by an instance.
        """
        pass

    @abstractmethod
    def average_queue_size(self) -> pd.DataFrame:
        """ Predicts the average queue size given a certain arrival rate for
        an instance and its processing rate.
        """
        pass

    @abstractmethod
    def end_to_end_latencies(self) -> list:
        """ Totals up execute latency and waiting times to find end to end latencies
        across all paths of a topology
        """
        pass
