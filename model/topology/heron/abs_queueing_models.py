# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module models different kinds of queues and performs relevant calculations for it."""

from abc import abstractmethod
import pandas as pd


class QueueingModels:
    """ Abstract base class for different queueing theory models """
    def __init__(self, kwargs: dict):
        pass

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
