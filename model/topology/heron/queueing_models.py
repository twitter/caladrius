# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module models different queues and performs relevant calculations for it."""

import logging

LOG: logging.Logger = logging.getLogger(__name__)


class MMCQueue(object):
    def __init__(self, average_arrival_rate, average_service_rate, num_servers):
        """
        This function initializes relevant variables to calculate queue related metrics
        given an M/M/c model.
        Arguments:
                average_arrival_rate (float): (λ) lambda. This number indicates the
                average arrival rate of tuples at each instance.
                average_service_rate(float): (µ) mu. This number indicates the average
                service time of tuples per instance (calculated by averaging execute
                latency) for the instance.
        """
        self.average_arrival_rate: float = average_arrival_rate
        self.average_service_rate: float = average_service_rate
        self.num_servers: float = num_servers
        self.server_utilization: float = self.average_arrival_rate/self.average_service_rate

    def average_waiting_time(self) -> float:
        average_waiting_time: float = self.average_arrival_rate / (self.average_service_rate
                                                                   * (self.average_service_rate - self.average_arrival_rate))
        return average_waiting_time

    def average_queue_size(self) -> float:
        queue_size = (self.server_utilization ** 2)/(1 - self.server_utilization)
        return queue_size
