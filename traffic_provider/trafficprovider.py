# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

from abc import abstractmethod


class TrafficProvider(object):
    """ Abstract base class for objects that can provide topology arrival rates.
     This is useful when we want to abstract away whether the arrival rate is current
     or predicted."""

    @abstractmethod
    def service_times(self):
        """ This function returns the service times for a given topology. Depending on arguments
        provided, the arrival rates can be for future traffic or for current traffic. """
        pass

    @abstractmethod
    def arrival_rates(self):
        """ The function returns the arrival rates of the given topology. Depending on arguments
        provided, the arrival rates can be for future traffic or for current traffic. """
        pass

    @abstractmethod
    def tuple_arrivals(self):
        """ The function returns the number of tuples that have arrived at every instance
        in the last minute. """
        pass

    @abstractmethod
    def inter_arrival_times(self):
        """ The function returns the times between subsequent tuple arrivals """
        pass