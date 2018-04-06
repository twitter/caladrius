""" Module containing abstract classes for the metrics clients """

from abc import ABC, abstractmethod

class MetricsClient(ABC):

    @abstractmethod
    def __init__(self, config) -> None:
        self.config = config
