""" Module containing abstract classes for the graph clients """

from abc import ABC, abstractmethod

class GraphClient(ABC):

    @abstractmethod
    def __init__(self, config) -> None:
        self.config = config
