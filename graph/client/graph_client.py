""" Module containing abstract classes for the graph clients """

from abc import ABC, abstractmethod
from typing import List

class GraphClient(ABC):
    """ Abstract Base Class for all graph connection clients. """

    @abstractmethod
    def __init__(self, config: dict) -> None:
        self.config = config
