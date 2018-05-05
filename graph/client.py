""" Module containing abstract classes for the graph clients """

from abc import ABC, abstractmethod
from typing import List

class GraphClient(ABC):
    """ Abstract Base Class for all graph connection clients. """

    @abstractmethod
    def __init__(self, config: dict) -> None:
        self.config = config

    @abstractmethod
    def __hash__(self) -> int:
        pass

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        pass

    @abstractmethod
    def topology_ref_exists(self, topology_id: str, topology_ref: str) -> bool:
        """ Checks weather the graph database already contains entries with the
        supplied topology ID and reference. """
        pass
