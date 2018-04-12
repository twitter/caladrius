""" Module containing abstract classes for the graph clients """

from abc import ABC, abstractmethod
from typing import List

class GraphClient(ABC):
    """ Abstract Base Class for all graph connection clients. """

    @abstractmethod
    def __init__(self, config: dict) -> None:
        self.config = config

    @abstractmethod
    def get_sources(self, topology_id: str) -> List[str]:
        """ Gets a list of source component names for the supplied topology.

        Arguments:
            topology_id (str):  The identification string for the DSPS topology
                                that is to be queried.

        Returns:
            A list of component identification strings
        """
        pass
