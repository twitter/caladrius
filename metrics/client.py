""" Module containing abstract classes for the metrics clients """

from abc import ABC, abstractmethod
from typing import Dict, Any

class MetricsClient(ABC):
    """ Abstract base class for all metric clients """

    @abstractmethod
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    @abstractmethod
    def __hash__(self) -> int:
        pass

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        pass
