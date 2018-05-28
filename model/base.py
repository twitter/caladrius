# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" Abstract base class from which all Caladrius models inherit """

from abc import ABC, abstractmethod
from typing import Any

from caladrius.metrics.client import MetricsClient
from caladrius.graph.gremlin.client import GremlinClient


class Model(ABC):
    """ Abstract base class for all caladrius model classes"""

    # The name of the model. This is used to reference the model in the API
    # so it should be unique
    name: str = "base"

    # A brief description of the model. This is shown by the model info
    # endpoints.
    description: str = "base"

    @abstractmethod
    def __init__(self, config: dict, metrics_client: MetricsClient,
                 graph_client: GremlinClient, **kwargs: Any) -> None:

        self.config: dict = config
        self.metrics_client: MetricsClient = metrics_client
        self.graph_client: GremlinClient = graph_client
