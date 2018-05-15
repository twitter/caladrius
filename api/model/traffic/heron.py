""" This module contains the API resources for the Apache Heron traffic
modelling """

import logging
import json

from typing import List, Dict, Type, Any

from flask_restful import Resource, reqparse

from caladrius.model.traffic.base import TrafficModel
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

class HeronTrafficModels(Resource):

    def __init__(self, model_classes: List[Type],
            model_config: Dict[str, Any]) -> None:


        self.models_info: List[Dict[str, Any]] = []

        for model in model_classes:
            model_info: Dict[str, str] = {}
            model_info["name"] = model.name
            model_info["description"] = model.description
            self.models_info.append(model_info)

    def get(self) -> List[Dict[str, Any]]:
        """ Returns the configured traffic models as a list of model
        information dictionaries that contain "name" and "description" keys.
        """
        return self.models_info

class HeronTraffic(Resource):
    """ This resource handles requests for traffic modelling of specific
    topologies. """

    def __init__(self, model_classes: List[Type], model_config: Dict[str, Any],
                 metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:

        self.models: List[TrafficModel] = []
        for model_class in model_classes:
            self.models.append(model_class(model_config, metrics_client,
                                           graph_client))

        self.parser = reqparse.RequestParser()
        super().__init__()

    def get(self, topo_id: str) -> Dict[str, Any]:

        # Any parameters included with the get request will be passed to the
        # TrafficModel instances
        args = self.parser.parse_args()

        LOG.info("Traffic prediction requested for Heron topology: %s",
                 topo_id)

        output: dict = {}

        for model in self.models:
            LOG.info("Running traffic prediction model %s for topology %s ",
                     model.name, topo_id)
            output[model.name] = model.predict_traffic(topo_id, **args)

        return output
