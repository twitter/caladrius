""" This module contains the API resources for the Apache Heron traffic
modelling """

import logging
import json

from typing import List, Dict, Type, Any

from flask_restful import Resource, reqparse

from caladrius.model.traffic.model import TrafficModel
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

class HeronTraffic(Resource):

    def __init__(self, model_classes: List[Type], model_config: Dict[str, Any],
                 metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:

        self.models: List[TrafficModel] = []
        for model_class in model_classes:
            self.models.append(model_class(model_config, metrics_client,
                                           graph_client))

        self.parser = reqparse.RequestParser()
        #self.parser.add_argument('hours', type=int, required=False,
        #                         help='Duration must be supplied')
        super().__init__()

    def get(self, topo_id: str) -> Dict[str, Any]:

        args = self.parser.parse_args()

        LOG.info("Traffic prediction requested for Heron topology: %s",
                 topo_id)

        output: dict = {}

        for model in self.models:
            LOG.info("Running traffic prediction model %s for topology %s ",
                     model.name, topo_id)
            output[model.name] = model.predict_traffic(topo_id, **args)

        return output
