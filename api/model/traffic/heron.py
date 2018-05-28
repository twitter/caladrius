# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains the API resources for the Apache Heron traffic
modelling """

import logging

from typing import List, Dict, Type, Any

from flask_restful import Resource, reqparse

from caladrius.model.traffic.base import TrafficModel
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.utils.heron import graph_check

LOG: logging.Logger = logging.getLogger(__name__)


class HeronTrafficModels(Resource):
    """ Resource class for the traffic model information endpoint. """

    def __init__(self, model_classes: List[Type]) -> None:

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
                 graph_client: GremlinClient, tracker_url: str) -> None:

        self.metrics_client: HeronMetricsClient = metrics_client
        self.graph_client: GremlinClient = graph_client

        self.tracker_url: str = tracker_url
        self.model_config: Dict[str, Any] = model_config

        self.models: List[TrafficModel] = []
        for model_class in model_classes:
            self.models.append(model_class(model_config, metrics_client,
                                           graph_client))

        self.parser = reqparse.RequestParser()
        self.parser.add_argument("cluster", type=str, required=True,
                                 help="The name of the cluster the topology is"
                                      " running in")
        self.parser.add_argument("environ", type=str, required=True,
                                 help="The name of the environment the "
                                      "topology is running in")
        self.parser.add_argument("source_hours", type=float, required=False,
                                 help=("The number of hours of historical "
                                       "metrics data to use in the traffic "
                                       "prediction"))
        super().__init__()

    def get(self, topology_id: str) -> Dict[str, Any]:

        request_args = self.parser.parse_args()

        LOG.info("Traffic prediction requested for Heron topology: %s",
                 topology_id)

        # Make sure we have a current graph representing the physical plan for
        # the topology
        graph_check(self.graph_client, self.model_config, self.tracker_url,
                    request_args["cluster"], request_args["environ"],
                    topology_id)

        output: Dict[str, Any] = {}
        output["errors"] = {}
        output["results"] = {}

        for model in self.models:
            LOG.info("Running traffic prediction model %s for topology %s ",
                     model.name, topology_id)
            try:
                output["results"][model.name] = \
                    model.predict_traffic(topology_id, **request_args)
            except Exception as err:
                output["errors"][model.name] = \
                    str(type(err)).split("'")[1] + " : " + str(err)

        return output
