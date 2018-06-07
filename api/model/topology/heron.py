# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This is the rooting logic for the Apache Heron topology performance
modelling API """
import logging

from typing import List, Type, Dict, Any, Tuple

import pandas as pd

from flask import request
from flask_restful import Resource

from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.graph.utils.heron import graph_check
from caladrius.model.topology.heron.base import HeronTopologyModel
from caladrius.api import utils

LOG: logging.Logger = logging.getLogger(__name__)


class HeronTopologyModels(Resource):
    """ Resource class for the Heron topology model information end point."""

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


class HeronCurrent(Resource):
    """ Resource class for modelling the performance of currently running Heron
    topologies. """

    def __init__(self, model_classes: List[Type], model_config: Dict[str, Any],
                 metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient, tracker_url: str) -> None:

        self.metrics_client: HeronMetricsClient = metrics_client
        self.graph_client: GremlinClient = graph_client

        self.tracker_url: str = tracker_url
        self.model_config: Dict[str, Any] = model_config

        self.models: Dict[str, HeronTopologyModel] = {}
        for model_class in model_classes:
            model = model_class(model_config, metrics_client, graph_client)
            self.models[model.name] = model

        super().__init__()

    def post(self, topology_id: str) -> Tuple[Dict[str, Any], int]:
        """ Method handling POST requests to the current topology performance
        modelling endpoint."""

        # Make sure we have the args we need
        errors: List[Dict[str, str]] = []
        if "cluster" not in request.args:
            errors.append({"type": "MissingParameter",
                           "error": "'cluster' parameter should be supplied"})

        if "environ" not in request.args:
            errors.append({"type": "MissingParameter",
                           "error": "'environ' parameter should be supplied"})

        if "model" not in request.args:
            errors.append({"type": "MissingParameter",
                           "error": ("At least one 'model' parameter should "
                                     "be supplied. Supply 'all' to run all "
                                     "configured models")})

        # Return useful errors to the client if any parameters are missing
        if errors:
            return {"errors": errors}, 400

        LOG.info("Processing performance modelling request for topology: %s, "
                 "cluster: %s, environment: %s, using model: %s", topology_id,
                 request.args.get("cluster"), request.args.get("environ"),
                 str(request.args.getlist("model")))

        # Make sure we have a current graph representing the physical plan for
        # the topology
        graph_check(self.graph_client, self.model_config, self.tracker_url,
                    request.args.get("cluster"), request.args.get("environ"),
                    topology_id)

        # Get the spout traffic state and convert the json string task ID to
        # integers
        json_traffic: Dict[str, Dict[str, float]] = request.get_json()
        traffic: Dict[int, Dict[str, float]] = \
            {int(key): value for key, value in json_traffic.items()}

        if "all" in request.args.getlist("model"):
            LOG.info("Running all configured Heron topology performance "
                     "models")
            models = self.models.keys()
        else:
            models = request.args.getlist("model")

        # Convert the request.args to a dict suitable for passing as **kwargs
        model_kwargs: Dict[str, Any] = \
            utils.convert_wimd_to_dict(request.args)

        # Remove the models list from the kwargs as it is only needed by this
        # method
        model_kwargs.pop("model")

        output = {}
        for model_name in models:
            LOG.info("Running topology performance model %s", model_name)

            model = self.models[model_name]

            try:
                results: pd.DataFrame = model.predict_current_performance(
                    topology_id=topology_id,
                    cluster=request.args.get("cluster"),
                    environ=request.args.get("environ"),
                    spout_traffic=traffic, **model_kwargs)
            except Exception as err:
                LOG.error("Error running model: %s -> %s", model.name,
                          str(err))
                errors.append({"model": model.name, "type": str(type(err)),
                               "error": str(err)})
            else:
                output[model_name] = results.to_json(orient="records")

        if errors:
            return {"errors": errors}, 500

        return output, 200

class HeronProposed(Resource):

    def get(self, topology_id: str) -> Tuple[Dict[str, Any], int]:
        return {"errors": ["Not implemented yet"]}, 501

    def post(self, topo_id: str) -> Tuple[Dict[str, Any], int]:
        return {"errors": ["Not implemented yet"]}, 501
