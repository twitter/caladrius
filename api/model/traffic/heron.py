# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains the API resources for the Apache Heron traffic
modelling """

import logging
from flask import request
from flask_restful import Resource
from typing import List, Dict, Type, Any, Tuple

from caladrius.api import utils
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.traffic.heron.base import HeronTrafficModel
from caladrius.graph.gremlin.client import GremlinClient
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

        self.models: Dict[str, HeronTrafficModel] = {}
        for model_class in model_classes:
            self.models[model_class.name] = \
                model_class(model_config, metrics_client, graph_client)

        super().__init__()

    def get(self, topology_id) -> Tuple[Dict[str, Any], int]:

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

        LOG.info("Traffic prediction requested for Heron topology: %s on "
                 "cluster: %s in environment: %s", topology_id,
                 request.args["cluster"], request.args["environ"])

        # Make sure we have a current graph representing the physical plan for
        # the topology
        try:
            graph_check(self.graph_client, self.model_config, self.tracker_url,
                        request.args["cluster"], request.args["environ"],
                        topology_id)
        except Exception as err:
            LOG.error("Error running graph check for topology: %s -> %s",
                      topology_id, str(err))
            errors.append({"topology": topology_id,
                           "type": str(type(err)),
                           "error": str(err)})
            return {"errors": errors}, 400

        output: Dict[str, Any] = {}
        output["errors"] = {}
        output["results"] = {}

        if "all" in request.args.getlist("model"):
            LOG.info("Running all configured Heron traffic performance models")
            models = self.models.keys()
        else:
            models = request.args.getlist("model")

        # Convert the request.args to a dict suitable for passing as **kwargs
        model_kwargs: Dict[str, Any] = \
            utils.convert_wimd_to_dict(request.args)

        # Remove the models list from the kwargs as it is only needed by this
        # method, same with topology_id, cluster and environ values
        model_kwargs.pop("model")
        model_kwargs.pop("cluster")
        model_kwargs.pop("environ")

        output = {}
        for model_name in models:
            LOG.info("Running traffic performance model %s", model_name)

            model: HeronTrafficModel = self.models[model_name]

            try:
                results: Dict[str, Any] = model.predict_traffic(
                    topology_id=topology_id,
                    cluster=request.args.get("cluster"),
                    environ=request.args.get("environ"),
                    **model_kwargs)
            except Exception as err:
                LOG.error("Error running model: %s -> %s", model.name,
                          str(err))
                errors.append({"model": model.name, "type": str(type(err)),
                               "error": str(err)})
            else:
                output[model_name] = results

        if errors:
            return {"errors": errors}, 500

        return output, 200
