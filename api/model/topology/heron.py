# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This is the rooting logic for the Apache Heron topology performance
modelling API """
from flask import request
from flask_restful import Resource
import logging
import json
import pandas as pd
from typing import List, Type, Dict, Any, Tuple

from caladrius.api import utils
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.graph.utils.heron import graph_check, paths_check
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.topology.heron.base import HeronTopologyModel
from caladrius.model.topology.heron.queueing_theory import get_start_end_times
from caladrius.traffic_provider.predicted_traffic import PredictedTraffic
from caladrius.traffic_provider.current_traffic import CurrentTraffic

LOG: logging.Logger = logging.getLogger(__name__)


class HeronTopologyModels(Resource):
    """ Resource class for the Heron topology model information end point."""

    def __init__(self, model_classes: List[Type]) -> None:

        self.models_info: List[Dict[str, Any]] = []

        for model in model_classes:
            model_info: Dict[str, str] = dict()
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
                 "cluster: %s, environment: %s, using model: %s",
                 topology_id, request.args.get("cluster"),
                 request.args.get("environ"),
                 str(request.args.getlist("model")))

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

        # Remove the models list + other keys from the kwargs as it is only
        # needed by this method
        model_kwargs.pop("model")
        model_kwargs.pop("cluster")
        model_kwargs.pop("environ")
        cluster = request.args.get("cluster")
        environ = request.args.get("environ")

        output = {}
        for model_name in models:
            LOG.info("Running topology performance model %s", model_name)

            model = self.models[model_name]

            try:
                results: pd.DataFrame = model.predict_current_performance(
                    topology_id=topology_id,
                    cluster=cluster,
                    environ=environ,
                    spout_traffic=traffic, **model_kwargs)
            except Exception as err:
                LOG.error("Error running model: %s -> %s", model.name,
                          str(err))
                errors.append({"model": model.name, "type": str(type(err)),
                               "error": str(err)})
            else:
                output[model_name] = results.to_json()

        if errors:
            return {"errors": errors}, 500

        return output, 200

    def get(self, topology_id: str) -> Tuple[Dict[str, Any], int]:
        """ Method handling requests for the currently running topology's end to
        end latency"""

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

        cluster = request.args.get("cluster")
        environ = request.args.get("environ")
        # Make sure we have a current graph representing the physical plan for
        # the topology
        graph_check(self.graph_client, self.model_config, self.tracker_url,
                    cluster, environ, topology_id)

        # Make sure we have a file containing all paths for the job
        paths_check(self.graph_client, self.model_config, cluster,
                    environ, topology_id)

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
        model_kwargs.pop("cluster")
        model_kwargs.pop("environ")
        model_kwargs["zk.time.offset"] = self.model_config["zk.time.offset"]
        model_kwargs["heron.statemgr.root.path"] = self.model_config["heron.statemgr.root.path"]
        model_kwargs["heron.statemgr.connection.string"] = self.model_config["heron.statemgr.connection.string"]

        start, end = get_start_end_times(**model_kwargs)
        traffic_provider: CurrentTraffic = CurrentTraffic(self.metrics_client, self.graph_client, topology_id,
                                                          cluster, environ, start, end, {}, **model_kwargs)
        output = {}
        for model_name in models:
            LOG.info("Running topology performance model %s", model_name)

            model = self.models[model_name]

            try:
                results: list = model.find_current_instance_waiting_times(topology_id=topology_id, cluster=cluster,
                                                                          environ=environ,
                                                                          traffic_source=traffic_provider,
                                                                          start=start, end=end,
                                                                          **model_kwargs)
            except Exception as err:
                LOG.error("Error running model: %s -> %s", model.name,
                          str(err))
                errors.append({"model": model.name, "type": str(type(err)),
                               "error": str(err)})
            else:
                output[model_name] = json.dumps(results)

        if errors:
            return {"errors": errors}, 500

        return output, 200


class HeronProposed(Resource):
    """ Resource class for predicting a new packing plan for the topology, given its current or
    future traffic.  """

    def __init__(self, model_classes: List[Type], model_config: Dict[str, Any], traffic_config: Dict[str, Any],
                 metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient, tracker_url: str) -> None:
        self.metrics_client: HeronMetricsClient = metrics_client
        self.graph_client: GremlinClient = graph_client

        self.tracker_url: str = tracker_url
        self.model_config: Dict[str, Any] = model_config
        self.traffic_config: Dict[str, Any] = traffic_config

        self.models: Dict[str, HeronTopologyModel] = {}
        for model_class in model_classes:
            model = model_class(model_config, metrics_client, graph_client)
            self.models[model.name] = model

        self.CURRENT = "current"
        self.FUTURE = "future"

        super().__init__()

    def get(self, topology_id: str, traffic_source: str):
        """ Method handling get requests to the current topology packing plan
            modelling endpoint."""

        # Checking to make sure we have required arguments
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

        cluster = request.args.get("cluster")
        environ = request.args.get("environ")

        # Make sure we have a current graph representing the physical plan for
        # the topology
        graph_check(self.graph_client, self.model_config, self.tracker_url,
                    cluster, environ, topology_id)

        # Make sure we have a file containing all paths for the job
        paths_check(self.graph_client, self.model_config, cluster,
                    environ, topology_id)

        if "all" in request.args.getlist("model"):
            LOG.info("Running all configured Heron topology performance "
                     "models")
            models = self.models.keys()
        else:
            models = request.args.getlist("model")

        # Convert the request.args to a dict suitable for passing as **kwargs
        model_kwargs: Dict[str, Any] = \
            utils.convert_wimd_to_dict(request.args)

        # Remove the models list from the kwargs as it is only needed by this method
        model_kwargs.pop("model")
        model_kwargs.pop("cluster")
        model_kwargs.pop("environ")

        start, end = get_start_end_times(**model_kwargs)

        # traffic source can be one of two values -- current or future. If it is of a future value, we must first
        # create an object that gathers together future traffic information. Otherwise, if it is current, then we
        # simply propose a packing plan based on current information
        if traffic_source == self.CURRENT:
            traffic_provider: CurrentTraffic = CurrentTraffic(self.metrics_client, self.graph_client, topology_id,
                                                              cluster, environ, start, end, {}, **model_kwargs)
        elif traffic_source == self.FUTURE:
            # the predicted traffic variable is initialized by the future traffic. It contains functions to convert
            # the predicted traffic into arrival rates
            traffic_provider: PredictedTraffic = PredictedTraffic(self.metrics_client, self.graph_client,
                                                                  topology_id, cluster, environ, start, end,
                                                                  self.traffic_config, **model_kwargs)

        else:
            errors.append(
                {"type": "ValueError", "error": (f"{traffic_source} is an incorrect URI. Please either specify"
                                                 f" future or current as possible values and provide parameters"
                                                 f" accordingly.")})
            return errors, 400

        model_kwargs["zk.time.offset"] = self.model_config["zk.time.offset"]
        model_kwargs["heron.statemgr.root.path"] = self.model_config["heron.statemgr.root.path"]
        model_kwargs["heron.statemgr.connection.string"] = self.model_config["heron.statemgr.connection.string"]

        for model_name in models:
            LOG.info("Running topology packing plan model %s", model_name)
            model = self.models[model_name]
            results: list = model.predict_packing_plan(topology_id=topology_id,
                                                       cluster=cluster,
                                                       environ=environ,
                                                       start=start,
                                                       end=end,
                                                       traffic_provider=traffic_provider,
                                                       **model_kwargs)

        return results

