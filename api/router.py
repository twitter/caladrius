""" This module contains the methods to load the configured model and client
classes and create routing logic for the Caladrius API. """

import logging

from typing import List, Dict, Any, Type

from flask import Flask
from flask_restful import Api

from caladrius import loader
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.api.model.topology.heron import HeronCurrent, HeronProposed
from caladrius.api.model.traffic.heron import HeronTraffic

LOG: logging.Logger = logging.getLogger(__name__)

def _load_traffic_models(config: Dict[str, Any], dsps_name: str) -> List[Type]:

    model_classes: List[Type] = [loader.get_class(model) for model in
                                 config[f"{dsps_name}.traffic.models"]]

    return model_classes

def create_router(config: Dict[str, Any]) -> Flask:
    """ Creates the Flask router object by first creating all the client and
    model classes defined in the supplied configuration dictionary.

    Arguments:
        config (dict):  The configuration dictionary containing client and
                        model class paths and their associated configurations

    Returns:
        A Flask instance containing the routing logic for the API.
    """

    LOG.info("Creating REST API routing object")

    router: Flask = Flask("caladrius")
    api: Api = Api(router)

    #### GRAPH CLIENT ###

    # TODO: Consider making a copy of this for each model/resource to prevent
    # locking issue if we go multi-threaded
    graph_client: GremlinClient = \
        loader.get_class(config["graph.client"])(config["graph.client.config"])

    #### HERON METRICS CLIENT ####

    # TODO: Consider making a copy of this for each heron model to prevent
    # locking issues if we go multi-threaded
    heron_metrics_client: HeronMetricsClient = \
        loader.get_class(config["heron.metrics.client"])(
            config["heron.metrics.client.config"])

    #### TRAFFIC MODEL ENDPOINTS ####

    heron_traffic_model_classes: List[Type] = \
        _load_traffic_models(config, "heron")

    api.add_resource(HeronTraffic,
                     '/model/traffic/heron/<string:topo_id>',
                     resource_class_kwargs={
                         'model_classes': heron_traffic_model_classes,
                         'model_config': config["heron.traffic.models.config"],
                         'metrics_client' : heron_metrics_client,
                         'graph_client': graph_client}
                    )

    #### CURRENT TOPOLOGY MODELS ####

    api.add_resource(HeronCurrent,
                     '/model/topology/heron/current/<string:topo_id>')

    #### PROPOSED TOPOLOGY MODELS ####

    api.add_resource(HeronProposed,
                     '/model/topology/heron/proposed/<string:topo_id>')

    LOG.info("REST API router created")

    return router
