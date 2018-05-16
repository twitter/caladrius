""" This module contains the methods to load the configured model and client
classes and create routing logic for the Caladrius API. """

import logging
import warnings

from typing import List, Dict, Any, Type

from flask import Flask
from flask_restful import Api

from caladrius import loader
from caladrius.config.keys import ConfKeys
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.api.model.topology.heron import \
    HeronTopologyModels, HeronCurrent, HeronProposed
from caladrius.api.model.traffic.heron import HeronTraffic, HeronTrafficModels

LOG: logging.Logger = logging.getLogger(__name__)

def get_model_classes(config: Dict[str, Any], dsps_name: str,
                      model_type: str) -> List[Type]:
    """ Helper method that loads model classes and checks for name and
    description properties.

    Arguments:
        config (dict):  The main configuration dictionary containing the model
                        class paths under "{dsps_name}.{model_type}.models"
                        key.
        dsps_name (str):    The name of the streaming system whose models are
                            to be loaded.
        model_type (str):   The model type, traffic, topology etc.

    Returns:
        A list of Model Types.

    Raises:
        RuntimeError:   If a model class does not have a name class property
                        set and also if the name property of one model is the
                        same as another.
        UserWarning:    If a model class does not have the description class
                        property set.
    """
    model_classes: List[Type] = []
    model_names: List[str] = []

    for model in config[f"{dsps_name}.{model_type}.models"]:
        model_class: Type = loader.get_class(model)

        if model_class.name == "base":
            name_msg: str = (f"Model {str(model_class)} does not have a "
                             f"'name' class property defined. This is required"
                             f" for it to be correctly identified in the API.")
            LOG.error(name_msg)
            raise RuntimeError(name_msg)

        if model_class.name in model_names:
            other_model_index: int = model_names.index(model_class.name)
            dup_msg: str = (f"The model {str(model_class)} has the same 'name'"
                            f" class property as "
                            f"{str(model_classes[other_model_index])}. The "
                            f"names of models should be unique.")
            LOG.error(dup_msg)
            raise RuntimeError(dup_msg)

        if model_class.description == "base":
            desc_msg: str = (f"Model {str(model_class)} does not have a "
                             f"'description' class property defined. This is "
                             f"recommended for use in the API.")
            LOG.warning(desc_msg)
            warnings.warn(desc_msg)

        model_classes.append(model_class)
        model_names.append(model_class.name)

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
            get_model_classes(config, "heron", "traffic")

    api.add_resource(HeronTrafficModels,
                     "/model/traffic/heron/model_info",
                     resource_class_kwargs={
                         'model_classes': heron_traffic_model_classes})

    api.add_resource(
        HeronTraffic,
        '/model/traffic/heron/<string:topology_id>',
        resource_class_kwargs={
            'model_classes': heron_traffic_model_classes,
            'model_config': config["heron.traffic.models.config"],
            'metrics_client' : heron_metrics_client,
            'graph_client': graph_client,
            'tracker_url' : config[ConfKeys.HERON_TRACKER_URL.value]})

    #### TOPOLOGY MODEL ENDPOINTS ####

    heron_topology_model_classes: List[Type] = \
            get_model_classes(config, "heron", "topology")

    #### MODEL INFORMATION ENDPOINT ####

    api.add_resource(
        HeronTopologyModels,
        "/model/topology/heron/model_info",
        resource_class_kwargs={
            'model_classes': heron_topology_model_classes})

    #### CURRENT TOPOLOGY MODELS ####

    api.add_resource(
        HeronCurrent, '/model/topology/heron/current/<string:topology_id>',
        resource_class_kwargs={
            'model_classes': heron_topology_model_classes,
            'model_config': config["heron.topology.models.config"],
            'metrics_client' : heron_metrics_client,
            'graph_client': graph_client,
            'tracker_url' : config[ConfKeys.HERON_TRACKER_URL.value]}
        )

    #### PROPOSED TOPOLOGY MODELS ####

    api.add_resource(HeronProposed,
                     '/model/topology/heron/proposed/<string:topology_id>')

    LOG.info("REST API router created")

    return router
