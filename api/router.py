""" This is the routing logic for the Caladrius API. """

import logging

from flask import Flask
from flask_restful import Api

from caladrius.api.model.topology.heron import HeronCurrent, HeronProposed
from caladrius.api.model.traffic.heron import HeronTraffic
from caladrius.loader import get_class

LOG: logging.Logger = logging.getLogger(__name__)

def create_router(config: dict) -> Flask:

    LOG.info("Creating REST API routing object")

    router = Flask(__name__)
    api = Api(router)

    api.add_resource(HeronCurrent,
                     '/model/topology/heron/current/<string:topo_id>')
    api.add_resource(HeronProposed,
                     '/model/topology/heron/proposed/<string:topo_id>')

    # Get the list of heron traffic model classes
    # TODO: This class processing will need to move to its own method/class
    models: List[str] = config["model.traffic.heron"]
    model_classes: list = [get_class(model) for model in models]

    api.add_resource(HeronTraffic,
                     '/model/traffic/heron/<string:topo_id>',
                     resource_class_kwargs={ 'models': model_classes})

    LOG.info("REST API router created")

    return router
