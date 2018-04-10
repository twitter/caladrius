""" This module contains the API resources for the Apache Heron traffic
modelling """

import logging

from flask_restful import Resource, reqparse

LOG: logging.Logger = logging.getLogger(__name__)

class HeronTraffic(Resource):

    def __init__(self, models: list):
        self.models = models
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('duration', type=int, required=True,
                                 help='Duration must be supplied')
        self.parser.add_argument('units', type=str, required=False)
        super().__init__()

    def get(self, topo_id: str):

        args = self.parser.parse_args()

        duration: int = args["duration"]
        units: str = args.get("units", "m")

        LOG.info("%d%s traffic prediction requested for Heron topology: %s",
                 duration, units, topo_id)

        output = {}

        for model_class in self.models:
            #TODO: Figure out how to supply client connections objects?
            model = model_class()
            LOG.info("Running traffic prediction model %s for topology %s "
                     "over %d%s", model.name, topo_id, duration, units)
            output.update(model.predict_traffic(topo_id, duration))

        return output
