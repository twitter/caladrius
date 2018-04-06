""" This is the routing logic for the Caladrius API. """

from flask import Flask
from flask_restful import Api

from caladrius.api.model.topology.heron import HeronCurrent, HeronProposed

app = Flask(__name__)
api = Api(app)

api.add_resource(HeronCurrent, '/model/topology/heron/current/<string:topo_id>')
api.add_resource(HeronProposed, '/model/topology/heron/proposed/<string:topo_id>')

