""" This is the rooting logic for the Apache Heron topology performance
modelling API """

from flask_restful import Resource, reqparse

class HeronCurrent(Resource):

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('arrival_rate', type=int, required=True,
                                 help='Arrival rate must be supplied')
        super().__init__()

    def get(self, topo_id: str):
        args = self.parser.parse_args()
        return (f"You want info on the currently running Heron topology: "
                f"{topo_id} if the arrival rate was {args['arrival_rate']}")

class HeronProposed(Resource):

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('model_id', type=int, required=True,
                                 help='Model ID must be supplied')
        super().__init__()

    def get(self, topo_id: str):
        args = self.parser.parse_args()
        msg:str = (f"Results requested for model: {args['model_id']} of "
                   f"topology: {topo_id}")
        return msg

    def post(self, topo_id: str):
        return 202
