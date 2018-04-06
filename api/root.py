""" This is the routing logic for the Caladrius API. """

import hug

from caladrius.api.model import root as model_root

@hug.extend_api("/model")
def model_api():
    return [model_root]
