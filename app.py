""" This module contains the main program for Caladrius and will set up all
resources and start the API server """

import logging

from caladrius.logs import get_top_level_logger
from caladrius.api.router import create_router

if __name__ == "__main__":

    TOP_LOG: logging.Logger = get_top_level_logger()

    CONFIG: dict = {
        "model.traffic.heron" :
            ["caladrius.model.traffic.heron.dummy_traffic.DummyTrafficModel"]
        }

    ROUTER = create_router(CONFIG)

    ROUTER.run()
