# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains the main program for Caladrius and will set up all
resources and start the API server """

import os
import sys
import logging
import argparse

from typing import Dict, Any

from caladrius import logs
from caladrius import loader
from caladrius.api.router import create_router

LOG: logging.Logger = logging.getLogger("caladrius.main")


def _create_parser() -> argparse.ArgumentParser:

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=("This is the command line interface for the Caladrius API"
                     " server"))

    parser.add_argument("-c", "--config", required=True,
                        help=("Path to the config file with data required by "
                              "all configured models and classes"))
    parser.add_argument("-q", "--quiet", required=False, action="store_true",
                        help=("Optional flag indicating if console log output "
                              "should be suppressed"))
    parser.add_argument("--debug", required=False, action="store_true",
                        help=("Optional flag indicating if debug level "
                              "information should be displayed"))

    return parser


if __name__ == "__main__":

    ARGS: argparse.Namespace = _create_parser().parse_args()

    try:
        CONFIG: Dict[str, Any] = loader.load_config(ARGS.config)
    except FileNotFoundError:
        print(f"Config file: {ARGS.config} was not found. Aborting...",
              file=sys.stderr)
        sys.exit(1)
    else:
        if not ARGS.quiet:
            print("\nStarting Caladrius API...\n")
            print(f"Loading configuration from file: {ARGS.config}")

    if not os.path.exists(CONFIG["log.file.dir"]):
        os.makedirs(CONFIG["log.file.dir"])

    LOG_FILE: str = CONFIG["log.file.dir"] + "/app.log"

    logs.setup(console=(not ARGS.quiet), logfile=LOG_FILE, debug=ARGS.debug)

    try:
        ROUTER = create_router(CONFIG)
    except ConnectionRefusedError as cr_err:
        if ARGS.quiet:
            print(str(cr_err), file=sys.stderr)
        sys.exit(1)

    ROUTER.run(debug=ARGS.debug)
