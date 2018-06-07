""" This module contains convenience methods for logging output from
Caladrius."""

import logging

from sys import stdout

LOG: logging.Logger = logging.getLogger(__name__)

def setup(console: bool = True, logfile: str = None,
          debug: bool = False) -> None:
    """ This will set up the root Python logger instance and by default will
    attach a stream handler piping all output to stdout. However an optional
    output filename can be specified to preserve the logs. The dubug argument
    will set the log level to DEBUG and will included line numbers and function
    name information in the log output.

    Arguments:
        console (bool): Optional flag indicating if logs should be output to
                        standard out
        logfile (str):  Optional path to the output file for the logs.
        debug (bool):   Optional flag (default False) to include debug level
                        output.
    """

    # Capture warnings issued by packages like pandas and numpy
    logging.captureWarnings(True)

    # Grab the root logger
    top_log: logging.Logger = logging.getLogger()

    if top_log.hasHandlers():
        LOG.warning("Root Logger already has registered handlers. There may "
                    "be duplicate output.")

    if debug:
        top_log.setLevel(logging.DEBUG)
        formatter = logging.Formatter(("{levelname} | {name} | "
                                       "function: {funcName} "
                                       "| line: {lineno} | {message}"),
                                      style='{')
    else:
        top_log.setLevel(logging.INFO)
        formatter = logging.Formatter(("{asctime} | {name} | {levelname} "
                                       "| {message}"), style='{')

    if console:
        print("Logging to standard out", file=stdout)
        console_handler: logging.StreamHandler = logging.StreamHandler(stdout)
        console_handler.setFormatter(formatter)
        top_log.addHandler(console_handler)

    if logfile:
        LOG.info("Logging to file: %s", logfile)
        file_handler: logging.FileHandler = logging.FileHandler(logfile)
        file_handler.setFormatter(formatter)
        top_log.addHandler(file_handler)
