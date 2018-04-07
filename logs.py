""" This module contains convenience methods for logging output from
Caladrius."""

import logging

from sys import stdout

def get_top_level_logger(logfile: str = None,
                         debug: bool = False) -> logging.Logger:
    """ This will get a Logger instance configured for the topo level of the
    program so will capture all output from Loggers within Caladrius. By
    default this will pass the output to stdout however an optional output file
    can be specified to preserve the logs. The dubug argument will set the log
    level to DEBUG and will included line numbers and function name information
    in the log output.

    Arguments:
        logfile (str):  Optional path to the output file for logs.
        debug (bool):   Optional flag (default False) to include debug level
                        output.

    Returns:
        A logging.Logger instance configure for the top level of the system.
    """

    top_log: logging.Logger = logging.getLogger("caladrius")

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

    console_handler: logging.StreamHandler = logging.StreamHandler(stdout)
    console_handler.setFormatter(formatter)
    top_log.addHandler(console_handler)

    if logfile:
        file_handler: logging.FileHandler = logging.FileHandler(logfile)
        file_handler.setFormatter(formatter)
        top_log.addHandler(file_handler)

    return top_log
