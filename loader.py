""" This module contains functions for loading classes from configuration file
variables. """

import logging

from importlib import import_module
from typing import Type, Dict, Any

import yaml

LOG: logging.Logger = logging.getLogger(__name__)

def get_class(class_path: str) -> Type:
    """ Method for loading a class from a absolute class path string.

    Arguments:
        class_path (str):   The absolute import path to the class. For example:
                            pkg.module.ClassName

    Returns:
        The class object reffered to in the supplied class path.

    Raises:
        ModuleNotFoundError:    If the module part of the class path could not
                                be found.
        AttributeError: If the module could be found but the specified class
                        name was not defined within it.
    """
    LOG.info("Loading class: %s", class_path)

    module_path, class_name = class_path.rsplit(".", 1)

    try:
        module = import_module(module_path)
    except ModuleNotFoundError as mnf_err:
        LOG.error("Module %s could not be found", module_path)
        raise mnf_err

    try:
        found_class = module.__getattribute__(class_name)
    except AttributeError as att_err:
        LOG.error("Class %s is not part of module %s", class_name, module_path)
        raise att_err

    return found_class

def load_config(file_path: str) -> Dict[str, Any]:
    """ Converts the yaml file at the supplied path to a dictionary.

    Arguments:
        file_path (str): The path to the yaml formatted configuration file.

    Returns:
        A dictionary formed from the supplied yaml file.
    """

    LOG.info("Loading yaml file at: %s", file_path)

    with open(file_path, "r") as yaml_file:

        yaml_dict: Dict[str, Any] = yaml.load(yaml_file)

    return yaml_dict
