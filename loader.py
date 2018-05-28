""" This module contains functions for loading classes from configuration file
variables. """
import logging
import warnings

from importlib import import_module
from typing import Type, Dict, Any, List

import yaml

LOG: logging.Logger = logging.getLogger(__name__)


def get_class(class_path: str) -> Type:
    """ Method for loading a class from a absolute class path string.

    Arguments:
        class_path (str):   The absolute import path to the class. For example:
                            pkg.module.ClassName

    Returns:
        The class object referred to in the supplied class path.

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

    LOG.info("Successfully loaded class: %s from module: %s", class_name,
             module_path)

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


def get_model_classes(config: Dict[str, Any], dsps_name: str,
                      model_type: str) -> List[Type]:
    """ This method loads model classes from lists in the config dictionary and
    checks for name and description properties.

    Arguments:
        config (dict):  The main configuration dictionary containing the model
                        class paths under "{dsps_name}.{model_type}.models"
                        key.
        dsps_name (str):    The name of the streaming system whose models are
                            to be loaded.
        model_type (str):   The model type, traffic, topology etc.

    Returns:
        List[Type]: A list of Model Types.

    Raises:
        RuntimeError:   If a model class does not have a name class property
                        set or if the name property of one model is the same
                        as another in the list.
        UserWarning:    If a model class does not have the description class
                        property set.
    """
    model_classes: List[Type] = []
    model_names: List[str] = []

    for model in config[f"{dsps_name}.{model_type}.models"]:
        model_class: Type = get_class(model)

        if model_class.name == "base":
            name_msg: str = (f"Model {str(model_class)} does not have a "
                             f"'name' class property defined. This is required"
                             f" for it to be correctly identified in the API.")
            LOG.error(name_msg)
            raise RuntimeError(name_msg)

        if model_class.name in model_names:
            other_model_index: int = model_names.index(model_class.name)
            dup_msg: str = (f"The model {str(model_class)} has the same 'name'"
                            f" class property as "
                            f"{str(model_classes[other_model_index])}. The "
                            f"names of models should be unique.")
            LOG.error(dup_msg)
            raise RuntimeError(dup_msg)

        if model_class.description == "base":
            desc_msg: str = (f"Model {str(model_class)} does not have a "
                             f"'description' class property defined. This is "
                             f"recommended for use in the API.")
            LOG.warning(desc_msg)
            warnings.warn(desc_msg)

        model_classes.append(model_class)
        model_names.append(model_class.name)

    return model_classes
