# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains helper methods for the REST API. """
from typing import Dict, List, Any, Hashable

from werkzeug.datastructures import ImmutableMultiDict


def convert_wimd_to_dict(wimd: ImmutableMultiDict) -> dict:
    """ Converts the supplied werkzeug ImmutableMultiDict object into a regular
    dictionary where items with single entries are re-mapped so the key points
    to that single value instead of a list. This method is used for converting
    the flask `requests.args` into a structure suitable for passing as
    `**kwargs` to a generic model method.

    Arguments:
        wimd (ImmutableMultiDict):  The object to be converted.

    Returns:
        dict:   A dictionary where keys with single value map to that value and
                not to a list.
    """

    list_dict: Dict[Hashable, List[Any]] = wimd.to_dict(flat=False)

    output: Dict[Hashable, Any] = {}

    for key, value_list in list_dict.items():

        if len(value_list) == 1:
            output[key] = value_list[0]
        else:
            output[key] = value_list

    return output
