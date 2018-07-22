# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains helper methods for extracting heron information from a
zookeeper cluster."""

import re
import logging

import datetime as dt

from typing import Dict

import requests

LOG: logging.Logger = logging.getLogger(__name__)

TOPO_UPDATED_SEARCH_STR: str = \
    r"ctime<\/td><td>(?P<date>\w+.? \d+, \d+ \d+[:\d+]* \w.\w.)\s\([\w|\d|\s|\,]*ago\)"

DATE_FORMAT: str = "%B %d, %Y %I:%M %p"
OLD_DATE_FORMAT: str = "%b %d, %Y %I:%M %p"
NO_MINS_DATE_FORMAT: str = "%B %d, %Y %I %p"


def last_topo_update_ts(zk_connection: str, zk_root_node: str,
                        topology_id: str, zk_time_offset: int = 0
                        ) -> dt.datetime:
    """ This method will attempt to obtain a timestamp of the most recent
    physical plan uploaded to the zookeeper cluster. To do this it simply
    parses the HTML returned by a GET request to pplan node for the specified
    topology.

    Arguments:
        zk_connection (str): The connection string for the zookeeper cluster.
        zk_root_node (str): The path to the root node used for Heron child
                            nodes.
        topology_id (str): The topology identification string.
        zk_time_offset (int): Optional offset amount for the Zookeeper server
                              clock in hours from UTC. If not supplied it will
                              be assumed that the times given by zookeeper are
                              in UTC.

    Returns:
        A timezone aware datetime object representing the time of the last
        update to the physical plan.

    Raises:
        requests.HTTPError: If a non-200 status code is returned by the get
                            request.
        RuntimeError:   If the returned HTML does not contain the required
                        information.
    """

    zk_str: str = \
        f"http://{zk_connection}/{zk_root_node}/pplans/{topology_id}/"

    response: requests.Response = requests.get(zk_str)

    response.raise_for_status()

    result = re.search(TOPO_UPDATED_SEARCH_STR, response.text)

    if not result:
        err_msg: str = (f"Could not obtain physical plan update timestamp "
                        f"from zookeeper node at: {zk_str}")
        LOG.error(err_msg)
        LOG.debug("Text returned from Zookeeper node page: %s", response.text)
        raise RuntimeError(err_msg)

    time_dict: Dict[str, str] = result.groupdict()

    time_str: str = time_dict["date"].replace(".", "")

    try:
        last_updated: dt.datetime = dt.datetime.strptime(time_str, DATE_FORMAT)
    except ValueError:
        try:
            last_updated = dt.datetime.strptime(time_str, OLD_DATE_FORMAT)
        except ValueError:
            last_updated = dt.datetime.strptime(time_str, NO_MINS_DATE_FORMAT)

    zk_tz: dt.timezone = dt.timezone(dt.timedelta(hours=zk_time_offset))

    last_updated_tz: dt.datetime = last_updated.replace(tzinfo=zk_tz)

    return last_updated_tz
