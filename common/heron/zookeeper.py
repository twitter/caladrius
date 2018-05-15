""" This module contains helper methods for extracting heron information from a
zookeeper cluster."""

import re
import logging

import datetime as dt

import requests

LOG: logging.Logger = logging.getLogger(__name__)

TOPO_UPDATED_SEARCH_STR: str = \
    (r"mtime</td><td>(?P<date>\w+ \d+, \d+ \d+:\d+ \w.\w.) "
     r"\((?P<hours>\d+)\shours, (?P<mins>\d+)\sminutes ago\)")

def last_topo_update_ts(zk_connection: str, zk_root_node: str,
                        topology_id: str) -> dt.datetime:
    """ This method will attempt to obtain a timestamp of the most recent
    physical plan uploaded to the zookeeper cluster. To do this it simply
    parses the HTML returned by a GET request to pplan node for the specified
    topology.

    Arguments:
        zk_connection (str): The connection string for the zookeeper cluster.
        zk_root_node (str): The path to the root node used for Heron child
                            nodes.

    Returns:
        A datetime object representing the UTC datetime of the last update to
        the physical plan.

    Raises:
        requests.HTTPError: If a non-200 status code is returned by the get
                            request.
        RuntimeError:   If the returned HTML does not contain the required
                        information.
    """

    zk_str: str = \
        f"http://{zk_connection}/{zk_root_node}/pplans/{topology_id}"

    response: requests.Response = requests.get(zk_str)

    response.raise_for_status()

    result = re.search(TOPO_UPDATED_SEARCH_STR, response.text)

    if not result:
        err_msg: str = (f"Could not obtain physical plan update timestamp "
                        f"from zookeeper node at: {zk_str}")
        LOG.error(err_msg)
        raise RuntimeError(err_msg)

    hours: int = int(result.groupdict()["hours"])
    mins: int = int(result.groupdict()["mins"])

    last_updated_ts: dt.datetime = (dt.datetime.now(dt.timezone.utc) -
                                    dt.timedelta(hours=hours, minutes=mins))

    return last_updated_ts
