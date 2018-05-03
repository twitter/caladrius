""" This module contains enum classes containing the various configuration keys
used throughout caladrius."""

from enum import Enum

class ConfKeys(Enum):
    """ Main enum class containing configuration keys for caladrius"""

    HERON_TRACKER_URL: str = "heron.tracker.url"

    GREMLIN_SERVER_URL: str = "gremlin.server.url"

    CUCKOO_SERVER_URL: str = "cuckoo.database.url"
    CUCKOO_CLIENT_NAME: str = "cuckoo.client.name"
