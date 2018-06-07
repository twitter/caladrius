# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains enum classes containing the various configuration keys
used throughout caladrius."""

from enum import Enum


class ConfKeys(Enum):
    """ Main enum class containing configuration keys for caladrius"""

    HERON_TRACKER_URL: str = "heron.tracker.url"
    HERON_TMASTER_METRICS_MAX_HOURS: str = "heron.tmaster.metrics.max.hours"

    GREMLIN_SERVER_URL: str = "gremlin.server.url"
