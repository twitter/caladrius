""" This module provides helper methods for dealing with timestamps for the
cuckoo metrics database. """

import logging

import datetime as dt

from typing import Tuple

from caladrius.common.time.timestamp import get_window_dt_from_now

LOG: logging.Logger = logging.getLogger(__name__)

def convert_dt_to_ts(time_object: dt.datetime) -> int:
    """ Converts the supplied datetime object into a POSIX timestamp consisting
    of integer seconds since epoch.

    Arguments:
        time_object (dt.datetime):  The datetime object to be converted.

    Returns:
        Integer UNIX timestamp in seconds.
    """

    return int(round(time_object.timestamp()))

def get_window_ts_from_now(seconds: int = 0, minutes: float = 0.0,
                           hours: float = 0.0) -> Tuple[int, int]:
    """ Gets a (start timestamp, end timestamp) tuple where the end its now
    (UTC) and the start is calculated based on supplied arguments. Arguments
    can be supplied in one unit: seconds = 150, or as multiple units: minutes =
    2, seconds = 30 which will be combined. At least one of seconds, minutes or
    hours must be supplied.

    Arguments:
        seconds(int):   The duration in whole seconds
        minutes (float): The duration in minutes (can be fractional)
        hours (float):  The duration in hours (can be fractional)

    Returns:
        A (start, end) tuple where start and end are POSIX UTC integer
        timestamps. The float timestamps are rounded up before being cast to
        integers.

    Raises:
        RuntimeError:   If none of the required arguments are supplied.

    """

    start, end = get_window_dt_from_now(seconds=seconds, minutes=minutes,
                                        hours=hours)

    LOG.info("Calculating Cuckoo compatible UNIX UTC timestamps for window "
             "from %s seconds ago until now", str((end-start).total_seconds()))

    return convert_dt_to_ts(start), convert_dt_to_ts(end)
