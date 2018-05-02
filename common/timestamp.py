""" This module provides general helper methods for dealing with timestamps
database. """

import logging

import datetime as dt

from typing import Tuple

LOG: logging.Logger = logging.getLogger(__name__)

def get_window_dt_from_now(seconds: int = 0, minutes: float = 0.0,
                           hours: float = 0.0
                          ) -> Tuple[dt.datetime, dt.datetime]:
    """ Gets a (start datetime, end datetime) tuple where the end its now
    (UTC) and the start is calculated based on supplied arguments. Arguments
    can be supplied in one unit: seconds = 150, or as multiple units: minutes =
    2, seconds = 30 which will be combined. At least one of seconds, minutes or
    hours must be supplied.

    Arguments:
        seconds(int):   The duration in whole seconds
        minutes (float): The duration in minutes (can be fractional)
        hours (float):  The duration in hours (can be fractional)

    Returns:
        A (start, end) tuple where start and end are UTC datetime objects.

    Raises:
        RuntimeError:   If none of the required arguments are supplied.

    """

    if not any((seconds, minutes, hours)):
        msg: str = ("At least one of the hours, minutes or seconds arguments "
                    "should be supplied")
        LOG.error(msg)
        raise RuntimeError(msg)

    # Set the end timestamp to be now
    end: dt.datetime = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

    duration = dt.timedelta(seconds=seconds, minutes=minutes, hours=hours)

    start: dt.datetime = end - duration

    return (start, end)