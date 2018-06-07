# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods used to help in the validation of Caladrius
performance and traffic models."""

import datetime as dt

from typing import List, Tuple


def create_start_end_list(total_hours: float, period_length_secs: int
                          ) -> List[Tuple[dt.datetime, dt.datetime]]:
    """ Helper method which creates a list of (start, end) datetime tuples for
    periods of the defined length, over the period from `total_hours` ago until
    now. The final period will not be of the defined length unless the total
    hours are a multiple of the period length."""

    now: dt.datetime = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

    start: dt.datetime = now - dt.timedelta(hours=total_hours)

    period: dt.timedelta = dt.timedelta(seconds=period_length_secs)

    end: dt.datetime = start + period

    output: List[Tuple[dt.datetime, dt.datetime]] = []

    while end < now:
        output.append((start, end))
        start = end
        end = start + period

    return output
