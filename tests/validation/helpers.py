import datetime as dt

from typing import List, Tuple


def create_start_end_list(total_hours: float, period_length_secs: int
                          ) -> List[Tuple[dt.datetime, dt.datetime]]:

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
