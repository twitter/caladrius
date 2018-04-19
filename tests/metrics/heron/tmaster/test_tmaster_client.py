import datetime as dt

from caladrius.metrics.heron.tmaster.client import HeronTMasterClient

CONFIG = {"heron.tracker.url" :
          "http://heron-tracker-new.prod.heron.service.smf1.twitter.com"}

TMASTER_CLIENT = HeronTMasterClient(CONFIG)

END = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
START = END - dt.timedelta(seconds=600)

START_TS = int(START.timestamp())
END_TS = int(END.timestamp())

SERVICE_TIMES = TMASTER_CLIENT.get_component_service_times(
    "ossWordCount3", "smf1", "test", "splitter", START_TS, END_TS)
