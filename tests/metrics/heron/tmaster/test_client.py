from caladrius.metrics.heron.tmaster.client import HeronTMasterClient
from caladrius.common.timestamp import  get_window_dt_from_now

CONFIG = {"heron.tracker.url" :
          "http://heron-tracker-new.prod.heron.service.smf1.twitter.com"}

TMASTER_CLIENT = HeronTMasterClient(CONFIG)

START, END = get_window_dt_from_now(seconds=600)

print("Tmaster Metrics Client available as: TMASTER_CLIENT")
