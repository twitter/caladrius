from caladrius.metrics.heron.cuckoo.heron_cuckoo_client import \
        HeronTwitterCuckooClient

config = {"database.url": 'https://cuckoo-prod-{zone}.twitter.biz/',
          "default.zone": 'smf1'}


cuckoo = HeronTwitterCuckooClient(config, "Infra-Caladrius")

splitter_service_times = cuckoo.get_service_times("ossWordCount3", "splitter")
