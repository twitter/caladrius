from caladrius.metrics.heron.cuckoo.heron_cuckoo_client import \
        HeronCuckooClient

config = {"cuckoo.database.url": 'https://cuckoo-prod-smf1.twitter.biz'}


cuckoo = HeronCuckooClient(config, "Infra-Caladrius")

splitter_service_times = cuckoo.get_service_times("ossWordCount3", "splitter")
