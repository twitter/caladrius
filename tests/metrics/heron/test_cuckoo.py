from caladrius.metrics.heron.cuckoo.heron_cuckoo_client import \
        HeronTwitterCuckooClient

config = {"database.url": 'https://cuckoo-prod-{zone}.twitter.biz/',
          "default.zone": 'smf1'}


cuckoo = HeronTwitterCuckooClient(config, "Infra-Caladrius")
