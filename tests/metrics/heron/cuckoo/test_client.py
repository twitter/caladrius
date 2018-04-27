from caladrius.metrics.heron.cuckoo.client import HeronCuckooClient

config = {"cuckoo.database.url": 'https://cuckoo-prod-smf1.twitter.biz'}

cuckoo = HeronCuckooClient(config, "Infra-Caladrius")

print("Cookoo metrics client available as: cuckoo")
