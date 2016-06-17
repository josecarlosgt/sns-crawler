import json
from master.master import Master

config = ""
with open('./configuration.json', 'r') as f:
    config = json.load(f)

master = Master(config)
master.start()
