import datetime
import json
from master.master import Master

today = datetime.date.today()
ftoday = today.strftime('%d_%m_%Y')

config = ""
with open('./configuration.json', 'r') as f:
    config = json.load(f)

master = Master(ftoday, config)
master.start()
