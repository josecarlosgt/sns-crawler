import sys
import json

from analysis.triadics import Triadics
from analysis.linkages import Linkages

config = ""
with open('./configuration.json', 'r') as f:
    config = json.load(f)
outputDir = config["Analysis"]["output"]
times = config["Analysis"]["times"]
egoId = config["Analysis"]["egoId"]

# triadics = Triadics(
#    outputDir,
#    egoId,
#    times
#)
# triadics.find()

linkages = Linkages(
    outputDir,
    egoId,
    times
)
linkages.find()
