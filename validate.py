import sys
import json

from validator.checkDegrees import CheckDegrees
from validator.degreeStats import DegreeStats

config = ""
with open('./configuration.json', 'r') as f:
    config = json.load(f)
outputDir = config["Analysis"]["output"]
times = config["Analysis"]["times"]
egoId = config["Analysis"]["egoId"]

if(len(sys.argv) > 2):
    print 'Argument List: ', str(sys.argv)
    action = sys.argv[1]
    direction = sys.argv[2]
    if(action == "CHECK"):
        # Adjust function to check only degrees for ego connections
        # checkDegrees = CheckDegrees(times, direction)
        checkDegrees.run()
    elif(action == "STATS"):
        stats = DegreeStats(outputDir, egoId, times, direction)
        stats.run()
else:
    print ("Invalid argument list. \n"
        "\tTry: python validate.py CHECK inEdges \n"
        "\tor: python validate.py STATS inEdges")
