import sys

from validator.checkDegrees import CheckDegrees
from validator.degreeStats import DegreeStats

if(len(sys.argv) > 2):
    print 'Argument List: ', str(sys.argv)
    timeId = sys.argv[1]
    action = sys.argv[2]
    if(action == "CHECK"):
        checkDegrees = CheckDegrees(timeId)
        checkDegrees.run()
    elif(action == "STATS"):
        stats = DegreeStats(timeId)
        stats.run()
else:
    print "Invalid argument list"
