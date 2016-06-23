import sys

from validator.checkDegrees import CheckDegrees

if(len(sys.argv) > 1):
    print 'Argument List: ', str(sys.argv)
    timeId = sys.argv[1]
    checkDegrees = CheckDegrees(timeId)
    checkDegrees.run()
else:
    print "Invalid argument list"
