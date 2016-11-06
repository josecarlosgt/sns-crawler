import numpy
import pymongo
from pymongo import MongoClient

from master.database.mongoDB import MongoDB
from profile_parser.userInfo import UserInfo

class DegreeStats:

    def __init__(self, CONF, EGO_ID, TIMES, DIRECTION):
        client = MongoClient()
        self.db = client[MongoDB.REMOTE_DATABASE_NAME]
        self.CONF = CONF + "/" + "degreeStats-"
        self.TIMES = TIMES
        self.EGO_ID = EGO_ID
        self.DIRECTION = DIRECTION

    def percentileStat(self, data, q):
        print "{}th percentile for: {:,}".format(q, round(numpy.percentile(data, q)))

    def generateStats(self, timeId, nodes, degrees):
        if len(degrees) == 0:
            print "Degree frequencies is emtpy."
        else:
            print "DEGREES DESCRIPTIVE STATISTICS %s %s \n" % (self.DIRECTION, timeId)
            print "Mean: {:,}".format(round(numpy.mean(degrees), 2))
            print ""
            print "Standard Variation: {:,}".format(round(numpy.std(degrees), 2))
            print ""
            print "Variance: {:,}".format(round(numpy.var(degrees), 2))
            print ""
            print "Max: {:,}".format(numpy.amax(degrees))
            print ""

            for q in [60, 80, 90, 95, 96, 97, 98, 99]:
                self.percentileStat(degrees, q)
                print ""

        output = self.CONF + timeId
        fo = open(output, "w")
        s_degrees = sorted(degrees)
        for d in s_degrees:
            fo.write("%s\n" % d);
        fo.close()

    def run(self):
        for timeId in self.TIMES:

            inEdges = self.db['edges' + timeId].find(
                {"targetId": self.EGO_ID}
            )
            inEdgesS = []
            for edge in inEdges:
                inEdgesS.append(edge["sourceId"])

            if len(inEdgesS) > 0:
                nodes = self.db['nodes' + timeId].find(
                    {"twitterID":{"$in":inEdgesS}})
                degrees = []
                for node in nodes:
                    try:
                        if(self.DIRECTION == "inEdges"):
                            degrees.append(node["followers_count"])
                        else:
                            degrees.append(node["friends_count"])
                    except KeyError:
                        pass

                self.generateStats(timeId, nodes, degrees)
            else:
                print "Empty nodes."
