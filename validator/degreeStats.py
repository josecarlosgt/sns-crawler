import numpy
import pymongo
from pymongo import MongoClient

from master.database.mongoDB import MongoDB
from parser.userInfo import UserInfo

class DegreeStats:

    def __init__(self, TIME_ID):
        client = MongoClient()
        self.db = client[MongoDB.DATABASE_NAME]
        self.NODES_COLLECTION = 'nodes' + TIME_ID

    def percentileStat(self, degree, data, q):
        print "{}th percentile for {}: {:,}".format(q, degree, round(numpy.percentile(data, q)))

    def run(self):
        nodes = self.db[self.NODES_COLLECTION].find({})
        inDegrees = []
        outDegrees = []
        for node in nodes:
            try:
                outDegrees.append(UserInfo.getNumber(node["following"]))
                inDegrees.append(UserInfo.getNumber(node["followers"]))
            except KeyError:
                pass

        print "DEGREES DESCRIPTIVE STATISTICS\n"
        print "Mean indegree: {:,}".format(round(numpy.mean(inDegrees), 2))
        print "Mean outdegree: {:,}".format(round(numpy.mean(outDegrees), 2))
        print ""
        print "Standard Variation indegree: {:,}".format(round(numpy.std(inDegrees), 2))
        print "Standard Variation outdegree: {:,}".format(round(numpy.std(outDegrees), 2))
        print ""
        print "Variance indegree: {:,}".format(round(numpy.var(inDegrees), 2))
        print "Variance outdegree: {:,}".format(round(numpy.var(outDegrees), 2))
        print ""
        print "Max indegree: {:,}".format(numpy.amax(inDegrees))
        print "Max outdegree: {:,}".format(numpy.amax(outDegrees))
        print ""

        for q in [60, 80, 90, 95, 96, 97, 98, 99]:
            self.percentileStat("indegree", inDegrees, q)
            self.percentileStat("outdegree", outDegrees, q)
            print ""
