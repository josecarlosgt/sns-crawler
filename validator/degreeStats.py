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
        print "%sth percentile for %s: %s" % (q, degree, round(numpy.percentile(data, q)))

    def run(self):
        nodes = self.db[self.NODES_COLLECTION].find({})
        inDegrees = []
        outDegress = []
        for node in nodes:
            try:
                inDegrees.append(UserInfo.getNumber(node["following"]))
                outDegress.append(UserInfo.getNumber(node["followers"]))
            except KeyError:
                pass

        print "DEGREES DESCRIPTIVE STATISTICS"
        print "Mean indegree: %s" % numpy.mean(inDegrees)
        print "Mean outdegree: %s" % numpy.mean(outDegress)

        print "Standard Variation indegree: %s" % round(numpy.std(inDegrees), 2)
        print "Standard Variation outdegree: %s" % round(numpy.std(outDegress), 2)

        print "Variance indegree: %s" % round(numpy.var(inDegrees), 2)
        print "Variance outdegree: %s" % round(numpy.var(outDegress), 2)

        print "Max indegree: %s" % numpy.amax(inDegrees)
        print "Max outdegree: %s" % numpy.amax(outDegress)

        for q in [60,80,90, 99]:
            self.percentileStat("indegree", inDegrees, q)
            self.percentileStat("outdegree", outDegress, q)
