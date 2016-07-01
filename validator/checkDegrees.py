import pymongo
from pymongo import MongoClient

from master.database.mongoDB import MongoDB

class CheckDegrees:

    def __init__(self, TIME_ID):
        client = MongoClient()
        self.db = client[MongoDB.DATABASE_NAME]
        self.EDGES_COLLECTION = 'edges' + TIME_ID
        self.NODES_COLLECTION = 'nodes' + TIME_ID

    def run(self):
        nodes = self.db[self.NODES_COLLECTION].find({})
        for node in nodes:
            inEdgesP = node["followers"]
            outEdgesP = node["following"]
            outEdgesPI = 0
            inEdgesPI = 0
            try:
                outEdgesPI = int(outEdgesP.replace(',', ''))
                inEdgesPI = int(inEdgesP.replace(',', ''))
            except ValueError:
                print "PARSE ERROR"

            inEdgesC = self.db[self.EDGES_COLLECTION].find(
                {"targetId": node["twitterID"]}
            ).count()
            outEdgesC = self.db[self.EDGES_COLLECTION].find(
                {"sourceId": node["twitterID"]}
            ).count()

            if( ((outEdgesPI != outEdgesC) or
                (inEdgesPI != inEdgesC)) and not node["private"] ):
                print ("ALERT (parsed/crawled) for %s %s "
                    "Indegree (followers): %s/%s "
                    "Outdegree (following): %s/%s") %\
                    (node["twitterID"], node["collectorIP"], \
                        inEdgesP, inEdgesC,\
                        outEdgesP, outEdgesC)
