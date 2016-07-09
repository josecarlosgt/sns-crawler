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
            inEdgesP = str(node["followers_count"])
            outEdgesP = str(node["friends_count"])
            outEdgesPI = 0
            inEdgesPI = 0

            inEdgesC = self.db[self.EDGES_COLLECTION].find(
                {"targetId": node["twitterID"]}
            ).count()
            outEdgesC = self.db[self.EDGES_COLLECTION].find(
                {"sourceId": node["twitterID"]}
            ).count()

            if( ((outEdgesPI != outEdgesC) or
                (inEdgesPI != inEdgesC)) and not node["protected"] ):
                print ("ALERT (parsed/crawled) for %s"
                    "Indegree (followers): %s/%s "
                    "Outdegree (following): %s/%s") %\
                    (node["twitterID"],
                        inEdgesP, inEdgesC,
                        outEdgesP, outEdgesC)
