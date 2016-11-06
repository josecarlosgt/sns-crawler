import pymongo
from pymongo import MongoClient

from master.database.mongoDB import MongoDB

class CheckDegrees:

    def __init__(self, TIME_ID, DIRECTION):
        client = MongoClient()
        self.db = client[MongoDB.REMOTE_DATABASE_NAME]
        self.EDGES_COLLECTION = 'edges' + TIME_ID
        self.NODES_COLLECTION = 'nodes' + TIME_ID
        self.DIRECTION = DIRECTION

    def checkDegreesByDirection(self, node):
        edgesC = 0
        edgesP = 0
        if(self.DIRECTION == "inEdges"):
            edgesC = self.db[self.EDGES_COLLECTION].find(
                {"targetId": node["twitterID"]}
            ).count()
            edgesP = str(node["followers_count"])
        else:
            edgesC = self.db[self.EDGES_COLLECTION].find(
                {"sourceId": node["twitterID"]}
            ).count()
            edgesP = str(node["friends_count"])

        print ("RESULT: %s (protected) parsed/crawled for %s "
            "(%s) %s/%s ") %\
            (self.DIRECTION, node["twitterID"],
                node["protected"], edgesP, edgesC)

    def run(self):
        nodes = self.db[self.NODES_COLLECTION].find({})
        for node in nodes:

            outEdgesPI = 0
            inEdgesPI = 0

            self.checkDegreesByDirection(node)

            #if( ((outEdgesPI != outEdgesC) or
            #    (inEdgesPI != inEdgesC)) and not node["protected"] ):
            #    print ("ALERT (parsed/crawled) for %s "
            #        "Indegree (followers): %s/%s "
            #        "Outdegree (following): %s/%s") %\
            #        (node["twitterID"],
            #            inEdgesP, inEdgesC,
            #            outEdgesP, outEdgesC)
