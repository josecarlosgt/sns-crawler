import datetime

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class MongoDB:
    # Constants
    CLASS_NAME="MongoDB"
    DATABASE_NAME = "research_db"

    def __init__(self, timeId, logger):
        self.timeId = timeId;
        self.logger = logger;

        client = MongoClient()
        self.db = client[self.DATABASE_NAME]
        self.logger.info(str(self.db))

    # Breadth first search queue operations

    def retrieveBFSQ(self, level, limit):
        queue = self.db.BFSQ
        options = {"level": level, "visited": False}

        if(limit > 0):
            return queue.find(options, no_cursor_timeout=True).limit(limit)
        else:
            return queue.find(options, no_cursor_timeout=True)

    def updateBFSQ(self, id, level):
        queue = self.db.BFSQ
        try:
            node = { "_id": id, "level": level, "visited": False}
            queue.insert_one(node)
            self.logger.info("Node %s added in BSF queue level %i" % (id, level))

        except DuplicateKeyError:
            self.logger.info("Node %s already exists in BSF queue level %i" % (id, level))

            return False

        return True

    def removeBFSQ(self, id):
        queue = self.db.BFSQ

        queue.update_one({ "_id": id }, { "$set": {"visited": True} })
        self.logger.info("Node %s checked as visited in BSF queue" % id)

        return True

    def clearBFSQ(self):
        queue = self.db.BFSQ

        queue.drop()
        self.logger.info("BFS queue cleared")

        return True

# Create indexes

    def createEdgesIndex(self):
        self.logger.info(
            "Creating indexes for edges collection: %s" % 'edges' + self.timeId)
        self.db['edges' + self.timeId].create_index([
            ('sourceId', pymongo.ASCENDING),
            ('targetId', pymongo.ASCENDING)],
        unique=True);

    def createNodesIndex(self):
        self.logger.info(
            "Creating indexes for nodes collection")
        self.db['nodes'].create_index([
            ('twitterID', pymongo.ASCENDING)],
        unique=True);

    def createNodesValidatorIndex(self):
        self.logger.info(
            "Creating indexes for collection: %s" % 'nodes' + self.timeId)
        self.db['nodes' + self.timeId].create_index([
            ('twitterID', pymongo.ASCENDING)],
        unique=True);

    # Network operations

    def insertNode(self, info, id):
        nodesCollection = self.db.nodes
        nodesValidatorCollection = self.db['nodes' + self.timeId]
        node = {
            "twitterID": id,
            "initialTimeId": self.timeId,
            "name": info.name,
            "image": info.image,
            "webSite": info.webSite,
            "location": info.location
        }
        nodeValidator = {
            "twitterID": id,
            "timeId": self.timeId,
            "tweets": info.tweets,
            "following": info.following,
            "followers": info.followers,
            "favourites": info.favourites,
            "private": info.private
        }
        try:
            nodesCollection.insert_one(node)
            self.logger.info("Adding node to network: %s" % id)
        except DuplicateKeyError:
            pass

        try:
            nodesValidatorCollection.insert_one(nodeValidator)
            self.logger.info("Adding node validator to network: %s" % id)
        except DuplicateKeyError:
            pass

        return True

    def insertEdge(self, sourceId, targetId):
        edgesCollection = self.db['edges' + self.timeId]
        try:
            edgesCollection.insert_one(
                {"sourceId": sourceId, "targetId": targetId})
            self.logger.info("Adding edge to network: (%s, %s)" %
                (sourceId, targetId))
        except DuplicateKeyError:
            return False

        return True
