import datetime

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class MongoDB:
    # Constants
    CLASS_NAME="MongoDB"
    DATABASE_NAME = "research_db"
    TWITTER_SNAME_ID_KEY = "twitterSN"
    TWITTER_ID_KEY = "twitterID"

    def __init__(self, timeId, logger):
        self.timeId = timeId;
        self.logger = logger;

        client = MongoClient()
        self.db = client[self.DATABASE_NAME]
        self.logger.info(str(self.db))

    # Breadth first search queue operations

    def retrieveBFSQ_0(self, options, limit, hasProfile):
        options.update({"hasProfile": hasProfile})

        queue = self.db.BFSQ

        if(limit > 0):
            return queue.find(options, no_cursor_timeout=True).limit(limit)
        else:
            return queue.find(options, no_cursor_timeout=True)

    def retrieveBFSQ(self, level, limit, hasProfile):
        options = {"level": level}

        return self.retrieveBFSQ_0(options, limit, hasProfile)

    # ORDER BY: Followers count descending
    def retrieve4FollowersBFSQ(self, level, limit):
        options = {"level": level, "followersVisited": False}

        return self.retrieveBFSQ_0(options, limit, True)

    def retrieve4FollowingBFSQ(self, level, limit):
        options = {"level": level, "followingVisited": False}

        return self.retrieveBFSQ_0(options, limit, True)

    def enqueBFSQ(self, nodeData, level):
        queue = self.db.BFSQ
        try:
            node = { "level": level,
                "followersVisited": False,
                "followingVisited": False,
                "hasProfile": False }
            node.update(nodeData)
            queue.insert_one(node)
            self.logger.info("Node added in BSF queue level <%i>: %s" %\
                (level, node))

        except DuplicateKeyError:
            self.logger.info("Node already exists in BSF queue level <%i>: %s" %\
                (level, node))

            return False

        return True

    def removeBFSQ(self, profile):
        queue = self.db.BFSQ

        result1 = queue.remove({ self.TWITTER_ID_KEY: profile["id_str"] })
        result2 = queue.remove({ self.TWITTER_SNAME_ID_KEY: profile["screen_name"] })
        self.logger.info("Profile is deleted from BFSQ %s / %s, results: %s / %s" %\
            (profile["id_str"], profile["screen_name"], result1, result2))

    def updateBFSQ(self, profile):
        queue = self.db.BFSQ

        result1 = queue.update_one({ self.TWITTER_ID_KEY: profile["id_str"] },
            { "$set": {
                self.TWITTER_SNAME_ID_KEY: profile["screen_name"],
                "hasProfile": True,
                "cursor": -1
            }})
        result2 = queue.update_one({ self.TWITTER_SNAME_ID_KEY: profile["screen_name"] },
            { "$set": {
                self.TWITTER_ID_KEY: profile["id_str"],
                "hasProfile": True,
                "cursor": -1
            }})

        self.logger.info("Node updated in BSF queue: %s / %s, results: %s / %s" %\
            (profile["id_str"], profile["screen_name"], result1, result2))

        return True

    def dequeBFSQ(self, id, visitedType):
        queue = self.db.BFSQ

        queue.update_one({ self.TWITTER_ID_KEY: id },
            { "$set": {visitedType: True} })
        self.logger.info("Node <%s> checked as visited in BSF queue" % id)

        return True

    def deque4FollowersBFSQ(self, id):
        return self.dequeBFSQ(id, "followersVisited")

    def deque4FollowingBFSQ(self, id):
        return self.dequeBFSQ(id, "followingVisited")

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
            (self.TWITTER_ID_KEY, pymongo.ASCENDING),
            (self.TWITTER_SNAME_ID_KEY, pymongo.ASCENDING)],
        unique=True);

    def createBFSQIndex(self):
        self.logger.info(
            "Creating indexes for BFSQ collection")
        self.db['BFSQ'].create_index([
            (self.TWITTER_ID_KEY, pymongo.ASCENDING),
            (self.TWITTER_SNAME_ID_KEY, pymongo.ASCENDING)],
        unique=True);

    def createNodesValidatorIndex(self):
        self.logger.info(
            "Creating indexes for collection: %s" % 'nodes' + self.timeId)
        self.db['nodes' + self.timeId].create_index([
            (self.TWITTER_ID_KEY, pymongo.ASCENDING)],
        unique=True);

    # Network operations

    def insertNode(self, profile):
        nodesCollection = self.db.nodes
        nodesValidatorCollection = self.db['nodes' + self.timeId]
        node = {
            self.TWITTER_ID_KEY: profile["id_str"],
            self.TWITTER_SNAME_ID_KEY: profile["screen_name"],
            "initialTimeId": self.timeId,

            "name": profile["name"],
            "profile_image_url": profile["profile_image_url"],
            "url": profile["url"],
            "location": profile["location"]
        }
        nodeValidator = {
            self.TWITTER_ID_KEY: profile["id_str"],
            "timeId": self.timeId,

            "statuses_count": profile["statuses_count"],
            "friends_count": profile["friends_count"],
            "followers_count": profile["followers_count"],
            "favourites_count": profile["favourites_count"],
            "retweet_count": profile["retweet_count"],
            "protected": profile["protected"]
        }
        try:
            nodesCollection.insert_one(node)
            self.logger.info("Adding node to network: %s" %  node)
        except DuplicateKeyError:
            pass

        try:
            nodesValidatorCollection.insert_one(nodeValidator)
            self.logger.info("Adding node validator to network: %s" % nodeValidator)
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
