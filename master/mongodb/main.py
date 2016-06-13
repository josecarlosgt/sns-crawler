import datetime

# INITIALIZE mongo database client

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

def initDB():
    client = MongoClient()
    db = client.research_db
    print db

    return db

# Breadth first search queue operations

def retrieveBFSQ(db, level):
    queue = db.BFSQ

    return queue.find({"level": level, "visited": False}, no_cursor_timeout=True)

def updateBFSQ(db, id, level):
    queue = db.BFSQ
    try:
        node = { "_id": id, "level": level, "visited": False}
        queue.insert_one(node)
        print "DB: Node %s added in level %i" % (id, level)

    except DuplicateKeyError:
        print "**DB** Node %s already exists in level %i" % (id, level)

        return False

    return True

def removeBFSQ(db, id):
    queue = db.BFSQ

    queue.update_one({ "_id": id }, { "$set": {"visited": True} })
    print "DB: Node %s checked as visited" % id

    return True

def clearBFSQ(db):
    queue = db.BFSQ

    queue.drop()

    return True

# Network operations

def insertNode(db, info, timeId, id):
    nodesCollection = db.nodes
    nodeResult = nodesCollection.find({"twitterID": id, "timeId": timeId})
    if(nodeResult.count() == 0):
            node = {
                "twitterID": id,
                "timeId": timeId,
                "tweets": info.tweets,
                "following": info.following,
                "followers": info.followers,
                "favourites": info.favourites,
                "name": info.name,
                "image": info.image,
                "webSite": info.webSite,
                "location": info.location
            }
            nodesCollection.insert_one(node)
    else:
        print "**DB** Node %s already exists for time %s" % (id, timeId)

        return False

    return True

def insertEdge(db, sourceId, targetId, timeId):
    edgesCollection = db.edges
    edgeResult = edgesCollection.find(
        {"sourceId": sourceId, "targetId": targetId})
    if(edgeResult.count() == 0):
        edgesCollection.insert_one(
            {"sourceId": sourceId, "targetId": targetId, "timeId": timeId})
    else:
        print "**DB** Edge %s to %s already exists for time %s" % (sourceId, targetId, timeId)

        return False

    return True
