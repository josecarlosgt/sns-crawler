import copy
import time
from requests_oauthlib import OAuth1
import requests
import json

from database.mongoDB import MongoDB
from logger import Logger
from configKeys import ConfigKeys
from processor import Processor

class MasterFollowers():
    # Constants
    CLASS_NAME="MasterFollowers"

    def __init__(self,
        level,
        logger, db,
        config):

        self.level = level
        self.logger = logger
        self.db = db
        self.config = config

    def start(self):
        self.logger.info("CREATING THREADS for level <%i>" %\
            self.level)

        hasMore = True
        # Retrieve nodes from previous level
        nodes = self.db.retrieve4FollowersBFSQ(self.level - 1,
            self.config[ConfigKeys.BFS][ConfigKeys.LEVEL_SIZE])
        nodesCount = nodes.count()
        if(nodesCount == 0):
            self.logger.info("BFSQ is empty at level <%i>" %\
                self.level)
            hasMore = False

        node = None
        cursor = -1
        while hasMore :
            if node is None:
                node = nodes.next()
            appsPool = copy.copy(self.config[ConfigKeys.APP_LABELS].values())

            while len(appsPool) > 0:
                app = appsPool.pop(0)
                reqRemaining = self.config[ConfigKeys.MULTITHREADING]\
                    [ConfigKeys.FOLLOWERS_WINDOW_REQUESTS_NUMBER]

                while(reqRemaining > 0):
                    reqRemaining, cursor = FollowersCollector(
                        node, self.level,
                        app, cursor,
                        Logger.clone(
                            self.logger, FollowersCollector.CLASS_NAME),
                        self.db, self.config
                    ).start()

                    if cursor == 0:
                        cursor = -1
                        try:
                            node = nodes.next()
                        except StopIteration:
                            self.logger.info("ALL NODES IN BFSQ RETRIEVED (level %i)" %\
                                (self.level))
                            reqRemaining = 0
                            appsPool = []
                            hasMore = False

                self.logger.info("LIMIT REACHED for this application (level %i)" % self.level)
            self.logger.info("ALL APPLICATIONS USED (level %i)" % self.level)
            if hasMore:
                self.logger.info("Sleeping for <%s> min." %\
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME])
                time.sleep(
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME] * 60)

        self.logger.info("LEVEL <%i> COMPLETED" % self.level)

class FollowersCollector():
    # Constants
    CLASS_NAME="FollowersCollector"
    BASE_URL="https://api.twitter.com/1.1"

    def __init__(self,
        node, level,
        app, cursor,
        logger, db, config):

        self.node = node
        self.level = level
        self.app = app
        self.cursor = cursor
        self.logger = logger
        self.db = db
        self.config = config

    def start(self):
        self.logger.info("Collecting followers for node: %s" % (self.node))

        oauth = OAuth1(self.app["consumerKey"],
            self.app["consumerSecret"],
            self.app["accessToken"],
            self.app["accessTokenSecret"])

        url="%s/followers/ids.json?stringify_ids=true&user_id=%s&count=%s&cursor=%s" %\
            (self.BASE_URL,
            self.node[MongoDB.TWITTER_ID_KEY],
            self.config[ConfigKeys.MULTITHREADING][ConfigKeys.FOLLOWERS_REQUEST_SIZE],
            self.cursor)
        self.logger.info("Connecting to: %s" % (url))

        resp = requests.get(url, auth=oauth)
        self.logger.info("HEADERS RECEIVED for node %s: %s" % (self.node, resp.headers))

        limitRemaining = 0
        nextCursor = self.cursor
        if 'x-rate-limit-remaining' in resp.headers:
            limitRemaining = int(resp.headers['x-rate-limit-remaining'])
        else:
            self.logger.error("INVALID RESPONSE: for node %s: %s" %\
                (self.node, resp))
            return (0, 0)

        data = resp.content.decode('utf-8')
        dataO = {}
        try:
            dataO = json.loads(data)
        except ValueError:
            self.logger.error("INVALID RESPONSE: json not found for node %s: %s" %\
                (self.node, data))
            return (0, 0)

        if "ids" in dataO:
            edges = dataO["ids"]
            nextCursor = dataO["next_cursor"]

            Processor.processEdges(self.config,
                self.logger, self.db,
                self.node, self.level,
                ConfigKeys.IN_EDGES_KEY, edges, MongoDB.TWITTER_ID_KEY)

            return (limitRemaining, nextCursor)

        else:
            self.logger.info("IDS NOT FOUND (LIMIT MIGHT BE EXCEEDED): for node %s: %s" %\
                (self.node, dataO))

            return (limitRemaining, self.cursor)
