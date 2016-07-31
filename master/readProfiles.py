import copy
import time
from requests_oauthlib import OAuth1
import requests
import json

from database.mongoDB import MongoDB
from logger import Logger
from configKeys import ConfigKeys
from processor import Processor

class ReadProfiles():
    # Constants
    CLASS_NAME="ReadProfiles"

    def __init__(self,
        level,
        logger, db,
        config):

        self.level = level
        self.logger = logger
        self.db = db
        self.config = config

    def batchNodes(self, nodes):
        batch=[]

        profCount = 0
        while profCount >= 0 and\
            profCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_REQUEST_SIZE]:
            try:
                node = nodes.next()
                batch.append(node)
            except StopIteration:
                profCount = -2
            profCount += 1

        return batch


    def start(self):
        self.logger.info("CREATING THREADS for level <%i>" %\
            self.level)

        hasMore = True
        # Retrieve nodes from previous level
        nodes = self.db.retrieveBFSQ(self.level - 1,
            self.config[ConfigKeys.BFS][ConfigKeys.LEVEL_SIZE], False)
        nodesCount = nodes.count()
        if(nodesCount == 0):
            self.logger.info("BFSQ is empty at level <%i>" %\
                self.level)
            hasMore = False

        batch = None
        while hasMore :
            if batch is None:
                batch = self.batchNodes(nodes)
            appsPool = copy.copy(self.config[ConfigKeys.APP_LABELS].values())

            while len(appsPool) > 0:
                app = appsPool.pop(0)
                reqRemaining = self.config[ConfigKeys.MULTITHREADING]\
                    [ConfigKeys.PROFILES_WINDOW_REQUESTS_NUMBER]

                while(reqRemaining > 0):
                    reqRemaining, success = ProfilesCollector(
                        batch, self.level,
                        app,
                        Logger.clone(
                            self.logger, ProfilesCollector.CLASS_NAME),
                        self.db, self.config
                    ).start()

                    if success:
                        batch = self.batchNodes(nodes)
                        if(len(batch) == 0):
                            self.logger.info("ALL NODES IN BFSQ RETRIEVED (level %i)" %\
                                (self.level))
                            reqRemaining = 0
                            appsPool = []
                            hasMore = False

                self.logger.info("LIMIT REACHED for this application (level %i)" % self.level)
            self.logger.info("ALL APPLICATIONS USED (level %i)" % self.level)
            if hasMore :
                self.logger.info("Sleeping for <%s> min." %\
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME])
                time.sleep(
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME] * 60)

        self.logger.info("LEVEL <%i> COMPLETED" % self.level)

class ProfilesCollector():
    # Constants
    CLASS_NAME="ProfilesCollector"
    BASE_URL="https://api.twitter.com/1.1"

    def __init__(self,
        batch, level,
        app,
        logger, db, config):

        self.batch = batch
        self.level = level
        self.app = app
        self.logger = logger
        self.db = db
        self.config = config

    def start(self):
        self.logger.info("Collecting profiles for batch: %s" % (self.batch))

        oauth = OAuth1(self.app["consumerKey"],
            self.app["consumerSecret"],
            self.app["accessToken"],
            self.app["accessTokenSecret"])

        nodesIds=[]
        nodesSNs=[]
        while(len(self.batch) > 0):
            node = self.batch.pop()
            if(MongoDB.TWITTER_ID_KEY in node):
                nodesIds.append(node[MongoDB.TWITTER_ID_KEY])
            elif(MongoDB.TWITTER_SNAME_ID_KEY in node):
                nodesSNs.append(node[MongoDB.TWITTER_SNAME_ID_KEY])

        url="%s/users/lookup.json" % self.BASE_URL
        self.logger.info("Connecting to: %s" % (url))

        nodesIdsStr = ','.join([str(n) for n in nodesIds])
        nodesSNsStr = ','.join([str(n) for n in nodesSNs])
        resp = requests.post(url, auth=oauth,
            data ={'screen_name': nodesSNsStr,
                'user_id': nodesIdsStr})

        limitRemaining = 0
        if 'x-rate-limit-remaining' in resp.headers:
            limitRemaining = int(resp.headers['x-rate-limit-remaining'])
        else:
            self.logger.error("INVALID RESPONSE: for node %s: %s" %\
                (self.batch, resp))
            return (0, True)

        dataO = {}
        try:
            dataO = resp.json()
        except ValueError:
            self.logger.error("JSON NOT FOUND: for batch %s: %s" %\
                (self.batch, resp.content.decode('utf-8')))
            return (0, True)

        if(type(dataO) is list):
            for profile in dataO:
                pprofile = Processor.processJSONProfile(self.config,
                    self.db,
                    self.logger,
                    profile)
            return (limitRemaining, True)
        else:
            self.logger.info("PROFILES NOT FOUND (LIMIT MIGHT BE EXCEEDED): for batch %s: %s" %\
                (self.batch, dataO))

            return (limitRemaining, False)
