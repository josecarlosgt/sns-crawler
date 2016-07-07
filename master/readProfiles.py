import threading
import httplib
import socket
from httplib import HTTPException
import copy
import time
import json

from logger import Logger
from database.mongoDB import MongoDB
from configKeys import ConfigKeys
from processor import Processor

class ReadProfiles:
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

    def start(self):
        self.logger.info("CREATING THREADS for level <%i>" %\
            self.level)

        # Retrieve nodes from current level
        nodes = self.db.retrieveBFSQ(self.level - 1,
            self.config[ConfigKeys.BFS][ConfigKeys.LEVEL_SIZE], False)
        threads = []
        hasMoreNodes = False
        nodesCount = nodes.count()
        if(nodesCount > 0):
            hasMoreNodes = True
        else:
            self.logger.info("BFSQ is empty at level <%i>" %\
                self.level)
        ipsPool = copy.copy(self.config[ConfigKeys.MULTITHREADING][ConfigKeys.IPS_POOL])
        while(hasMoreNodes):
            i = 1
            # Create a thread per available ip
            ipsCount = 0
            while ipsCount >= 0 and ipsCount < len(ipsPool):
                ip = ipsPool.pop(0)
                ipsPool.append(ip)

                # Iterate over: requests number * request size (on each ip)
                nodesA = []
                reqCount = 0
                while reqCount >= 0 and\
                    reqCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_WINDOW_REQUESTS_NUMBER]:
                    profCount = 0
                    while profCount >= 0 and\
                        profCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_REQUEST_SIZE]:

                        try:
                            node = nodes.next()
                            nodesA.append(node)
                        except StopIteration:
                            self.logger.info("ALL NODES <%i> IN BFSQ RETRIEVED (level %i)" %\
                                (nodesCount, self.level))
                            hasMoreNodes = False
                            profCount = -2
                            reqCount = -2
                            ipsCount = -2

                        profCount += 1
                    reqCount += 1

                thread = ProfilesThread(
                    nodesA,
                    self.level,
                    Logger.clone(
                        self.logger, ProfilesThread.CLASS_NAME + "-" + ip),
                    self.db,
                    ip,
                    self.config
                )
                threads.append(thread)
                thread.start()
                ipsCount += 1

            # Wait until all threads finish to continue with next level
            self.logger.info("WAITING FOR <%ith> POOL OF <%i> THREADS (level %i)" %\
                (i, len(threads), self.level))
            for t in threads: t.join()

            self.logger.info("ALL <%ith> POOL OF <%i> THREADS FINISHED (level %i)" %\
                (i, len(threads), self.level))
            i = i + 1
            threads = []

            if(hasMoreNodes and nodes.alive):
                self.logger.info("Sleeping for <%s> min." %\
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME])
                time.sleep(
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME] * 60)

        self.logger.info("LEVEL <%i> COMPLETED" % self.level)

class ProfilesThread(threading.Thread):
    # Constants
    CLASS_NAME="ProfilesThread"

    def __init__(self,
        node, level,
        logger, db, ip,
        config):

        threading.Thread.__init__(self)

        self.node = node
        self.level = level
        self.logger = logger
        self.db = db
        self.ip = ip
        self.config = config

    def run(self):
        result = self.collect()

    def collect(self):
        self.logger.info("Collecting profiles for level <%s>" % (self.level))

        # Iterate over: requests number * request size (for this ip)
        # One HTTP connection per each request
        reqCount = 0
        while reqCount >= 0 and\
            reqCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_WINDOW_REQUESTS_NUMBER]:
            nodesIds = []
            nodesSNs = []
            profCount = 0
            while profCount >= 0 and\
                profCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_REQUEST_SIZE]:
                    if(len(self.node) > 0):
                        node = self.node.pop()
                        if(MongoDB.TWITTER_ID_KEY in node):
                            nodesIds.append(node[MongoDB.TWITTER_ID_KEY])
                        elif(MongoDB.TWITTER_SNAME_ID_KEY in node):
                            nodesSNs.append(node[MongoDB.TWITTER_SNAME_ID_KEY])

                    else:
                        profCount = -2
                        reqCount = -2

                    profCount += 1

            data=""
            try:
                conn = httplib.HTTPConnection(self.ip)
                nodesIdsStr = ','.join([str(n) for n in nodesIds])
                nodesSNsStr = ','.join([str(n) for n in nodesSNs])
                appLabel = self.config[ConfigKeys.APP_LABELS_MAP][self.ip]
                url = "/api-client/profiles?app_label=%s&ids=%s&snames=%s" %\
                    (appLabel, nodesIdsStr, nodesSNsStr)
                self.logger.info("Connecting to: %s%s" % (self.ip, url))
                conn.request("GET", url)
                r = conn.getresponse()
                data = r.read()
                conn.close()
            except socket.error:
                self.logger.\
                    error("Socket error when collecting profiles")
                return False
            except HTTPException:
                self.logger.\
                    error("HTTP error when collecting profiles")
                return False

            try:
                profiles = json.loads(data)
                for profile in profiles:
                    pprofile = Processor.processJSONProfile(self.config, profile)
                    self.logger.info("PROFILE PARSED: %s" % pprofile)

                    if(pprofile["friends_count"] <= self.config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE] and\
                        pprofile["followers_count"] <= self.config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE]):
                        self.db.insertNode(pprofile, self.ip)
                        self.db.updateBFSQ(pprofile)
                    else:
                        self.logger.info("Profile <%s> exceeded degree limit: %s,%s" %\
                            (pprofile[MongoDB.TWITTER_SNAME_ID_KEY],
                                pprofile["friends_count"],
                                pprofile["followers_count"]))
            except ValueError:
                self.logger.\
                    error("Not valid JSON response received: **%s**" % data)

            reqCount += 1

        return True
