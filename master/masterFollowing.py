import threading
import httplib
import socket
from httplib import HTTPException
import copy
import time

from database.mongoDB import MongoDB
from logger import Logger
from configKeys import ConfigKeys
from processor import Processor

class MasterFollowing():
    # Constants
    CLASS_NAME="MasterFollowing"

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

        # Retrieve nodes from previous level
        nodes = self.db.retrieve4FollowingBFSQ(self.level - 1,
            self.config[ConfigKeys.BFS][ConfigKeys.LEVEL_SIZE])
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
            for count in range(0, self.config[ConfigKeys.MULTITHREADING][ConfigKeys.THREADS_POOL_SIZE]):
                try:
                    node = nodes.next()
                    ip = ipsPool.pop(0)
                    ipsPool.append(ip)
                    thread = FollowingThread(
                        node,
                        self.level,
                        Logger.clone(
                            self.logger, FollowingThread.CLASS_NAME + "-" + ip),
                        self.db,
                        ip,
                        self.config
                    )
                    threads.append(thread)
                    thread.start()
                    time.sleep(self.config[ConfigKeys.MULTITHREADING][ConfigKeys.THREADS_INTERVAL_TIME])
                except StopIteration:
                    hasMoreNodes = False
                    self.logger.info("ALL NODES IN BFSQ RETRIEVED (level %i)" %\
                        (self.level))
                    break

            # Wait until all threads finish to continue with next level
            self.logger.info("WAITING FOR <%ith> POOL OF <%i> THREADS (level %i)" %\
                (i, len(threads), self.level))
            for t in threads: t.join()

            self.logger.info("ALL %ith POOL OF <%i> THREADS FINISHED (level %i)" %\
                (i, len(threads), self.level))
            i = i + 1
            threads = []

        self.logger.info("LEVEL <%i> COMPLETED" % self.level)

class FollowingThread(threading.Thread):
    # Constants
    CLASS_NAME="FollowingThread"

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
        self.logger.info("Collecting following edges for node: %s" % (self.node))

        data=""
        try:
            conn = httplib.HTTPConnection(self.ip)
            url = "/website-crawler/following?&cursor_count=%s&screen_name=%s" %\
                (self.config[ConfigKeys.SNS][ConfigKeys.CURSOR_COUNT],
                self.node[MongoDB.TWITTER_SNAME_ID_KEY])

            self.logger.info("Connecting to: %s%s" % (self.ip, url))
            conn.request("GET", url)
            r = conn.getresponse()
            data = r.read()
            conn.close()
        except socket.error:
            self.logger.\
                error("Socket error when collecting edges for node: %s" %\
                    self.node)
            return False
        except HTTPException:
            self.logger.\
                error("HTTP error when collecting edges for node: %s" %\
                    self.node)
            return False

        edges = set([sname.strip() \
                for sname in data.split(",") if len(sname.strip()) > 0])

        Processor.processEdges(self.config,
            self.logger, self.db,
            self.node, self.level,
            ConfigKeys.OUT_EDGES_KEY, edges, MongoDB.TWITTER_SNAME_ID_KEY)

        return True
