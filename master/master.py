import logging
import datetime
import time
from pymongo.errors import ServerSelectionTimeoutError

from database.mongoDB import MongoDB
from logger import Logger
from masterThread import MasterThread

class Master:
    # Constants
    CLASS_NAME="Master"

    def __init__(self, config):
        today = datetime.date.today()
        timeId = today.strftime('%d_%m_%Y')

        enableStdout = config["MasterLogging"]["enableStdout"]
        logFile = config["MasterLogging"]["logFile"]
        with open(logFile, 'w'): pass

        logging.basicConfig(filename=logFile, level=logging.INFO)
        logging.basicConfig(filename=logFile, level=logging.ERROR)
        self.logger = Logger(enableStdout, self.CLASS_NAME, timeId)
        self.logger.info("Running crawler")

        self.logger.info("Checking configuration")
        for section, sValue in config.iteritems():
            self.logger.info(section)
            for option, value in sValue.iteritems():
                self.logger.info("\t %s: %s" % (option, value))

        self.ipsPool = config["SNS"]["ipsPool"]
        self.THREADS_POOL_SIZE = config["SNS"]["threadsPoolSize"]

        self.EGO_ID = config["SNS"]["egoId"]
        self.INVALID_IDS = config["SNS"]["invalidIds"]
        self.SAMPLE_SIZE = config["SNS"]["sampleSize"]
        self.THREADS_INTERVAL_TIME = config["SNS"]["threadsIntervalTime"]

        self.TOP_LEVEL = config["BFS"]["topLevel"]
        self.BFSQ_LEVEL_SIZE = config["BFS"]["levelSize"]

        self.db = MongoDB(timeId,
            Logger.clone(self.logger, MongoDB.CLASS_NAME))

    def breadth_first_search(self, level):
        self.logger.info("CREATING THREADS for level %i" %\
            level)

        # Retrieve nodes from current level
        nodes = self.db.retrieveBFSQ(level - 1, self.BFSQ_LEVEL_SIZE)
        threads = []
        hasMoreNodes = True
        while(hasMoreNodes):
            i = 1
            for count in range(0, self.THREADS_POOL_SIZE):
                try:
                    node = nodes.next()
                    ip = self.ipsPool.pop(0)
                    self.ipsPool.append(ip)
                    nodeId = node["_id"]
                    thread = MasterThread(
                        nodeId,
                        level,
                        Logger.clone(
                            self.logger, MasterThread.CLASS_NAME + "-" + nodeId),
                        self.db,
                        ip,
                        self.INVALID_IDS,
                        self.SAMPLE_SIZE
                    )
                    threads.append(thread)
                    thread.start()
                    time.sleep(self.THREADS_INTERVAL_TIME)
                except StopIteration:
                    hasMoreNodes = False
                    self.logger.info("ALL NODES IN BFSQ RETRIEVED (level %i)" %\
                        (level))
                    break

            # Wait until all threads finish to continue with next level
            self.logger.info("WAITING FOR %ith POOL OF %i THREADS (level %i)" %\
                (i, len(threads), level))
            for t in threads: t.join()
            self.logger.info("ALL %ith POOL OF %i THREADS FINISHED (level %i)" %\
                (i, len(threads), level))
            i = i + 1
            threads = []

        self.logger.info("LEVEL %i COMPLETED" % level)

        # Proceed with next level
        if(level < self.TOP_LEVEL):
            next_level = level + 1
            self.breadth_first_search(next_level)

    def start(self):
        LEVEL_0 = 0

        try:
            self.db.createEdgesIndex()
            self.db.createNodesIndex()

            # Disable this when resuming a failed operation
            self.db.clearBFSQ()

            # Add seed node to the bfs_queue
            self.db.updateBFSQ(self.EGO_ID, LEVEL_0)

            # Start bfs
            self.breadth_first_search(LEVEL_0 + 1)
        except ServerSelectionTimeoutError:
            self.logger.error("Connection to database failed")
