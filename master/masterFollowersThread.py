import copy
import threading
import time

from database.mongoDB import MongoDB
from logger import Logger
from configKeys import ConfigKeys
from subMasterThread import SubMasterThread

class MasterFollowersThread(threading.Thread):
    # Constants
    CLASS_NAME="MasterFollowersThread"

    def __init__(self,
        level,
        logger, db,
        config):

        threading.Thread.__init__(self)

        self.level = level
        self.logger = logger
        self.db = db
        self.config = config

    def start(self):
        self.logger.info("CREATING THREADS for level <%i>" %\
            self.level)

        # Retrieve nodes from previous level
        nodes = self.db.retrieve4FollowersBFSQ(self.level - 1,
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
            # Create a thread per available ip: One thread for each node
            ipsCount = 0
            while ipsCount >= 0 and ipsCount < len(ipsPool):
                ip = ipsPool.pop(0)
                ipsPool.append(ip)

                try:
                    node = nodes.next()
                    thread = SubMasterThread(
                        ConfigKeys.IN_EDGES_KEY,
                        node,
                        self.level,
                        Logger.clone(
                            self.logger, SubMasterThread.CLASS_NAME + "-FOLLOWERS-" + ip),
                        self.db,
                        ip,
                        self.config
                    )
                    threads.append(thread)
                    thread.start()

                except StopIteration:
                    self.logger.info("ALL NODES <%i> IN BFSQ RETRIEVED (level %i)" %\
                        (nodesCount, self.level))
                    hasMoreNodes = False
                    ipsCount = -2

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
